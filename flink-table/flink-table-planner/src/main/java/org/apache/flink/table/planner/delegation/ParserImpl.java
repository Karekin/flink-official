/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.delegation;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.parse.ExtendedParser;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
/**
 * {@link Parser} 的实现，使用 Calcite 进行 SQL 解析和校验。
 *
 * 该类主要负责：
 * 1. 解析 SQL 语句，转换为 Flink 内部的操作（Operation）。
 * 2. 校验 SQL 语句的正确性，并解析 SQL 表达式。
 * 3. 提供 SQL 语法补全功能，以帮助用户更好地编写 SQL 查询。
 *
 * `ParserImpl` 使用了 Calcite 解析器（CalciteParser）和 Flink 校验器（FlinkPlannerImpl），
 * 并结合了扩展解析器（ExtendedParser）以增强 SQL 解析能力。
 */
public class ParserImpl implements Parser {

    // CatalogManager 负责管理数据库和表的元数据
    private final CatalogManager catalogManager;

    // 使用 Supplier 模式来动态获取 FlinkPlannerImpl，以便支持动态配置更新
    private final Supplier<FlinkPlannerImpl> validatorSupplier;

    // 使用 Supplier 模式来动态获取 CalciteParser，以便支持动态配置更新
    private final Supplier<CalciteParser> calciteParserSupplier;

    // 用于创建 SQL 到 RexNode（关系表达式）的转换器
    private final RexFactory rexFactory;

    // Flink 扩展解析器，用于解析特定 SQL 语法
    private static final ExtendedParser EXTENDED_PARSER = ExtendedParser.INSTANCE;

    /**
     * 构造方法，初始化解析器。
     *
     * @param catalogManager        负责管理元数据（数据库、表等）。
     * @param validatorSupplier     提供 Flink SQL 语法校验器实例的 Supplier。
     * @param calciteParserSupplier 提供 Calcite SQL 解析器实例的 Supplier。
     * @param rexFactory            负责将 SQL 解析为 RexNode 的工厂类。
     */
    public ParserImpl(
            CatalogManager catalogManager,
            Supplier<FlinkPlannerImpl> validatorSupplier,
            Supplier<CalciteParser> calciteParserSupplier,
            RexFactory rexFactory) {
        this.catalogManager = catalogManager;
        this.validatorSupplier = validatorSupplier;
        this.calciteParserSupplier = calciteParserSupplier;
        this.rexFactory = rexFactory;
    }

    /**
     * 解析 SQL 语句并返回解析后的操作列表。
     *
     * 解析逻辑：
     * 1. 先尝试使用 {@link ExtendedParser} 解析 SQL 语句（支持 Flink 特定扩展）。
     * 2. 如果扩展解析器无法解析，则使用 {@link CalciteParser} 解析标准 SQL 语句。
     * 3. 解析完成后，将 SQL 语法树（SqlNode）转换为 Flink 内部的 Operation。
     * 4. 仅支持解析单条 SQL 语句，否则抛出异常。
     *
     * @param statement 需要解析的 SQL 语句
     * @return 解析后的操作列表（通常只包含单个操作）
     * @throws TableException 当 SQL 解析失败或不受支持时抛出异常
     */
    @Override
    public List<Operation> parse(String statement) {
        // 获取 Calcite 解析器实例
        CalciteParser parser = calciteParserSupplier.get();
        // 获取 Flink 规划器实例（用于 SQL 语法校验）
        FlinkPlannerImpl planner = validatorSupplier.get();

        // 先尝试使用扩展解析器解析 SQL 语句
        Optional<Operation> command = EXTENDED_PARSER.parse(statement);
        if (command.isPresent()) {
            // 如果扩展解析器成功解析，则直接返回结果
            return Collections.singletonList(command.get());
        }

        /**
         * 使用 Calcite 解析器解析 SQL 语句
         * 由于 Flink SQL Client 允许 SQL 语句以 `;` 结尾，因此使用 parseSqlList 方法进行解析
         */
        SqlNodeList sqlNodeList = parser.parseSqlList(statement);
        // 获取解析结果（通常只包含单个 SQL 语句）
        List<SqlNode> parsed = sqlNodeList.getList();

        // 确保只解析了一条 SQL 语句，否则抛出异常
        Preconditions.checkArgument(parsed.size() == 1, "仅支持单条 SQL 语句解析");

        /**
         * 将解析的 SqlNode 转换为 Flink 内部 Operation
         * 如果转换失败，则抛出 TableException 异常
         */
        return Collections.singletonList(
                SqlNodeToOperationConversion.convert(planner, catalogManager, parsed.get(0))
                        .orElseThrow(() -> new TableException("不支持的 SQL 查询: " + statement)));
    }

    /**
     * 解析 SQL 标识符（如表名或列名）。
     *
     * @param identifier 需要解析的 SQL 标识符
     * @return 解析后的未解析标识符（UnresolvedIdentifier）
     */
    @Override
    public UnresolvedIdentifier parseIdentifier(String identifier) {
        // 获取 Calcite 解析器实例
        CalciteParser parser = calciteParserSupplier.get();
        // 解析 SQL 标识符
        SqlIdentifier sqlIdentifier = parser.parseIdentifier(identifier);
        return UnresolvedIdentifier.of(sqlIdentifier.names);
    }

    /**
     * 解析 SQL 表达式，并将其转换为 Flink 内部的 ResolvedExpression。
     *
     * 该方法支持输入类型（RowType）和可选的输出类型（LogicalType），
     * 并会将解析后的 SQL 表达式转换为可执行的 RexNode。
     *
     * @param sqlExpression  需要解析的 SQL 表达式
     * @param inputRowType   SQL 表达式的输入数据类型（RowType）
     * @param outputType     （可选）SQL 表达式的目标输出类型（LogicalType）
     * @return 解析后的 `ResolvedExpression`
     * @throws ValidationException 当 SQL 表达式非法或无法解析时抛出异常
     */
    @Override
    public ResolvedExpression parseSqlExpression(
            String sqlExpression, RowType inputRowType, @Nullable LogicalType outputType) {
        try {
            // 创建 SQL 到 RexNode 的转换器
            final SqlToRexConverter sqlToRexConverter =
                    rexFactory.createSqlToRexConverter(inputRowType, outputType);
            // 将 SQL 表达式转换为 RexNode
            final RexNode rexNode = sqlToRexConverter.convertToRexNode(sqlExpression);
            // 获取解析后的逻辑数据类型
            final LogicalType logicalType = FlinkTypeFactory.toLogicalType(rexNode.getType());
            // 展开表达式，以支持序列化
            final String sqlExpressionExpanded = sqlToRexConverter.expand(sqlExpression);
            return new RexNodeExpression(
                    rexNode,
                    TypeConversions.fromLogicalToDataType(logicalType),
                    sqlExpression,
                    sqlExpressionExpanded);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format("非法 SQL 表达式: %s", sqlExpression), t);
        }
    }

    /**
     * 获取 SQL 语法补全建议（自动补全）。
     *
     * @param statement 输入的 SQL 语句
     * @param cursor    光标所在位置（用于提供合适的补全建议）
     * @return SQL 语法补全建议数组
     */
    public String[] getCompletionHints(String statement, int cursor) {
        // 存储补全建议
        List<String> candidates =
                new ArrayList<>(
                        Arrays.asList(EXTENDED_PARSER.getCompletionHints(statement, cursor)));

        // 获取 SQL 校验器
        SqlAdvisorValidator validator = validatorSupplier.get().getSqlAdvisorValidator();
        SqlAdvisor advisor =
                new SqlAdvisor(validator, validatorSupplier.get().config().getParserConfig());
        String[] replaced = new String[1];

        // 获取 SQL 语法补全建议，并转换为字符串列表
        List<String> sqlHints =
                advisor.getCompletionHints(statement, cursor, replaced).stream()
                        .map(item -> item.toIdentifier().toString())
                        .collect(Collectors.toList());

        candidates.addAll(sqlHints);

        return candidates.toArray(new String[0]);
    }

    /**
     * 获取 CatalogManager 实例，用于管理数据库和表的元数据。
     *
     * @return CatalogManager 实例
     */
    public CatalogManager getCatalogManager() {
        return catalogManager;
    }
}

