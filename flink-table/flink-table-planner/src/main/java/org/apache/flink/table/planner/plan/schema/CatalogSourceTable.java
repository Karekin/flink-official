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

package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.OptionalUtils.firstPresent;

/**
 * A {@link FlinkPreparingTableBase} implementation which defines the interfaces required to
 * translate the Calcite {@link RelOptTable} to the Flink specific {@link TableSourceTable}.
 *
 * <p>This table is only used to translate the {@link CatalogTable} into {@link TableSourceTable}
 * during the last phase of the SQL-to-rel conversion, it is not necessary anymore once the SQL node
 * was converted to a relational expression.
 */
public final class CatalogSourceTable extends FlinkPreparingTableBase {

    private final CatalogSchemaTable schemaTable;

    public CatalogSourceTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            CatalogSchemaTable schemaTable) {
        super(relOptSchema, rowType, names, schemaTable.getStatistic());
        this.schemaTable = schemaTable;
    }

    /**
     * Create a {@link CatalogSourceTable} from an anonymous {@link ContextResolvedTable}. This is
     * required to manually create a preparing table skipping the calcite catalog resolution.
     */
    public static CatalogSourceTable createAnonymous(
            FlinkRelBuilder relBuilder,
            ContextResolvedTable contextResolvedTable,
            boolean isBatchMode) {
        Preconditions.checkArgument(
                contextResolvedTable.isAnonymous(), "ContextResolvedTable must be anonymous");

        // Statistics are unknown for anonymous tables
        // Look at DatabaseCalciteSchema#getStatistic for more details
        FlinkStatistic flinkStatistic =
                FlinkStatistic.unknown(contextResolvedTable.getResolvedSchema()).build();

        CatalogSchemaTable catalogSchemaTable =
                new CatalogSchemaTable(contextResolvedTable, flinkStatistic, !isBatchMode);

        return new CatalogSourceTable(
                relBuilder.getRelOptSchema(),
                contextResolvedTable.getIdentifier().toList(),
                catalogSchemaTable.getRowType(relBuilder.getTypeFactory()),
                catalogSchemaTable);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将SQLNode转换为RelNode
     */
    @Override
    public RelNode toRel(ToRelContext toRelContext) {
        // 获取Calcite的集群信息，它是构建查询计划时所需的基础环境
        final RelOptCluster cluster = toRelContext.getCluster();
        // 获取表级别的提示信息，这些提示可以影响查询计划的生成
        final List<RelHint> hints = toRelContext.getTableHints();
        // 从集群信息中解包出Flink的上下文信息
        final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        // 创建一个Flink的RelBuilder，用于构建Calcite的RelNode
        final FlinkRelBuilder relBuilder = FlinkRelBuilder.of(cluster, relOptSchema);

        // finalize catalog table with option hints
        // 使用表级别的提示信息来最终确定catalog表的选项
        final Map<String, String> hintedOptions = FlinkHints.getHintedOptions(hints);
        // 根据Flink上下文和带有提示的选项，计算并解析出最终的catalog表
        final ContextResolvedTable catalogTable =
                computeContextResolvedTable(context, hintedOptions);

        // create table source
        // 根据解析后的catalog表信息，创建DynamicTableSource，这是Flink中用于表示表数据源的一种方式
        final DynamicTableSource tableSource =
                createDynamicTableSource(context, catalogTable.getResolvedTable());

        // prepare table source and convert to RelNode
        // 准备tableSource，并将其转换为Calcite的RelNode。
        return DynamicSourceUtils.convertSourceToRel(
                !schemaTable.isStreamingMode(),
                context.getTableConfig(),
                relBuilder,
                schemaTable.getContextResolvedTable(),
                schemaTable.getStatistic(),
                hints,
                tableSource);
    }

    private ContextResolvedTable computeContextResolvedTable(
            FlinkContext context, Map<String, String> hintedOptions) {
        ContextResolvedTable contextResolvedTable = schemaTable.getContextResolvedTable();
        if (hintedOptions.isEmpty()) {
            return contextResolvedTable;
        }
        if (!context.getTableConfig().get(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED)) {
            throw new ValidationException(
                    String.format(
                            "The '%s' hint is allowed only when the config option '%s' is set to true.",
                            FlinkHints.HINT_NAME_OPTIONS,
                            TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key()));
        }
        if (contextResolvedTable.getResolvedTable().getTableKind() == TableKind.VIEW) {
            throw new ValidationException(
                    String.format(
                            "View '%s' cannot be enriched with new options. "
                                    + "Hints can only be applied to tables.",
                            contextResolvedTable.getIdentifier()));
        }
        return contextResolvedTable.copy(
                FlinkHints.mergeTableOptions(
                        hintedOptions, contextResolvedTable.getResolvedTable().getOptions()));
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 创建SourceTable
     */
    private DynamicTableSource createDynamicTableSource(
            FlinkContext context, ResolvedCatalogTable catalogTable) {
        // 尝试从catalog中获取DynamicTableSourceFactory
        final Optional<DynamicTableSourceFactory> factoryFromCatalog =
                schemaTable
                        .getContextResolvedTable()
                        .getCatalog()
                        .flatMap(Catalog::getFactory)
                        .map(
                                f ->
                                        f instanceof DynamicTableSourceFactory
                                                ? (DynamicTableSourceFactory) f
                                                : null);
        // 尝试从模块管理器中获取DynamicTableSourceFactory
        final Optional<DynamicTableSourceFactory> factoryFromModule =
                context.getModuleManager().getFactory(Module::getTableSourceFactory);

        // Since the catalog is more specific, we give it precedence over a factory provided by any
        // modules.
        // 由于catalog提供的工厂更具体，因此它比模块提供的工厂具有更高的优先级
        // 使用firstPresent方法从两个Optional中选择第一个非空的
        final DynamicTableSourceFactory factory =
                firstPresent(factoryFromCatalog, factoryFromModule).orElse(null);
        // 使用FactoryUtil的静态方法创建DynamicTableSource
        return FactoryUtil.createDynamicTableSource(
                factory,
                schemaTable.getContextResolvedTable().getIdentifier(),
                catalogTable,
                context.getTableConfig(),
                context.getClassLoader(),
                schemaTable.isTemporary());
    }

    public CatalogTable getCatalogTable() {
        return schemaTable.getContextResolvedTable().getResolvedTable();
    }
}
