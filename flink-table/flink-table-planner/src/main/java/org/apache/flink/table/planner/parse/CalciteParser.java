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

package org.apache.flink.table.planner.parse;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.SqlParserException;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.SourceStringReader;

import java.io.Reader;

/**
 * Thin wrapper around {@link SqlParser} that does exception conversion and {@link SqlNode} casting.
 */
public class CalciteParser {
    private final SqlParser.Config config;

    public CalciteParser(SqlParser.Config config) {
        this.config = config;
    }

    /**
     * Parses a SQL statement into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
     *
     * @param sql a sql string to parse
     * @return a parsed sql node
     * @throws SqlParserException if an exception is thrown when parsing the statement
     * @throws SqlParserEOFException if the statement is incomplete
     */
    public SqlNode parse(String sql) {
        try {
            SqlParser parser = SqlParser.create(sql, config);
            return parser.parseStmt();
        } catch (SqlParseException e) {
            if (e.getMessage().contains("Encountered \"<EOF>\"")) {
                throw new SqlParserEOFException(e.getMessage(), e);
            }
            throw new SqlParserException("SQL parse failed. " + e.getMessage(), e);
        }
    }

    /**
     * Parses a SQL string into a {@link SqlNodeList}. The {@link SqlNodeList} is not yet validated.
     *
     * @param sql a sql string to parse
     * @return a parsed sql node list
     * @throws SqlParserException if an exception is thrown when parsing the statement
     * @throws SqlParserEOFException if the statement is incomplete
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将 SQL 字符串解析为 {@link SqlNodeList} 对象。
     * 该 {@link SqlNodeList} 对象尚未进行验证。
     *
     * @param sql 要解析的 SQL 字符串
     * @return 解析后的 SQL 节点列表
     * @throws SqlParserException 解析语句时抛出异常
     * @throws SqlParserEOFException 如果语句不完整
    */
    public SqlNodeList parseSqlList(String sql) {
        try {
            // 创建一个 SqlParser 实例来解析 SQL 字符串
            SqlParser parser = SqlParser.create(sql, config);
            /**
             * 调用 SqlParser 的 parseStmtList 方法来解析 SQL 语句列表
             * 1，javaCC 使用自顶向下（top-down）递归下降（recursive descent)解析。
             * 2，默认是LL(1)文法用于预测性解析。
             */
            return parser.parseStmtList();
        } catch (SqlParseException e) {
            // 如果异常消息中包含 "Encountered \"<EOF>\""，说明语句不完整
            if (e.getMessage().contains("Encountered \"<EOF>\"")) {
                //抛出异常
                throw new SqlParserEOFException(e.getMessage(), e);
            }
            //抛出异常
            throw new SqlParserException("SQL parse failed. " + e.getMessage(), e);
        }
    }

    /**
     * Parses a SQL expression into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
     *
     * @param sqlExpression a SQL expression string to parse
     * @return a parsed SQL node
     * @throws SqlParserException if an exception is thrown when parsing the statement
     */
    public SqlNode parseExpression(String sqlExpression) throws SqlParserException {
        try {
            final SqlParser parser = SqlParser.create(sqlExpression, config);
            return parser.parseExpression();
        } catch (SqlParseException e) {
            throw new SqlParserException("SQL parse failed. " + e.getMessage(), e);
        }
    }

    /**
     * Parses a SQL string as an identifier into a {@link SqlIdentifier}.
     *
     * @param identifier a sql string to parse as an identifier
     * @return a parsed sql node
     * @throws SqlParserException if an exception is thrown when parsing the identifier
     */
    public SqlIdentifier parseIdentifier(String identifier) throws SqlParserException {
        try {
            SqlAbstractParserImpl flinkParser = createFlinkParser(identifier);
            if (flinkParser instanceof FlinkSqlParserImpl) {
                return ((FlinkSqlParserImpl) flinkParser).TableApiIdentifier();
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized sql parser type " + flinkParser.getClass().getName());
            }
        } catch (Exception e) {
            throw new SqlParserException(
                    String.format("Invalid SQL identifier %s.", identifier), e);
        }
    }

    /**
     * Equivalent to {@link SqlParser#create(Reader, SqlParser.Config)}. The only difference is we
     * do not wrap the {@link FlinkSqlParserImpl} with {@link SqlParser}.
     *
     * <p>It is so that we can access specific parsing methods not accessible through the {@code
     * SqlParser}.
     */
    private SqlAbstractParserImpl createFlinkParser(String expr) {
        SourceStringReader reader = new SourceStringReader(expr);
        SqlAbstractParserImpl parser = config.parserFactory().getParser(reader);
        parser.setTabSize(1);
        parser.setQuotedCasing(config.quotedCasing());
        parser.setUnquotedCasing(config.unquotedCasing());
        parser.setIdentifierMaxLength(config.identifierMaxLength());
        parser.setConformance(config.conformance());
        switch (config.quoting()) {
            case DOUBLE_QUOTE:
                parser.switchTo(SqlAbstractParserImpl.LexicalState.DQID);
                break;
            case BACK_TICK:
                parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);
                break;
            case BRACKET:
                parser.switchTo(SqlAbstractParserImpl.LexicalState.DEFAULT);
                break;
        }

        return parser;
    }
}
