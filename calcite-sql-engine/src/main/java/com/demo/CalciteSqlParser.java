package com.demo;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.PrestoSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.util.Util;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * @Author: velar
 * @Date: 2024/10/17 11:40
 * @Description:
 */
public class CalciteSqlParser {

    private final SqlWriterConfig sqlWriterConfig = SqlPrettyWriter.config()
            .withAlwaysUseParentheses(true)
            .withUpdateSetListNewline(false)
            .withHavingFolding(SqlWriterConfig.LineFolding.TALL)
            .withIndentation(0);

    private List<EngineType> engineTypes = Arrays.asList(EngineType.HIVE, EngineType.PRESTO, EngineType.SPARK);

    private UnaryOperator<SqlParser.Config> getTransform(
            SqlDialect dialect) {
        return dialect == null ? UnaryOperator.identity()
                : dialect::configureParser;
    }


    private SqlParser getSqlParser(Reader source,
                                   UnaryOperator<SqlParser.Config> transform,
                                   EngineType engineType) {
        final SqlParser.Config config = getSqlParserConfig(transform, engineType);
        return SqlParser.create(source, config);
    }

    private SqlParser.Config getSqlParserConfig
            (UnaryOperator<SqlParser.Config> transform, EngineType engineType) {
        final SqlParser.Config configBuilder =
                SqlParser.config()
                        .withParserFactory(SqlParserImpl.FACTORY)
                        .withUnquotedCasing(Casing.UNCHANGED)
                        .withQuotedCasing(Casing.UNCHANGED)
                        .withCaseSensitive(false)
                        .withConformance(getDefaultSqlConformance(engineType));
        return transform.apply(configBuilder);
    }

    private SqlDialect getDefaultSqlDialect(EngineType engineType) {
        switch (engineType) {
            case SPARK:
                return SparkSqlDialect.DEFAULT;
            case PRESTO:
                return PrestoSqlDialect.DEFAULT;
            case HIVE:
                SqlDialect.Context hiveContext = SqlDialect.EMPTY_CONTEXT
                        .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
                        .withQuotedCasing(Casing.UNCHANGED)
                        .withNullCollation(NullCollation.LOW)
                        .withIdentifierQuoteString("`")
                        .withCaseSensitive(false);
                return new HiveSqlDialect(hiveContext);

            default:
                return CalciteSqlDialect.DEFAULT;
        }
    }

    private SqlConformance getDefaultSqlConformance(EngineType engineType) {
        switch (engineType) {
            case SPARK:
                return SqlConformanceEnum.DEFAULT;
            case PRESTO:
                return SqlConformanceEnum.PRESTO;
            case HIVE:
                return SqlConformanceEnum.DEFAULT;
            default:
                return SqlConformanceEnum.DEFAULT;
        }

    }

    private SqlNode parseStmtAndHandleEx(String sql,
                                         UnaryOperator<SqlParser.Config> transform,
                                         Consumer<SqlParser> parserChecker,
                                         EngineType engineType) throws SqlParseException {
        final Reader reader = new SourceStringReader(sql);
        final SqlParser parser = getSqlParser(reader, transform, engineType);
        final SqlNode sqlNode;
        sqlNode = parser.parseStmt();
        parserChecker.accept(parser);
        return sqlNode;
    }

    public SqlNode parserSql(String sql, EngineType userSpecified)  throws SqlParseException {
        if (userSpecified.equals(EngineType.DEFAULT)) {
            for (EngineType engineType : engineTypes) {
                try {
                    final UnaryOperator<SqlParser.Config> transform = getTransform(getDefaultSqlDialect(engineType));
                    return parseStmtAndHandleEx(sql, transform, parser -> {}, userSpecified);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            final UnaryOperator<SqlParser.Config> transform = getTransform(getDefaultSqlDialect(userSpecified));

            return parseStmtAndHandleEx(sql, transform, parser -> {}, userSpecified);
        }
        throw new RuntimeException("parser sql error");
    }
    public String actualSql(SqlNode sqlNode, EngineType recommendEngine) {
        SqlDialect recommendSqlDialect = getDefaultSqlDialect(recommendEngine);
        final SqlDialect outDialect = Util.first(recommendSqlDialect, HiveSqlDialect.DEFAULT);
        final SqlWriterConfig writerConfig = sqlWriterConfig.withDialect(outDialect);
        return sqlNode.toSqlString(c -> writerConfig).getSql();
    }

    public static void main(String[] args) throws SqlParseException {
        String sql = "select c1 from emps where `id` = 1";
        CalciteSqlParser calciteSqlParser = new CalciteSqlParser();
        SqlNode sqlNode = calciteSqlParser.parserSql(sql, EngineType.HIVE);
        String reWriteSql = calciteSqlParser.actualSql(sqlNode, EngineType.PRESTO);
        System.out.println(reWriteSql);
    }

}
