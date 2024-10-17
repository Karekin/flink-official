package com.lineage.demo;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class LineageDemoV2 {

    private static final Map<JobID, String> jobSqlMapping = new HashMap<>();
    private static final List<JobListener> jobListeners = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建源表和目标表
        tableEnv.executeSql("CREATE TABLE datagen (\n" +
                "        f_sequence1 INT,\n" +
                "        f_random1 INT,\n" +
                "        f_random_str1 STRING,\n" +
                "        ts AS localtimestamp,\n" +
                "        WATERMARK FOR ts AS ts\n" +
                "    ) WITH (\n" +
                "        'connector' = 'datagen',\n" +
                "        'rows-per-second' = '5',\n" +
                "        'fields.f_sequence1.kind' = 'sequence',\n" +
                "        'fields.f_sequence1.start' = '1',\n" +
                "        'fields.f_sequence1.end' = '500',\n" +
                "        'fields.f_random1.min' = '1',\n" +
                "        'fields.f_random1.max' = '500',\n" +
                "        'fields.f_random_str1.length' = '10'\n" +
                "        )");

        tableEnv.executeSql("CREATE TABLE print_table (\n" +
                "        f_sequence INT,\n" +
                "        f_random INT,\n" +
                "        f_random_str STRING\n" +
                "    ) WITH (\n" +
                "        'connector' = 'print'\n" +
                "        )");

        // SQL 查询
        String sql = "INSERT INTO print_table " +
                "SELECT t1.f_sequence1, t1.f_random1, t2.f_random_str1 " +
                "FROM datagen t1 LEFT JOIN datagen t2 ON t1.f_sequence1 = t2.f_sequence1";

        // 注册 JobListener 监听作业提交和执行状态
        registerJobListener(new LineageJobListener((StreamTableEnvironmentImpl) tableEnv));

        // 执行 SQL，获取 JobClient
        JobClient jobClient = executeAsync(sql, tableEnv);
    }

    public static JobClient executeAsync(String sql, StreamTableEnvironment tableEnv) throws Exception {
        Optional<JobClient> jobClientFuture = tableEnv.executeSql(sql).getJobClient();

        // 获取 JobClient
        JobClient jobClient = jobClientFuture.get();

        // 将 JobID 与 SQL 映射存储起来
        jobSqlMapping.put(jobClient.getJobID(), sql);

        // 通知所有监听器作业已提交
        jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));

        // 返回 JobClient
        return jobClient;

    }

    // 注册 JobListener
    public static void registerJobListener(JobListener jobListener) {
        jobListeners.add(jobListener);
    }

    // 自定义 JobListener，用于作业提交和执行后的回调
    public static class LineageJobListener implements JobListener {

        private final StreamTableEnvironmentImpl tableEnvImpl;

        public LineageJobListener(StreamTableEnvironmentImpl tableEnvImpl) {
            this.tableEnvImpl = tableEnvImpl;
        }

        @Override
        public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
            if (throwable != null) {
                System.err.println("Job submission failed: " + throwable.getMessage());
            } else {
                System.out.println("Job submitted successfully: " + jobClient.getJobID());

                // 获取作业对应的 SQL，调用解析方法 parseStatement，并生成血缘关系
                String sql = jobSqlMapping.get(jobClient.getJobID());

                Tuple2<String, RelNode> parsed = parseStatement(tableEnvImpl, sql);
                String sinkTable = parsed.f0;
                RelNode oriRelNode = parsed.f1;

                // 生成字段级别的血缘信息
                buildFiledLineageResult(tableEnvImpl, sinkTable, oriRelNode);
            }
        }

        @Override
        public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
            if (throwable != null) {
                System.err.println("Job execution failed: " + throwable.getMessage());
            } else {
                System.out.println("Job executed successfully: " + jobExecutionResult.getJobID());
            }
        }
    }

    // 解析 SQL 语句，生成 RelNode
    public static Tuple2<String, RelNode> parseStatement(StreamTableEnvironmentImpl tableEnv, String sql) {
        List<Operation> operations = tableEnv.getParser().parse(sql);
        if (operations.size() != 1) {
            throw new TableException("Unsupported SQL query! Only accepts a single SQL statement.");
        }
        Operation operation = operations.get(0);
        SinkModifyOperation sinkOperation = (SinkModifyOperation) operation;
        PlannerQueryOperation queryOperation = (PlannerQueryOperation) sinkOperation.getChild();
        RelNode relNode = queryOperation.getCalciteTree();
        return new Tuple2<>(sinkOperation.getContextResolvedTable().getIdentifier().asSummaryString(), relNode);
    }

    // 生成字段级别的血缘结果
    public static void buildFiledLineageResult(StreamTableEnvironment tableEnv, String sinkTable, RelNode optRelNode) {
        // 目标表的字段
        ResolvedSchema schema = tableEnv.from(sinkTable).getResolvedSchema();
        List<String> targetColumnList = schema.getColumnNames();
        RelMetadataQuery metadataQuery = optRelNode.getCluster().getMetadataQuery();

        for (int index = 0; index < targetColumnList.size(); index++) {
            String targetColumn = targetColumnList.get(index);
            Set<RelColumnOrigin> relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index);
            if (relColumnOriginSet != null && !relColumnOriginSet.isEmpty()) {
                for (RelColumnOrigin relColumnOrigin : relColumnOriginSet) {
                    // 源表信息
                    RelOptTable table = relColumnOrigin.getOriginTable();
                    String sourceTable = String.join(".", table.getQualifiedName());
                    // 源字段
                    int ordinal = relColumnOrigin.getOriginColumnOrdinal();
                    List<String> fieldNames = ((TableSourceTable) table).contextResolvedTable().getResolvedSchema().getColumnNames();
                    String sourceColumn = fieldNames.get(ordinal);
                    // 输出血缘信息
                    System.out.println("sourceTable: " + sourceTable + ", sourceColumn: " + sourceColumn + ", sinkTable: " + sinkTable + ", targetColumn: " + targetColumn);
                }
            }
        }
    }
}
