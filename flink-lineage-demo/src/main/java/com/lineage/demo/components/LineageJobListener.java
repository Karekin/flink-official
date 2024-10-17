package com.lineage.demo.components;

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

public class LineageJobListener implements JobListener {

    private final StreamTableEnvironmentImpl tableEnvImpl;
    private final SQLExecutor sqlExecutor;
    private final LineageBuilder lineageBuilder;

    // 在构造函数中传入 LineageBuilder 实例
    public LineageJobListener(StreamTableEnvironmentImpl tableEnvImpl, SQLExecutor sqlExecutor, LineageBuilder lineageBuilder) {
        this.tableEnvImpl = tableEnvImpl;
        this.sqlExecutor = sqlExecutor;
        this.lineageBuilder = lineageBuilder; // 通过参数传入 LineageBuilder
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        if (throwable != null) {
            System.err.println("Job submission failed: " + throwable.getMessage());
        } else {
            System.out.println("Job submitted successfully: " + jobClient.getJobID());

            // 获取 SQL
            String sql = sqlExecutor.getSqlForJob(jobClient.getJobID());

            // 解析 SQL
            Tuple2<String, RelNode> parsed = lineageBuilder.parseStatement(tableEnvImpl, sql);
            String sinkTable = parsed.f0;
            RelNode oriRelNode = parsed.f1;

            // 使用已构造的 LineageBuilder 实例生成血缘信息
            lineageBuilder.buildFiledLineageResult(tableEnvImpl, sinkTable, oriRelNode);
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


