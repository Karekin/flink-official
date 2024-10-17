package com.lineage.demo;

import com.lineage.demo.components.ExecutionEnvironmentManager;
import com.lineage.demo.components.LineageBuilder;
import com.lineage.demo.components.LineageJobListener;
import com.lineage.demo.components.LineageTopologyBuilder;
import com.lineage.demo.components.SQLExecutor;
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
public class LineageDemoV3 {

    public static void main(String[] args) throws Exception {
        // 初始化环境和表
        ExecutionEnvironmentManager environmentManager = new ExecutionEnvironmentManager();
        environmentManager.createSourceAndSinkTables();

        // 构建拓扑和血缘生成器
        LineageTopologyBuilder topologyBuilder = new LineageTopologyBuilder();
        LineageBuilder lineageBuilder = new LineageBuilder(topologyBuilder);

        // SQL 执行和监听
        SQLExecutor sqlExecutor = new SQLExecutor(environmentManager.getTableEnv());
        LineageJobListener jobListener = new LineageJobListener((StreamTableEnvironmentImpl) environmentManager.getTableEnv(), sqlExecutor, lineageBuilder);
        sqlExecutor.registerJobListener(jobListener);

        // 创建中间表
        environmentManager.getTableEnv().executeSql("CREATE TABLE intermediate_table1 (\n" +
                "        f_sequence INT,\n" +
                "        f_random INT,\n" +
                "        f_random_str STRING\n" +
                "    ) WITH (\n" +
                "        'connector' = 'print'\n" +
                "        )");

        environmentManager.getTableEnv().executeSql("CREATE TABLE intermediate_table2 (\n" +
                "        f_sequence INT,\n" +
                "        f_random INT,\n" +
                "        f_random_str STRING\n" +
                "    ) WITH (\n" +
                "        'connector' = 'print'\n" +
                "        )");

        // 多个 SQL 查询，形成复杂的多层拓扑
        String sql1 = "INSERT INTO intermediate_table1 " +
                "SELECT t1.f_sequence1, t1.f_random1, t2.f_random_str1 " +
                "FROM datagen t1 LEFT JOIN datagen t2 ON t1.f_sequence1 = t2.f_sequence1";

        String sql2 = "INSERT INTO intermediate_table2 " +
                "SELECT f_sequence, f_random, f_random_str FROM intermediate_table1 WHERE f_random > 100";

        String sql3 = "INSERT INTO print_table " +
                "SELECT f_sequence, f_random, f_random_str FROM intermediate_table2 WHERE f_random < 400";

        // 异步执行多个 SQL 查询，形成复杂的拓扑结构
        sqlExecutor.executeAsync(sql1);
        sqlExecutor.executeAsync(sql2);
        sqlExecutor.executeAsync(sql3);

        // 打印完整的数据血缘拓扑结构
        System.out.println("Complete Data Lineage Topology in DOT format:");
        System.out.println(topologyBuilder.toDotFormat()); // 调用 toDotFormat 打印完整的拓扑结构
    }
}


