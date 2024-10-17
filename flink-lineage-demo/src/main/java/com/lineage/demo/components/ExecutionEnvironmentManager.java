package com.lineage.demo.components;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ExecutionEnvironmentManager {
    private final StreamExecutionEnvironment env;
    private final StreamTableEnvironment tableEnv;

    public ExecutionEnvironmentManager() {
        this.env = StreamExecutionEnvironment.createLocalEnvironment();
        this.tableEnv = StreamTableEnvironment.create(env);
    }

    public StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }

    public void createSourceAndSinkTables() {
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
    }
}
