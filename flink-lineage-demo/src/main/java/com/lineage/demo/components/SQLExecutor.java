package com.lineage.demo.components;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SQLExecutor {
    private final StreamTableEnvironment tableEnv;
    private final Map<JobID, String> jobSqlMapping = new HashMap<>();
    private final List<JobListener> jobListeners = new ArrayList<>();

    public SQLExecutor(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }

    public JobClient executeAsync(String sql) throws Exception {
        Optional<JobClient> jobClientFuture = tableEnv.executeSql(sql).getJobClient();
        JobClient jobClient = jobClientFuture.get();
        jobSqlMapping.put(jobClient.getJobID(), sql);
        jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
        return jobClient;
    }

    public void registerJobListener(JobListener jobListener) {
        jobListeners.add(jobListener);
    }

    public String getSqlForJob(JobID jobID) {
        return jobSqlMapping.get(jobID);
    }
}
