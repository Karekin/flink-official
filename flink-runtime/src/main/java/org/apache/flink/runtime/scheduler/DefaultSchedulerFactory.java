/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.DefaultSchedulerComponents.createSchedulerComponents;
import static org.apache.flink.runtime.scheduler.SchedulerBase.computeVertexParallelismStore;

/** Factory for {@link DefaultScheduler}. */
public class DefaultSchedulerFactory implements SchedulerNGFactory {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建SchedulerNG
    */
    @Override
    public SchedulerNG createInstance(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final SlotPoolService slotPoolService,
            final ScheduledExecutorService futureExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final Time rpcTimeout,
            final BlobWriter blobWriter,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final Time slotRequestTimeout,
            final ShuffleMaster<?> shuffleMaster,
            final JobMasterPartitionTracker partitionTracker,
            final ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final FatalErrorHandler fatalErrorHandler,
            final JobStatusListener jobStatusListener,
            final Collection<FailureEnricher> failureEnrichers,
            final BlocklistOperations blocklistOperations)
            throws Exception {

        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * SlotPool 管理Slot的
        */
        final SlotPool slotPool =
                slotPoolService
                        .castInto(SlotPool.class)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The DefaultScheduler requires a SlotPool."));

        /** 用于创建DefaultScheduler的组件。 */
        final DefaultSchedulerComponents schedulerComponents =
                createSchedulerComponents(
                        jobGraph.getJobType(),
                        jobGraph.isApproximateLocalRecoveryEnabled(),
                        jobMasterConfiguration,
                        slotPool,
                        slotRequestTimeout);
        /** 是否重新启动失败任务的策略以及重新启动的延迟 */
        final RestartBackoffTimeStrategy restartBackoffTimeStrategy =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                                jobGraph.getSerializedExecutionConfig()
                                        .deserializeValue(userCodeLoader)
                                        .getRestartStrategy(),
                                jobGraph.getJobConfiguration(),
                                jobMasterConfiguration,
                                jobGraph.isCheckpointingEnabled())
                        .create();
        log.info(
                "Using restart back off time strategy {} for {} ({}).",
                restartBackoffTimeStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());
        /** 创建ExecutionGraph的工厂类 */
        final ExecutionGraphFactory executionGraphFactory =
                new DefaultExecutionGraphFactory(
                        jobMasterConfiguration,
                        userCodeLoader,
                        executionDeploymentTracker,
                        futureExecutor,
                        ioExecutor,
                        rpcTimeout,
                        jobManagerJobMetricGroup,
                        blobWriter,
                        shuffleMaster,
                        partitionTracker);
        /** 清理检查点 */
        final CheckpointsCleaner checkpointsCleaner =
                new CheckpointsCleaner(
                        jobMasterConfiguration.get(CheckpointingOptions.CLEANER_PARALLEL_MODE));
        /** 创建DefaultScheduler */
        return new DefaultScheduler(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                schedulerComponents.getStartUpAction(),
                new ScheduledExecutorServiceAdapter(futureExecutor),
                userCodeLoader,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                schedulerComponents.getSchedulingStrategyFactory(),
                FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(jobMasterConfiguration),
                restartBackoffTimeStrategy,
                /** 对Execution等各种操作、部署、取消、标记失败 */
                new DefaultExecutionOperations(),
                /** 记录对 ExecutionVertex ExecutionVertices 的修改，并允许检查顶点是否已修改。 */
                new ExecutionVertexVersioner(),
                schedulerComponents.getAllocatorFactory(),
                initializationTimestamp,
                mainThreadExecutor,
                (jobId, jobStatus, timestamp) -> {
                    if (jobStatus == JobStatus.RESTARTING) {
                        slotPool.setIsJobRestarting(true);
                    } else {
                        slotPool.setIsJobRestarting(false);
                    }
                    jobStatusListener.jobStatusChanges(jobId, jobStatus, timestamp);
                },
                failureEnrichers,
                executionGraphFactory,
                shuffleMaster,
                rpcTimeout,
                /**  计算并行度并存储到Map结构 */
                computeVertexParallelismStore(jobGraph),
                new DefaultExecutionDeployer.Factory());
    }

    @Override
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return JobManagerOptions.SchedulerType.Default;
    }
}
