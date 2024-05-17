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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistContext;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatReceiver;
import org.apache.flink.runtime.heartbeat.HeartbeatSender;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.BlocklistDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.JobShuffleContextImpl;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation.ResolutionMode;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.deserializeTaskStateSnapshot;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single {@link
 * JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 *
 * <ul>
 *   <li>{@link #updateTaskExecutionState} updates the task execution state for given task
 * </ul>
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * JobMaster负责管理整个作业，盛情资源提交任务运行，心跳等
*/
public class JobMaster extends FencedRpcEndpoint<JobMasterId>
        implements JobMasterGateway, JobMasterService {

    /** Default names for Flink's distributed components. */
    public static final String JOB_MANAGER_NAME = "jobmanager";

    // ------------------------------------------------------------------------
   /** JobMaster 组件中需要的参数，RpcTimeout、slotRequestTimeout*/
    private final JobMasterConfiguration jobMasterConfiguration;
    /** JobMaster的唯一资源ID,区分JobMaster服务 */
    private final ResourceID resourceId;
    /** 当前提交Job对应的JobGraph，从dispatcher获取 */
    private final JobGraph jobGraph;
    /** JobMaster中RPC通信的超时时间 */
    private final Time rpcTimeout;

    /** 高可用服务，用来获取ResourceManagerLeaderRetriever */
    private final HighAvailabilityServices highAvailabilityServices;
    /** 对象数据写入到BlobStore,比如TaskInformation进行持久化 */
    private final BlobWriter blobWriter;
    /** 心跳服务，用来创建TaskManager、ResourceManager组件之间的心跳服务 */
    private final HeartbeatServices heartbeatServices;
    /** 带延迟定时调度的线程池 执行定时任务的*/
    private final ScheduledExecutorService futureExecutor;

    /** 提交运行一个任务 */
    private final Executor ioExecutor;
    /** Flink作业达到终端状态后用于完成操作的接口 */
    private final OnCompletionActions jobCompletionActions;
    /** 定义系统异常处理的Handle实现类 */
    private final FatalErrorHandler fatalErrorHandler;
    /** JobGraph中对应的类加载器，用来加载和实例化用户编写的代码*/
    private final ClassLoader userCodeLoader;
    /** 用于管理SlotPool的服务 */
    private final SlotPoolService slotPoolService;
    /** 初始化时间 */
    private final long initializationTimestamp;

    private final boolean retrieveTaskManagerHostName;

    // --------- ResourceManager --------
    /**
     * 用于获取ResourceManagerLeader节点组件，然后通过resourceManagerLeaderRetriever进行监控
     * ，当ResourceManagerLeader节点状态发生变化了,会触发ResourceManagerLeaderListener.notifyLeaderAddress
     * 返回最新的Leader节点
     */
    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // --------- TaskManagers --------
    /**
     * 注册在JobManager中的TaskExecutor信息。当新的TaskExecutor启动时候，通知JobLeaderService中的监听器。
     * 将TaskExecutor注册在registeredTaskManagers
     */
    private final Map<ResourceID, TaskManagerRegistration> registeredTaskManagers;
    /**
     * 注册管理任务的Shuffle信息，比如注册Job、卸载job
     */
    private final ShuffleMaster<?> shuffleMaster;

    // --------- Scheduler --------
    /**调度Flink作业接口 任务调度器*/
    private final SchedulerNG schedulerNG;
    /**
     * Job状态监听器，当job状态发生改变后的异步操作。比如job执行完毕了，从高可用存储中一处当前job信息
     */
    private final JobManagerJobStatusListener jobStatusListener;
    /**
     * Job中产生的metric监控之变，通过MetricGroup管理和存储
     */
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    // -------- Misc ---------

    private final Map<String, Object> accumulators;
    /** 追踪Job中的Partition信息，startTrackingPartition、stopTrackingAndReleasePartitions
     *  启动和释放Taskexecutor、ShuffleMaster中的Partition信息
     */
    private final JobMasterPartitionTracker partitionTracker;

    /** 跟进已部署执行的Execution */
    private final ExecutionDeploymentTracker executionDeploymentTracker;
    /** 用于协调执行的部署状态的组件 */
    private final ExecutionDeploymentReconciler executionDeploymentReconciler;
    /** Failure Enricher启用自定义逻辑，并以标签的形式将元数据附加到JobMaster中跟踪的每种类型的故障。 */
    private final Collection<FailureEnricher> failureEnrichers;

    // -------- Mutable fields ---------
    /** ResourceManager的地址信息*/
    @Nullable private ResourceManagerAddress resourceManagerAddress;
    /**
     * 创建与ResourceManager的RPC链接，JobManager启动的时候会调用registerJobManager方法向
     * ResourceManager注册
     */
    @Nullable private ResourceManagerConnection resourceManagerConnection;
    /**
     * 维护JobManager与ResourceManager之间的链接信息，内部包括ResourceManagerGateway、ResourceID
     */
    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    /**
     * TaskExecutorToJobManagerHeartbeatPayload 从TaskExecutor发送到JobManager的检测信号的有效负载。
     * AllocatedSlotReport JobMaster从给定的TaskExecutor中当前分配的插槽的报告。此报告会定期发送到TaskExecutor，以便协调插槽分配的内部状态。
     */
    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            taskManagerHeartbeatManager;
    /**
     * 心跳管理器
     */
    private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;
    /** 这个类负责管理所有｛@link BlockedNode｝并在资源上执行它们。 */
    private final BlocklistHandler blocklistHandler;

    // ------------------------------------------------------------------------

    public JobMaster(
            RpcService rpcService,
            JobMasterId jobMasterId,
            JobMasterConfiguration jobMasterConfiguration,
            ResourceID resourceId,
            JobGraph jobGraph,
            HighAvailabilityServices highAvailabilityService,
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            JobManagerSharedServices jobManagerSharedServices,
            HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobMetricGroupFactory,
            OnCompletionActions jobCompletionActions,
            FatalErrorHandler fatalErrorHandler,
            ClassLoader userCodeLoader,
            ShuffleMaster<?> shuffleMaster,
            PartitionTrackerFactory partitionTrackerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ExecutionDeploymentReconciler.Factory executionDeploymentReconcilerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            Collection<FailureEnricher> failureEnrichers,
            long initializationTimestamp)
            throws Exception {

        super(rpcService, RpcServiceUtils.createRandomName(JOB_MANAGER_NAME), jobMasterId);
        /** 用于在状态不匹配的情况下触发操作的接口 */
        final ExecutionDeploymentReconciliationHandler executionStateReconciliationHandler =
                new ExecutionDeploymentReconciliationHandler() {

                    @Override
                    public void onMissingDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Failing deployments {} due to no longer being deployed.",
                                executionAttemptIds);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            schedulerNG.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId,
                                            ExecutionState.FAILED,
                                            new FlinkException(
                                                    String.format(
                                                            "Execution %s is unexpectedly no longer running on task executor %s.",
                                                            executionAttemptId, host))));
                        }
                    }

                    @Override
                    public void onUnknownDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Canceling left-over deployments {} on task executor {}.",
                                executionAttemptIds,
                                host);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            TaskManagerRegistration taskManagerRegistration =
                                    registeredTaskManagers.get(host);
                            if (taskManagerRegistration != null) {
                                taskManagerRegistration
                                        .getTaskExecutorGateway()
                                        .cancelTask(executionAttemptId, rpcTimeout);
                            }
                        }
                    }
                };
        /** 跟进已部署执行的Execution */
        this.executionDeploymentTracker = executionDeploymentTracker;
        /** 用于协调执行的部署状态的组件 */
        this.executionDeploymentReconciler =
                executionDeploymentReconcilerFactory.create(executionStateReconciliationHandler);
        /** JobMaster配置 */
        this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
        /** JobMaster的唯一资源ID,区分JobMaster服务 */
        this.resourceId = checkNotNull(resourceId);
        /** JobGraph */
        this.jobGraph = checkNotNull(jobGraph);
        /** RPC超时时间 */
        this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
        /** 高可用服务 */
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        /** 用于将对象数据写入BlobStore,主要是TaskInfomation的信息持久化 */
        this.blobWriter = jobManagerSharedServices.getBlobWriter();
        /** 待延迟、定时调度的线程池 */
        this.futureExecutor = jobManagerSharedServices.getFutureExecutor();
        /** 提交运行一个任务 */
        this.ioExecutor = jobManagerSharedServices.getIoExecutor();
        /** Flink作业达到终端状态后用于完成操作的接口。 */
        this.jobCompletionActions = checkNotNull(jobCompletionActions);
        /** 致命错误 */
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.initializationTimestamp = initializationTimestamp;
        /**  获取jobmanager.retrieve-taskmanager-hostname */
        this.retrieveTaskManagerHostName =
                jobMasterConfiguration
                        .getConfiguration()
                        .get(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);
        /** 创建jobName */
        final String jobName = jobGraph.getName();
        /** 创建jobName */
        final JobID jid = jobGraph.getJobID();

        log.info("Initializing job '{}' ({}).", jobName, jid);
        /** 构建ResourceManager监听的ResourceManager Leader*/
        resourceManagerLeaderRetriever =
                highAvailabilityServices.getResourceManagerLeaderRetriever();

        this.registeredTaskManagers = new HashMap<>();
        /** 负责管理所有BlockedNode并在资源上执行它们。 */
        this.blocklistHandler =
                blocklistHandlerFactory.create(
                        new JobMasterBlocklistContext(),
                        this::getNodeIdOfTaskManager,
                        getMainThreadExecutor(),
                        log);
        /** 创建SlotPoolService*/
        this.slotPoolService =
                checkNotNull(slotPoolServiceSchedulerFactory)
                        .createSlotPoolService(
                                jid,
                                createDeclarativeSlotPoolFactory(
                                        jobMasterConfiguration.getConfiguration()));
        /** 追终Partition相关的 */
        this.partitionTracker =
                checkNotNull(partitionTrackerFactory)
                        .create(
                                resourceID -> {
                                    return Optional.ofNullable(
                                                    registeredTaskManagers.get(resourceID))
                                            .map(TaskManagerRegistration::getTaskExecutorGateway);
                                });
        /** 注册管理任务的Shuffle信息，比如注册Job、卸载job */
        this.shuffleMaster = checkNotNull(shuffleMaster);
        /** 监控JobManagerMetric*/
        this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
        /** 监控job status*/
        this.jobStatusListener = new JobManagerJobStatusListener();

        this.failureEnrichers = checkNotNull(failureEnrichers);
        /**调度Flink作业接口 任务调度器*/
        this.schedulerNG =
                createScheduler(
                        slotPoolServiceSchedulerFactory,
                        executionDeploymentTracker,
                        jobManagerJobMetricGroup,
                        jobStatusListener);
        /** 心跳服务 */
        this.heartbeatServices = checkNotNull(heartbeatServices);
        /** 心跳管理*/
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        /** 心跳服务 */
        this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        /** 链接ResourceManager对象*/
        this.resourceManagerConnection = null;
        /**  与ResourceManagers*/
        this.establishedResourceManagerConnection = null;

        this.accumulators = new HashMap<>();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建作业调度器
    */
    private SchedulerNG createScheduler(
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            JobStatusListener jobStatusListener)
            throws Exception {
        /**
         * 通过工厂方式创建SchedulerNG
         */
        final SchedulerNG scheduler =
                slotPoolServiceSchedulerFactory.createScheduler(
                        log,
                        jobGraph,
                        ioExecutor,
                        jobMasterConfiguration.getConfiguration(),
                        slotPoolService,
                        futureExecutor,
                        userCodeLoader,
                        highAvailabilityServices.getCheckpointRecoveryFactory(),
                        rpcTimeout,
                        blobWriter,
                        jobManagerJobMetricGroup,
                        jobMasterConfiguration.getSlotRequestTimeout(),
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentTracker,
                        initializationTimestamp,
                        getMainThreadExecutor(),
                        fatalErrorHandler,
                        jobStatusListener,
                        failureEnrichers,
                        blocklistHandler::addNewBlockedNodes);
        /** 返回SchedulerNG对象 */
        return scheduler;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个不主动发送检测信号的检测信号管理器。
    */
    private HeartbeatManager<Void, Void> createResourceManagerHeartbeatManager(
            HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个检测信号管理器，该管理器主动向监视目标发送检测信号。
     */
    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            createTaskManagerHeartbeatManager(HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManagerSender(
                resourceId, new TaskManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private DeclarativeSlotPoolFactory createDeclarativeSlotPoolFactory(
            Configuration configuration) {
        if (BlocklistUtils.isBlocklistEnabled(configuration)) {
            return new BlocklistDeclarativeSlotPoolFactory(blocklistHandler::isBlockedTaskManager);
        } else {
            return new DefaultDeclarativeSlotPoolFactory();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 启动内部服务
    */
    @Override
    protected void onStart() throws JobMasterException {
        try {
            /**
             * 方法内部首先尝试调用startJobExecution()方法，实际启动作业执行的操作。
             */
            startJobExecution();
        } catch (Exception e) {
            /**
             * 如果startJobExecution()方法抛出任何异常（Exception类型或其子类），则捕获这个异常。
             * 异常处理分几种？
             */
            final JobMasterException jobMasterException =
                    new JobMasterException("Could not start the JobMaster.", e);
            handleJobMasterError(jobMasterException);
            throw jobMasterException;
        }
    }

    /** Suspend the job and shutdown all other services including rpc. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info(
                "Stopping the JobMaster for job '{}' ({}).",
                jobGraph.getName(),
                jobGraph.getJobID());

        // make sure there is a graceful exit
        return stopJobExecution(
                        new FlinkException(
                                String.format(
                                        "Stopping JobMaster for job '%s' (%s).",
                                        jobGraph.getName(), jobGraph.getJobID())))
                .exceptionally(
                        exception -> {
                            throw new CompletionException(
                                    new JobMasterException(
                                            "Could not properly stop the JobMaster.", exception));
                        });
    }

    // ----------------------------------------------------------------------------------------------
    // RPC methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        schedulerNG.cancel();

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task
     * @return Acknowledge the task execution state update
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 更新任务的执行状态。
     * @param taskExecutionState 要更新的任务执行状态
     * @return 一个CompletableFuture，包含更新结果（成功时为Acknowledge，失败时为异常）
    */
    @Override
    public CompletableFuture<Acknowledge> updateTaskExecutionState(
            final TaskExecutionState taskExecutionState) {
        // 用于存储更新过程中可能发生的异常
        FlinkException taskExecutionException;
        try {
            // 检查taskExecutionState是否为null
            checkNotNull(taskExecutionState, "taskExecutionState");

            // 调用schedulerNG的updateTaskExecutionState方法来更新任务执行状态
            if (schedulerNG.updateTaskExecutionState(taskExecutionState)) {
                // 如果更新成功，则返回一个已完成的CompletableFuture，其中包含Acknowledge对象
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                // 如果更新失败（没有找到对应的执行尝试），则创建一个ExecutionGraphException异常
                taskExecutionException =
                        new ExecutionGraphException(
                                "The execution attempt "
                                        + taskExecutionState.getID()
                                        + " was not found.");
            }
        } catch (Exception e) {
            // 如果在更新过程中捕获到异常，则创建一个JobMasterException异常，并传入原始异常作为原因
            taskExecutionException =
                    new JobMasterException(
                            "Could not update the state of task execution for JobMaster.", e);
            // 处理JobMaster错误（可能是记录日志、发送告警等）
            handleJobMasterError(taskExecutionException);
        }
        // 如果发生异常或更新失败，则返回一个已完成的CompletableFuture，其中包含异常
        return FutureUtils.completedExceptionally(taskExecutionException);
    }

    @Override
    public void notifyEndOfData(final ExecutionAttemptID executionAttempt) {
        schedulerNG.notifyEndOfData(executionAttempt);
    }

    @Override
    public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            final JobVertexID vertexID, final ExecutionAttemptID executionAttempt) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestNextInputSplit(vertexID, executionAttempt));
        } catch (IOException e) {
            log.warn("Error while requesting next input split", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<ExecutionState> requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestPartitionState(intermediateResultId, resultPartitionId));
        } catch (PartitionProducerDisposedException e) {
            log.info("Error while requesting partition state", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectTaskManager(
            final ResourceID resourceID, final Exception cause) {
        log.info(
                "Disconnect TaskExecutor {} because: {}",
                resourceID.getStringWithMetadata(),
                cause.getMessage(),
                ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
        ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);

        taskManagerHeartbeatManager.unmonitorTarget(resourceID);
        slotPoolService.releaseTaskManager(resourceID, cause);
        partitionTracker.stopTrackingPartitionsFor(resourceID);

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.remove(resourceID);

        if (taskManagerRegistration != null) {
            taskManagerRegistration
                    .getTaskExecutorGateway()
                    .disconnectJobManager(jobGraph.getJobID(), cause);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // TODO: This method needs a leader session ID
    @Override
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            @Nullable final SerializedValue<TaskStateSnapshot> checkpointState) {
        schedulerNG.acknowledgeCheckpoint(
                jobID,
                executionAttemptID,
                checkpointId,
                checkpointMetrics,
                deserializeTaskStateSnapshot(checkpointState, getClass().getClassLoader()));
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {

        schedulerNG.reportCheckpointMetrics(
                jobID, executionAttemptID, checkpointId, checkpointMetrics);
    }

    @Override
    public void reportInitializationMetrics(
            JobID jobId, SubTaskInitializationMetrics initializationMetrics) {
        schedulerNG.reportInitializationMetrics(jobId, initializationMetrics);
    }

    // TODO: This method needs a leader session ID
    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        schedulerNG.declineCheckpoint(decline);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 接收到Task发送的OperatorEvent事件
    */
    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            final ExecutionAttemptID task,
            final OperatorID operatorID,
            final SerializedValue<OperatorEvent> serializedEvent) {

        try {
            final OperatorEvent evt = serializedEvent.deserializeValue(userCodeLoader);
            schedulerNG.deliverOperatorEventToCoordinator(task, operatorID, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> serializedRequest) {
        try {
            final CoordinationRequest request = serializedRequest.deserializeValue(userCodeLoader);
            return schedulerNG.deliverCoordinationRequestToCoordinator(operatorID, request);
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<KvStateLocation> requestKvStateLocation(
            final JobID jobId, final String registrationName) {
        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestKvStateLocation(jobId, registrationName));
        } catch (UnknownKvStateLocation | FlinkJobNotFoundException e) {
            log.info("Error while request key-value state location", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress) {

        try {
            schedulerNG.notifyKvStateRegistered(
                    jobId,
                    jobVertexId,
                    keyGroupRange,
                    registrationName,
                    kvStateId,
                    kvStateServerAddress);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName) {
        try {
            schedulerNG.notifyKvStateUnregistered(
                    jobId, jobVertexId, keyGroupRange, registrationName);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state de-registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 
    */
    @Override
    public CompletableFuture<Collection<SlotOffer>> offerSlots(
            final ResourceID taskManagerId, final Collection<SlotOffer> slots, final Time timeout) {

        // 从已注册的任务管理器集合中，根据taskManagerId获取对应的TaskManagerRegistration对象
        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.get(taskManagerId);

        // 如果找不到对应的TaskManagerRegistration对象
        if (taskManagerRegistration == null) {
            // 返回一个已完成的CompletableFuture对象，但其中包含了一个异常，异常信息为"Unknown TaskManager"加上传入的taskManagerId
            return FutureUtils.completedExceptionally(
                    new Exception("Unknown TaskManager " + taskManagerId));
        }
        // 创建一个新的RpcTaskManagerGateway对象，该对象封装了与TaskManager通信的接口
        final RpcTaskManagerGateway rpcTaskManagerGateway =
                new RpcTaskManagerGateway(
                        taskManagerRegistration.getTaskExecutorGateway(), getFencingToken());
         // 调用slotPoolService的offerSlots方法，
        // 返回一个已完成的CompletableFuture对象，其中包含slotPoolService.offerSlots的调用结果
        return CompletableFuture.completedFuture(
                slotPoolService.offerSlots(
                        taskManagerRegistration.getTaskManagerLocation(),
                        rpcTaskManagerGateway,
                        slots));
    }

    @Override
    public void failSlot(
            final ResourceID taskManagerId,
            final AllocationID allocationId,
            final Exception cause) {

        if (registeredTaskManagers.containsKey(taskManagerId)) {
            internalFailAllocation(taskManagerId, allocationId, cause);
        } else {
            log.warn(
                    "Cannot fail slot "
                            + allocationId
                            + " because the TaskManager "
                            + taskManagerId
                            + " is unknown.");
        }
    }

    private void internalFailAllocation(
            @Nullable ResourceID resourceId, AllocationID allocationId, Exception cause) {
        final Optional<ResourceID> resourceIdOptional =
                slotPoolService.failAllocation(resourceId, allocationId, cause);
        resourceIdOptional.ifPresent(
                taskManagerId -> {
                    if (!partitionTracker.isTrackingPartitionsFor(taskManagerId)) {
                        releaseEmptyTaskManager(taskManagerId);
                    }
                });
    }

    private void releaseEmptyTaskManager(ResourceID resourceId) {
        disconnectTaskManager(
                resourceId,
                new FlinkException(
                        String.format(
                                "No more slots registered at JobMaster %s.",
                                resourceId.getStringWithMetadata())));
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerTaskManager(
            final JobID jobId,
            final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            final Time timeout) {

        if (!jobGraph.getJobID().equals(jobId)) {
            log.debug(
                    "Rejecting TaskManager registration attempt because of wrong job id {}.",
                    jobId);
            return CompletableFuture.completedFuture(
                    new JMTMRegistrationRejection(
                            String.format(
                                    "The JobManager is not responsible for job %s. Maybe the TaskManager used outdated connection information.",
                                    jobId)));
        }

        final TaskManagerLocation taskManagerLocation;
        try {
            taskManagerLocation =
                    resolveTaskManagerLocation(
                            taskManagerRegistrationInformation.getUnresolvedTaskManagerLocation());
        } catch (FlinkException exception) {
            log.error("Could not accept TaskManager registration.", exception);
            return CompletableFuture.completedFuture(new RegistrationResponse.Failure(exception));
        }

        final ResourceID taskManagerId = taskManagerLocation.getResourceID();
        final UUID sessionId = taskManagerRegistrationInformation.getTaskManagerSession();
        final TaskManagerRegistration taskManagerRegistration =
                registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration != null) {
            if (taskManagerRegistration.getSessionId().equals(sessionId)) {
                log.debug(
                        "Ignoring registration attempt of TaskManager {} with the same session id {}.",
                        taskManagerId,
                        sessionId);
                final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
                return CompletableFuture.completedFuture(response);
            } else {
                disconnectTaskManager(
                        taskManagerId,
                        new FlinkException(
                                String.format(
                                        "A registered TaskManager %s re-registered with a new session id. This indicates a restart of the TaskManager. Closing the old connection.",
                                        taskManagerId)));
            }
        }

        return getRpcService()
                .connect(
                        taskManagerRegistrationInformation.getTaskManagerRpcAddress(),
                        TaskExecutorGateway.class)
                .handleAsync(
                        (TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
                            if (throwable != null) {
                                return new RegistrationResponse.Failure(throwable);
                            }

                            slotPoolService.registerTaskManager(taskManagerId);
                            registeredTaskManagers.put(
                                    taskManagerId,
                                    TaskManagerRegistration.create(
                                            taskManagerLocation, taskExecutorGateway, sessionId));

                            // monitor the task manager as heartbeat target
                            taskManagerHeartbeatManager.monitorTarget(
                                    taskManagerId,
                                    new TaskExecutorHeartbeatSender(taskExecutorGateway));

                            return new JMTMRegistrationSuccess(resourceId);
                        },
                        getMainThreadExecutor());
    }

    @Nonnull
    private TaskManagerLocation resolveTaskManagerLocation(
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) throws FlinkException {
        try {
            if (retrieveTaskManagerHostName) {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.RETRIEVE_HOST_NAME);
            } else {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.USE_IP_ONLY);
            }
        } catch (Throwable throwable) {
            final String errMsg =
                    String.format(
                            "TaskManager address %s cannot be resolved. %s",
                            unresolvedTaskManagerLocation.getExternalAddress(),
                            throwable.getMessage());
            throw new FlinkException(errMsg, throwable);
        }
    }

    @Override
    public void disconnectResourceManager(
            final ResourceManagerId resourceManagerId, final Exception cause) {

        if (isConnectingToResourceManager(resourceManagerId)) {
            reconnectToResourceManager(cause);
        }
    }

    private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
        return resourceManagerAddress != null
                && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
        return taskManagerHeartbeatManager.receiveHeartbeat(resourceID, payload);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(final ResourceID resourceID) {
        return resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobStatus());
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJob());
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestCheckpointStats());
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            final CheckpointType checkpointType, final Time timeout) {
        return schedulerNG.triggerCheckpoint(checkpointType);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            final Time timeout) {

        return schedulerNG.triggerSavepoint(targetDirectory, cancelJob, formatType);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            final Time timeout) {

        return schedulerNG.stopWithSavepoint(targetDirectory, terminate, formatType);
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        slotPoolService.notifyNotEnoughResourcesAvailable(acquiredResources);
    }

    @Override
    public CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {

        AggregateFunction aggregateFunction = null;
        try {
            aggregateFunction =
                    InstantiationUtil.deserializeObject(
                            serializedAggregateFunction, userCodeLoader);
        } catch (Exception e) {
            log.error("Error while attempting to deserialize user AggregateFunction.");
            return FutureUtils.completedExceptionally(e);
        }

        Object accumulator = accumulators.get(aggregateName);
        if (null == accumulator) {
            accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add(aggregand, accumulator);
        accumulators.put(aggregateName, accumulator);
        return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return this.sendRequestToCoordinator(operatorId, serializedRequest);
    }

    @Override
    public CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            partitionTracker.stopTrackingAndReleasePartitions(partitionIds, false);
            future.complete(null);
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        blocklistHandler.addNewBlockedNodes(newNodes);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<JobResourceRequirements> requestJobResourceRequirements() {
        return CompletableFuture.completedFuture(schedulerNG.requestJobResourceRequirements());
    }

    @Override
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements) {
        schedulerNG.updateJobResourceRequirements(jobResourceRequirements);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------------------------------
    // Internal methods
    // ----------------------------------------------------------------------------------------------

    // -- job starting and stopping
    // -----------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 启动服务触发调度
    */
    private void startJobExecution() throws Exception {
        /** 检查是否在主线程中执行 */
        validateRunsInMainThread();
        /**
         * 作业级shuffle上下文可以提供一些作业信息，如作业ID，shuffle插件通过它通知作业停止跟踪丢失的结果分区。
         * 跟踪和管理作业的状态。
         */
        JobShuffleContext context = new JobShuffleContextImpl(jobGraph.getJobID(), this);
        /**
         * 向shuffleMaster注册JobShuffleContext
         */
        shuffleMaster.registerJob(context);
        /** 启动作业主服务 */
        startJobMasterServices();

        log.info(
                "Starting execution of job '{}' ({}) under job master id {}.",
                jobGraph.getName(),
                jobGraph.getJobID(),
                getFencingToken());
        /** 开始作业的调度。启动作业的任务分配和执行计划 */
        startScheduling();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private void startJobMasterServices() throws Exception {
        try {
            /**
             * 调用createTaskManagerHeartbeatManager方法，并传入heartbeatServices作为参数，
             * 以创建任务管理器（TaskManager）的心跳管理器。
             */
            this.taskManagerHeartbeatManager = createTaskManagerHeartbeatManager(heartbeatServices);
            /**
             * 创建资源管理器（ResourceManager）的心跳管理器。
             */
            this.resourceManagerHeartbeatManager =
                    createResourceManagerHeartbeatManager(heartbeatServices);

            // start the slot pool make sure the slot pool now accepts messages for this leader
            /**
             * 启动插slotPool 服务
             */
            slotPoolService.start(getFencingToken(), getAddress(), getMainThreadExecutor());

            // job is ready to go, try to establish connection with resource manager
            //   - activate leader retrieval for the resource manager
            //   - on notification of the leader, the connection will be established and
            //     the slot pool will start requesting slots
            /**
             * 尝试与资源管理器建立连接。在收到leader的通知后，为资源管理器激活leader Retriever，连接将建立，插槽池将开始请求插槽
             * JobMaster内部也会像ResourceManager注册
             */
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
        } catch (Exception e) {
            /**
             * 异常情况霞调用
             */
            handleStartJobMasterServicesError(e);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理在启动作业主节点服务过程中出现的异常
    */
    private void handleStartJobMasterServicesError(Exception e) throws Exception {
        try {
            /**
             * 停止作业主节点服务
             */
            stopJobMasterServices();
        } catch (Exception inner) {
            /**
             * 如果在停止服务的过程中又抛出了异常（inner），则使用e.addSuppressed(inner)将内部异常添加到原始异常e的抑制异常列表中。
             */
            e.addSuppressed(inner);
        }

        throw e;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 停止作业主节点相关的服务
    */
    private void stopJobMasterServices() throws Exception {
        Exception resultingException = null;

        try {
            /**
             * resourceManagerLeaderRetriever停止对ResourceManager Leader变化的监听
             */
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            resultingException = e;
        }

        // TODO: Distinguish between job termination which should free all slots and a loss of
        // leadership which should keep the slots
        /** 关闭Slot池服务 */
        slotPoolService.close();
        /** 停止心跳服务 */
        stopHeartbeatServices();
        /**
         * 使用ExceptionUtils的tryRethrowException方法来尝试重新抛出resultingException。
         * 如果resultingException不为null，则该方法会抛出这个异常；否则，什么都不会发生。
         */
        ExceptionUtils.tryRethrowException(resultingException);
    }

    private CompletableFuture<Void> stopJobExecution(final Exception cause) {
        validateRunsInMainThread();

        final CompletableFuture<Void> terminationFuture = stopScheduling();

        return FutureUtils.runAfterwards(
                terminationFuture,
                () -> {
                    shuffleMaster.unregisterJob(jobGraph.getJobID());
                    disconnectTaskManagerResourceManagerConnections(cause);
                    stopJobMasterServices();
                });
    }

    private void disconnectTaskManagerResourceManagerConnections(Exception cause) {
        // disconnect from all registered TaskExecutors
        final Set<ResourceID> taskManagerResourceIds =
                new HashSet<>(registeredTaskManagers.keySet());

        for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
            disconnectTaskManager(taskManagerResourceId, cause);
        }

        // disconnect from resource manager:
        closeResourceManagerConnection(cause);
    }

    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        resourceManagerHeartbeatManager.stop();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 启动SchedulerNG调度器（scheduler）
    */
    private void startScheduling() {
        /** 调用SchedulerNG startScheduling启动调度器 */
        schedulerNG.startScheduling();
    }

    private CompletableFuture<Void> stopScheduling() {
        jobManagerJobMetricGroup.close();
        jobStatusListener.stop();

        return schedulerNG.closeAsync();
    }

    // ----------------------------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理JobMaster中发生的错误
    */
    private void handleJobMasterError(final Throwable cause) {
        /**
         * 使用ExceptionUtils.isJvmFatalError(cause)来判断发生的错误是否是JVM致命错误。
         * ExceptionUtils是一个工具类，用于处理异常相关的操作。
         */
        if (ExceptionUtils.isJvmFatalError(cause)) {
            log.error("Fatal error occurred on JobManager.", cause);
            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            /**
             * 致命错误处理程序实现应确保此调用是非阻塞的
             * 调用fatalErrorHandler.onFatalError(cause)来处理致命错误。
             */
            fatalErrorHandler.onFatalError(cause);
        } else {
            /**
             * 调用jobCompletionActions.jobMasterFailed(cause)来通知作业完成动作处理器jobCompletionActions，
             */
            jobCompletionActions.jobMasterFailed(cause);
        }
    }

    private void jobStatusChanged(final JobStatus newJobStatus) {
        /** 判断是否主线程中执行  */
        validateRunsInMainThread();
        /** 判断作业状态是否为全局状态 */
        if (newJobStatus.isGloballyTerminalState()) {
            CompletableFuture<Void> partitionPromoteFuture;
            /** 如果是完成状态 */
            if (newJobStatus == JobStatus.FINISHED) {
                /** 获取所有ResultPartitionID 集合 */
                Collection<ResultPartitionID> jobPartitions =
                        partitionTracker.getAllTrackedNonClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                /** 停止跟踪并释放分区 */
                partitionTracker.stopTrackingAndReleasePartitions(jobPartitions);
                /** 获取所有ResultPartitionID 集合 */
                Collection<ResultPartitionID> clusterPartitions =
                        partitionTracker.getAllTrackedClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                /** 停止跟踪并释放分区 */
                partitionPromoteFuture =
                        partitionTracker.stopTrackingAndPromotePartitions(clusterPartitions);
                /** 如果是未完成状态 */
            } else {
                /** 获取所有ResultPartitionID 集合 */
                Collection<ResultPartitionID> allTracked =
                        partitionTracker.getAllTrackedPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                /** 停止跟踪并释放分区 */
                partitionTracker.stopTrackingAndReleasePartitions(allTracked);
                partitionPromoteFuture = CompletableFuture.completedFuture(null);
            }

            final ExecutionGraphInfo executionGraphInfo = schedulerNG.requestJob();

            futureExecutor.execute(
                    () -> {
                        try {
                            partitionPromoteFuture.get();
                        } catch (Throwable e) {
                            // We do not want to fail the job in case of partition releasing and
                            // promoting fail. The TaskExecutors will release the partitions
                            // eventually when they find out the JobMaster is closed.
                            log.warn("Fail to release or promote partitions", e);
                        }
                        jobCompletionActions.jobReachedGloballyTerminalState(executionGraphInfo);
                    });
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 重新连接ResourceManager进行注册
    */
    private void notifyOfNewResourceManagerLeader(
            final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
        /**
         * 获取ResourceManager地址
         */
        resourceManagerAddress =
                createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        /**
         * reconnectToResourceManager：重新链接ResourceManager
         * 创建了一个新的 `FlinkException` 异常对象，异常信息包含格式化后的字符串，表示资源管理器领导者的地址发生了变化。
         */
        reconnectToResourceManager(
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newResourceManagerAddress,
            @Nullable ResourceManagerId resourceManagerId) {
        if (newResourceManagerAddress != null) {
            // the contract is: address == null <=> id == null
            checkNotNull(resourceManagerId);
            return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        } else {
            return null;
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 重新链接ResourceManager
    */
    private void reconnectToResourceManager(Exception cause) {
        /**
         * 关闭资源管理器连接
         * 先把历史建立的链接关闭
         */
        closeResourceManagerConnection(cause);
        /**
         * 试图重新链接ResourceManager
         */
        tryConnectToResourceManager();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 链接ResourceManager，内部会进行注册
    */
    private void tryConnectToResourceManager() {
        /** 检查资源管理器地址 */
        if (resourceManagerAddress != null) {
            /** 调用connectToResourceManager*/
            connectToResourceManager();
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建ResourceManagerConnection链接并启动进行注册
    */
    private void connectToResourceManager() {
        /**
         * assert 语句进行前置条件检查。这些断言用于确保方法执行前满足以下条件：
         * resourceManagerAddress 不为 null，即资源管理器地址是有效的。
         * resourceManagerConnection 为 null，即当前没有正在进行的资源管理器连接。
         * establishedResourceManagerConnection 为 null，即尚未建立有效的资源管理器连接。
         */
        assert (resourceManagerAddress != null);
        assert (resourceManagerConnection == null);
        assert (establishedResourceManagerConnection == null);
        /**
         * 打印日志
         */
        log.info("Connecting to ResourceManager {}", resourceManagerAddress);
        /** 创建 ResourceManagerConnection 对象 与ResourceManager建立链接 */
        resourceManagerConnection =
                new ResourceManagerConnection(
                        log,
                        jobGraph.getJobID(),
                        resourceId,
                        getAddress(),
                        getFencingToken(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        futureExecutor);
        /**
         * start 方法来启动 resourceManagerConnection 进行注册
         */
        resourceManagerConnection.start();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
        /**
         * 从 success 对象中获取资源管理器的ID。
         */
        final ResourceManagerId resourceManagerId = success.getResourceManagerId();

        /**
         * resourceManagerConnection 是否不为空，并且其目标领导者ID是否与从 success 中获取的资源管理器ID相等。
         */
        // verify the response with current connection
        if (resourceManagerConnection != null
                && Objects.equals(
                        resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

            log.info(
                    "JobManager successfully registered at ResourceManager, leader id: {}.",
                    resourceManagerId);
            /**
             * 从当前资源管理器连接中获取资源管理器的网关。
             */
            final ResourceManagerGateway resourceManagerGateway =
                    resourceManagerConnection.getTargetGateway();
            /**
             * 从 success 对象中获取资源管理器的资源ID。
             */
            final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();
            /**
             * 使用获取到的资源管理器网关和资源ID创建一个新的 EstablishedResourceManagerConnection 对象。
             */
            establishedResourceManagerConnection =
                    new EstablishedResourceManagerConnection(
                            resourceManagerGateway, resourceManagerResourceId);
            /**
             * 注册一个新的阻止列表监听器
             */
            blocklistHandler.registerBlocklistListener(resourceManagerGateway);
            /**
             * 连接槽池服务到资源管理器。
             */
            slotPoolService.connectToResourceManager(resourceManagerGateway);
            /**
             * 连接分区追踪器到资源管理器。
             */
            partitionTracker.connectToResourceManager(resourceManagerGateway);
            /**
             * 开始监控心跳目标
             */
            resourceManagerHeartbeatManager.monitorTarget(
                    resourceManagerResourceId,
                    new ResourceManagerHeartbeatReceiver(resourceManagerGateway));
        } else {
            log.debug(
                    "Ignoring resource manager connection to {} because it's duplicated or outdated.",
                    resourceManagerId);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 关闭ResourceManager对应的链接
    */
    private void closeResourceManagerConnection(Exception cause) {
        /**
         * 检查 `establishedResourceManagerConnection` 是否为 `null`。
         */
        if (establishedResourceManagerConnection != null) {
            /**
             * 关闭链接
             */
            dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
            /** 设置establishedResourceManagerConnection 为null */
            establishedResourceManagerConnection = null;
        }
        /**
         * 如果resourceManagerConnection 不为null
         *
         */
        if (resourceManagerConnection != null) {
            // stop a potentially ongoing registration process
            /**
             * 关闭resourceManagerConnection,
             * 并将resourceManagerConnection设置为null
             */
            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 关闭相关服务
    */
    private void dissolveResourceManagerConnection(
            EstablishedResourceManagerConnection establishedResourceManagerConnection,
            Exception cause) {
        /**
         * 获取资源管理器资源ID
         */
        final ResourceID resourceManagerResourceID =
                establishedResourceManagerConnection.getResourceManagerResourceID();
        /**
         * 根据日志级别，记录关闭资源管理器连接的信息。
         */
        if (log.isDebugEnabled()) {
            log.debug(
                    "Close ResourceManager connection {}.",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause);
        } else {
            log.info(
                    "Close ResourceManager connection {}: {}",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause.getMessage());
        }
        /**
         * 取消监控心跳目标，ResourceID是心跳目标的标识
         * 取消对ResourceManager之间相关的心跳
         */
        resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

        ResourceManagerGateway resourceManagerGateway =
                establishedResourceManagerConnection.getResourceManagerGateway();
        /** 断开作业管理器连接 */
        resourceManagerGateway.disconnectJobManager(
                jobGraph.getJobID(), schedulerNG.requestJobStatus(), cause);
        /** 注销阻塞列表监听器 */
        blocklistHandler.deregisterBlocklistListener(resourceManagerGateway);
        /** 断开资源管理器连接 */
        slotPoolService.disconnectResourceManager();
    }

    private String getNodeIdOfTaskManager(ResourceID taskManagerId) {
        checkState(registeredTaskManagers.containsKey(taskManagerId));
        return registeredTaskManagers.get(taskManagerId).getTaskManagerLocation().getNodeId();
    }

    // ----------------------------------------------------------------------------------------------
    // Service methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public JobMasterGateway getGateway() {
        return getSelfGateway(JobMasterGateway.class);
    }

    // ----------------------------------------------------------------------------------------------
    // Utility classes
    // ----------------------------------------------------------------------------------------------

    private static final class TaskExecutorHeartbeatSender
            extends HeartbeatSender<AllocatedSlotReport> {
        private final TaskExecutorGateway taskExecutorGateway;

        private TaskExecutorHeartbeatSender(TaskExecutorGateway taskExecutorGateway) {
            this.taskExecutorGateway = taskExecutorGateway;
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(
                ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            return taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
        }
    }

    private static final class ResourceManagerHeartbeatReceiver extends HeartbeatReceiver<Void> {
        private final ResourceManagerGateway resourceManagerGateway;

        private ResourceManagerHeartbeatReceiver(ResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID, Void payload) {
            return resourceManagerGateway.heartbeatFromJobManager(resourceID);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 监听ResourceManagerLeader leader变化会处罚notifyLeaderAddress
     * 第一次也会处罚执行，
    */
    private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * ResourceManagerLeader改变触发该方法
        */
        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            /** 用于异步执行传入的Lambda表达式或Runnable对象的 */
            runAsync(
                    () ->
                            /**
                             * 调用notifyOfNewResourceManagerLeader进行处理
                             */
                            notifyOfNewResourceManagerLeader(
                                    leaderAddress,
                                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 处理JobMaster中发生的错误
        */
        @Override
        public void handleError(final Exception exception) {
            handleJobMasterError(
                    new Exception("Fatal error in the ResourceManager leader service", exception));
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class ResourceManagerConnection
            extends RegisteredRpcConnection<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection> {
        private final JobID jobID;

        private final ResourceID jobManagerResourceID;

        private final String jobManagerRpcAddress;

        private final JobMasterId jobMasterId;

        ResourceManagerConnection(
                final Logger log,
                final JobID jobID,
                final ResourceID jobManagerResourceID,
                final String jobManagerRpcAddress,
                final JobMasterId jobMasterId,
                final String resourceManagerAddress,
                final ResourceManagerId resourceManagerId,
                final Executor executor) {
            super(log, resourceManagerAddress, resourceManagerId, executor);
            this.jobID = checkNotNull(jobID);
            this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
            this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
            this.jobMasterId = checkNotNull(jobMasterId);
        }

        @Override
        protected RetryingRegistration<
                        ResourceManagerId,
                        ResourceManagerGateway,
                        JobMasterRegistrationSuccess,
                        RegistrationResponse.Rejection>
                generateRegistration() {
            return new RetryingRegistration<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection>(
                    log,
                    getRpcService(),
                    "ResourceManager",
                    ResourceManagerGateway.class,
                    getTargetAddress(),
                    getTargetLeaderId(),
                    jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

                @Override
                protected CompletableFuture<RegistrationResponse> invokeRegistration(
                        ResourceManagerGateway gateway,
                        ResourceManagerId fencingToken,
                        long timeoutMillis) {
                    /** 获取超时时间 */
                    Time timeout = Time.milliseconds(timeoutMillis);
                    /**
                     * 向ResourceManaer注册JobMaster
                     */
                    return gateway.registerJobMaster(
                            jobMasterId,
                            jobManagerResourceID,
                            jobManagerRpcAddress,
                            jobID,
                            timeout);
                }
            };
        }

        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 处理作业主节点（JobMaster）注册成功的回调
        */
        @Override
        protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (this == resourceManagerConnection) {
                            /**
                             * 与ResourceManager建立链接
                             */
                            establishResourceManagerConnection(success);
                        }
                    });
        }

        @Override
        protected void onRegistrationRejection(RegistrationResponse.Rejection rejection) {
            handleJobMasterError(
                    new IllegalStateException(
                            "The ResourceManager should never reject a JobMaster registration."));
        }

        @Override
        protected void onRegistrationFailure(final Throwable failure) {
            handleJobMasterError(failure);
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class JobManagerJobStatusListener implements JobStatusListener {

        private volatile boolean running = true;

        @Override
        public void jobStatusChanges(
                final JobID jobId, final JobStatus newJobStatus, final long timestamp) {

            if (running) {
                // run in rpc thread to avoid concurrency
                runAsync(() -> jobStatusChanged(newJobStatus));
            }
        }

        private void stop() {
            running = false;
        }
    }

    private class TaskManagerHeartbeatListener
            implements HeartbeatListener<
                    TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> {

        @Override
        public void notifyHeartbeatTimeout(ResourceID resourceID) {
            final String message =
                    String.format(
                            "Heartbeat of TaskManager with id %s timed out.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        private void handleTaskManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            disconnectTaskManager(resourceID, cause);
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "TaskManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(
                ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
            validateRunsInMainThread();
            executionDeploymentReconciler.reconcileExecutionDeployments(
                    resourceID,
                    payload.getExecutionDeploymentReport(),
                    executionDeploymentTracker.getExecutionsOn(resourceID));
            for (AccumulatorSnapshot snapshot :
                    payload.getAccumulatorReport().getAccumulatorSnapshots()) {
                schedulerNG.updateAccumulators(snapshot);
            }
        }

        @Override
        public AllocatedSlotReport retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return slotPoolService.createAllocatedSlotReport(resourceID);
        }
    }

    private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            final String message =
                    String.format(
                            "The heartbeat of ResourceManager with id %s timed out.",
                            resourceId.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceId, new TimeoutException(message));
        }

        private void handleResourceManagerConnectionLoss(ResourceID resourceId, Exception cause) {
            validateRunsInMainThread();
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceID()
                            .equals(resourceId)) {
                reconnectToResourceManager(cause);
            }
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "ResourceManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    private class JobMasterBlocklistContext implements BlocklistContext {

        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {
            Set<String> blockedNodeIds =
                    blockedNodes.stream().map(BlockedNode::getNodeId).collect(Collectors.toSet());

            Collection<ResourceID> blockedTaskMangers =
                    registeredTaskManagers.keySet().stream()
                            .filter(
                                    taskManagerId ->
                                            blockedNodeIds.contains(
                                                    getNodeIdOfTaskManager(taskManagerId)))
                            .collect(Collectors.toList());

            blockedTaskMangers.forEach(
                    taskManagerId -> {
                        Exception cause =
                                new FlinkRuntimeException(
                                        String.format(
                                                "TaskManager %s is blocked.",
                                                taskManagerId.getStringWithMetadata()));
                        slotPoolService.releaseFreeSlotsOnTaskManager(taskManagerId, cause);
                    });
        }

        @Override
        public void unblockResources(Collection<BlockedNode> unblockedNodes) {}
    }
}
