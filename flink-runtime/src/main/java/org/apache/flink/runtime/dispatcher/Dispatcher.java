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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.cleanup.CleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleaner;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible for receiving
 * job submissions, persisting them, spawning JobManagers to execute the jobs and to recover them in
 * case of a master failure. Furthermore, it knows about the state of the Flink session cluster.
 */
public abstract class Dispatcher extends FencedRpcEndpoint<DispatcherId>
        implements DispatcherGateway {

    @VisibleForTesting
    public static final ConfigOption<Duration> CLIENT_ALIVENESS_CHECK_DURATION =
            ConfigOptions.key("$internal.dispatcher.client-aliveness-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1));

    public static final String DISPATCHER_NAME = "dispatcher";

    private static final int INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY = 16;

    private final Configuration configuration;

    private final JobGraphWriter jobGraphWriter;
    private final JobResultStore jobResultStore;

    private final HighAvailabilityServices highAvailabilityServices;
    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final JobManagerSharedServices jobManagerSharedServices;
    private final HeartbeatServices heartbeatServices;
    private final BlobServer blobServer;

    private final FatalErrorHandler fatalErrorHandler;
    private final Collection<FailureEnricher> failureEnrichers;

    private final OnMainThreadJobManagerRunnerRegistry jobManagerRunnerRegistry;

    private final Collection<JobGraph> recoveredJobs;

    private final Collection<JobResult> recoveredDirtyJobs;

    private final DispatcherBootstrapFactory dispatcherBootstrapFactory;

    private final ExecutionGraphInfoStore executionGraphInfoStore;

    private final JobManagerRunnerFactory jobManagerRunnerFactory;
    private final CleanupRunnerFactory cleanupRunnerFactory;

    private final JobManagerMetricGroup jobManagerMetricGroup;

    private final HistoryServerArchivist historyServerArchivist;

    private final Executor ioExecutor;

    @Nullable private final String metricServiceQueryAddress;

    private final Map<JobID, CompletableFuture<Void>> jobManagerRunnerTerminationFutures;
    private final Set<JobID> submittedAndWaitingTerminationJobIDs;

    protected final CompletableFuture<ApplicationStatus> shutDownFuture;

    private DispatcherBootstrap dispatcherBootstrap;

    private final DispatcherCachedOperationsHandler dispatcherCachedOperationsHandler;

    private final ResourceCleaner localResourceCleaner;
    private final ResourceCleaner globalResourceCleaner;

    private final Time webTimeout;

    private final Map<JobID, Long> jobClientExpiredTimestamp = new HashMap<>();
    private final Map<JobID, Long> uninitializedJobClientHeartbeatTimeout = new HashMap<>();
    private final long jobClientAlivenessCheckInterval;
    private ScheduledFuture<?> jobClientAlivenessCheck;

    /**
     * Simple data-structure to keep track of pending {@link JobResourceRequirements} updates, so we
     * can prevent race conditions.
     */
    private final Set<JobID> pendingJobResourceRequirementsUpdates = new HashSet<>();

    /** Enum to distinguish between initial job submission and re-submission for recovery. */
    protected enum ExecutionType {
        SUBMISSION,//作业提交
        RECOVERY //作业恢复
    }

    public Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices)
            throws Exception {
        this(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                dispatcherBootstrapFactory,
                dispatcherServices,
                new DefaultJobManagerRunnerRegistry(INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY));
    }

    private Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices,
            JobManagerRunnerRegistry jobManagerRunnerRegistry)
            throws Exception {
        this(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                dispatcherBootstrapFactory,
                dispatcherServices,
                jobManagerRunnerRegistry,
                new DispatcherResourceCleanerFactory(jobManagerRunnerRegistry, dispatcherServices));
    }

    @VisibleForTesting
    protected Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            ResourceCleanerFactory resourceCleanerFactory)
            throws Exception {
        super(rpcService, RpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
        assertRecoveredJobsAndDirtyJobResults(recoveredJobs, recoveredDirtyJobs);

        this.configuration = dispatcherServices.getConfiguration();
        this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
        this.resourceManagerGatewayRetriever =
                dispatcherServices.getResourceManagerGatewayRetriever();
        this.heartbeatServices = dispatcherServices.getHeartbeatServices();
        this.blobServer = dispatcherServices.getBlobServer();
        this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
        this.failureEnrichers = dispatcherServices.getFailureEnrichers();
        this.jobGraphWriter = dispatcherServices.getJobGraphWriter();
        this.jobResultStore = dispatcherServices.getJobResultStore();
        this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
        this.metricServiceQueryAddress = dispatcherServices.getMetricQueryServiceAddress();
        this.ioExecutor = dispatcherServices.getIoExecutor();
        /** 创建JobManagerSharedServices 共享服务方便后面直接获取，比如ShuffleMaster*/
        this.jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        configuration, blobServer, fatalErrorHandler);

        this.jobManagerRunnerRegistry =
                new OnMainThreadJobManagerRunnerRegistry(
                        jobManagerRunnerRegistry, this.getMainThreadExecutor());

        this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

        this.executionGraphInfoStore = dispatcherServices.getArchivedExecutionGraphStore();

        this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();
        this.cleanupRunnerFactory = dispatcherServices.getCleanupRunnerFactory();

        this.jobManagerRunnerTerminationFutures =
                CollectionUtil.newHashMapWithExpectedSize(
                        INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY);
        this.submittedAndWaitingTerminationJobIDs = new HashSet<>();

        this.shutDownFuture = new CompletableFuture<>();

        this.dispatcherBootstrapFactory = checkNotNull(dispatcherBootstrapFactory);

        this.recoveredJobs = new HashSet<>(recoveredJobs);

        this.recoveredDirtyJobs = new HashSet<>(recoveredDirtyJobs);

        this.blobServer.retainJobs(
                recoveredJobs.stream().map(JobGraph::getJobID).collect(Collectors.toSet()),
                dispatcherServices.getIoExecutor());

        this.dispatcherCachedOperationsHandler =
                new DispatcherCachedOperationsHandler(
                        dispatcherServices.getOperationCaches(),
                        this::triggerCheckpointAndGetCheckpointID,
                        this::triggerSavepointAndGetLocation,
                        this::stopWithSavepointAndGetLocation);

        this.localResourceCleaner =
                resourceCleanerFactory.createLocalResourceCleaner(this.getMainThreadExecutor());
        this.globalResourceCleaner =
                resourceCleanerFactory.createGlobalResourceCleaner(this.getMainThreadExecutor());

        this.webTimeout = Time.milliseconds(configuration.get(WebOptions.TIMEOUT));

        this.jobClientAlivenessCheckInterval =
                configuration.get(CLIENT_ALIVENESS_CHECK_DURATION).toMillis();
    }

    // ------------------------------------------------------
    // Getters
    // ------------------------------------------------------

    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    // ------------------------------------------------------
    // Lifecycle methods
    // ------------------------------------------------------

    @Override
    public void onStart() throws Exception {
        try {
            startDispatcherServices();
        } catch (Throwable t) {
            final DispatcherException exception =
                    new DispatcherException(
                            String.format("Could not start the Dispatcher %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }

        startCleanupRetries();
        startRecoveredJobs();

        this.dispatcherBootstrap =
                this.dispatcherBootstrapFactory.create(
                        getSelfGateway(DispatcherGateway.class),
                        this.getRpcService().getScheduledExecutor(),
                        this::onFatalError);
    }

    private void startDispatcherServices() throws Exception {
        try {
            registerDispatcherMetrics(jobManagerMetricGroup);
        } catch (Exception e) {
            handleStartDispatcherServicesException(e);
        }
    }

    private static void assertRecoveredJobsAndDirtyJobResults(
            Collection<JobGraph> recoveredJobs, Collection<JobResult> recoveredDirtyJobResults) {
        final Set<JobID> jobIdsOfFinishedJobs =
                recoveredDirtyJobResults.stream()
                        .map(JobResult::getJobId)
                        .collect(Collectors.toSet());

        final boolean noRecoveredJobGraphHasDirtyJobResult =
                recoveredJobs.stream()
                        .noneMatch(
                                recoveredJobGraph ->
                                        jobIdsOfFinishedJobs.contains(
                                                recoveredJobGraph.getJobID()));

        Preconditions.checkArgument(
                noRecoveredJobGraphHasDirtyJobResult,
                "There should be no overlap between the recovered JobGraphs and the passed dirty JobResults based on their job ID.");
    }

    private void startRecoveredJobs() {
        for (JobGraph recoveredJob : recoveredJobs) {
            runRecoveredJob(recoveredJob);
        }
        recoveredJobs.clear();
    }

    private void runRecoveredJob(final JobGraph recoveredJob) {
        checkNotNull(recoveredJob);

        initJobClientExpiredTime(recoveredJob);

        try {
            runJob(createJobMasterRunner(recoveredJob), ExecutionType.RECOVERY);
        } catch (Throwable throwable) {
            onFatalError(
                    new DispatcherException(
                            String.format(
                                    "Could not start recovered job %s.", recoveredJob.getJobID()),
                            throwable));
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化作业客户端的心跳超时时间，并设置相应的检查机制来确保客户端的活跃性
    */
    private void initJobClientExpiredTime(JobGraph jobGraph) {
        /**
         * 从作业图中获取作业的ID。
         */
        JobID jobID = jobGraph.getJobID();
        /**
         * 从作业图中获取初始的客户端心跳超时时间。
         */
        long initialClientHeartbeatTimeout = jobGraph.getInitialClientHeartbeatTimeout();
        /**
         * 如果 initialClientHeartbeatTimeout 大于0，说明设置了有效的心跳超时时间
         */
        if (initialClientHeartbeatTimeout > 0) {
            log.info(
                    "Begin to detect the client's aliveness for job {}. The heartbeat timeout is {}",
                    jobID,
                    initialClientHeartbeatTimeout);
            /**
             * 将作业ID和对应的心跳超时时间存储到一个映射（ Map<JobID, Long>）中，以便后续使用。
             * Map<JobID, Long> uninitializedJobClientHeartbeatTimeout = new HashMap<>();
             */
            uninitializedJobClientHeartbeatTimeout.put(jobID, initialClientHeartbeatTimeout);
            /**
             * 如果jobClientAlivenessCheck为空
             */
            if (jobClientAlivenessCheck == null) {
                // Use the client heartbeat timeout as the check interval.
                jobClientAlivenessCheck =
                        /** 使用RPC服务的调度执行器来设置一个定期任务。 */
                        this.getRpcService()
                                .getScheduledExecutor()
                                .scheduleWithFixedDelay(
                                        () ->
                                                getMainThreadExecutor()
                                                        /** 这个任务会调用 checkJobClientAliveness 方法来检查客户端的活跃性。 */
                                                        .execute(this::checkJobClientAliveness),
                                        /** 初始延迟是0毫秒，即任务会立即开始执行。 */
                                        0L,
                                        /** 每次检查之间的时间间隔。 */
                                        jobClientAlivenessCheckInterval,
                                        TimeUnit.MILLISECONDS);
            }
        }
    }

    private void startCleanupRetries() {
        recoveredDirtyJobs.forEach(this::runCleanupRetry);
        recoveredDirtyJobs.clear();
    }

    private void runCleanupRetry(final JobResult jobResult) {
        checkNotNull(jobResult);

        try {
            runJob(createJobCleanupRunner(jobResult), ExecutionType.RECOVERY);
        } catch (Throwable throwable) {
            onFatalError(
                    new DispatcherException(
                            String.format(
                                    "Could not start cleanup retry for job %s.",
                                    jobResult.getJobId()),
                            throwable));
        }
    }

    private void handleStartDispatcherServicesException(Exception e) throws Exception {
        try {
            stopDispatcherServices();
        } catch (Exception exception) {
            e.addSuppressed(exception);
        }

        throw e;
    }

    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping dispatcher {}.", getAddress());

        if (jobClientAlivenessCheck != null) {
            jobClientAlivenessCheck.cancel(false);
            jobClientAlivenessCheck = null;
        }

        final CompletableFuture<Void> allJobsTerminationFuture =
                terminateRunningJobsAndGetTerminationFuture();

        return FutureUtils.runAfterwards(
                allJobsTerminationFuture,
                () -> {
                    dispatcherBootstrap.stop();

                    stopDispatcherServices();

                    log.info("Stopped dispatcher {}.", getAddress());
                });
    }

    private void stopDispatcherServices() throws Exception {
        Exception exception = null;
        try {
            jobManagerSharedServices.shutdown();
        } catch (Exception e) {
            exception = e;
        }

        jobManagerMetricGroup.close();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ------------------------------------------------------
    // RPCs
    // ------------------------------------------------------

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 基于各种情况进行校验，在符合条件的情况提交job
    */
    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        /** 从传入的jobGraph中获取作业的唯一标识符jobID。UUID */
        final JobID jobID = jobGraph.getJobID();
        log.info("Received JobGraph submission '{}' ({}).", jobGraph.getName(), jobID);
        /**
         * 调用isInGloballyTerminalState方法来检查作业是否已经达到了全局终止状态。
         * 返回一个CompletableFuture<Boolean>，表示异步检查的结果。
         */
        return isInGloballyTerminalState(jobID)
                /**
                 * thenCompose 它用于链接两个异步操作，其中第二个操作依赖于第一个操作的结果。
                 */
                .thenComposeAsync(
                        isTerminated -> {
                            /**
                             * 作业已经处于全局终止状态，则记录一条警告级别的日志，并返回一个异常完成的CompletableFuture，
                             * 如果JobId对应的状态是终止状态，则打印警告日志，将异步变成结果封装为DuplicateJobSubmissionException异常。
                             */
                            if (isTerminated) {
                                log.warn(
                                        "Ignoring JobGraph submission '{}' ({}) because the job already "
                                                + "reached a globally-terminal state (i.e. {}) in a "
                                                + "previous execution.",
                                        jobGraph.getName(),
                                        jobID,
                                        Arrays.stream(JobStatus.values())
                                                .filter(JobStatus::isGloballyTerminalState)
                                                .map(JobStatus::name)
                                                .collect(Collectors.joining(", ")));
                                return FutureUtils.completedExceptionally(
                                        DuplicateJobSubmissionException.ofGloballyTerminated(
                                                jobID));
                            /**
                             * 如果作业ID已经在jobManagerRunnerRegistry中注册，
                             * 或者它已经在submittedAndWaitingTerminationJobIDs集合中（意味着作业已经提交并正在等待终止），
                             * 则返回一个异常完成的CompletableFuture，表示尝试重复提交作业。
                             */
                            } else if (jobManagerRunnerRegistry.isRegistered(jobID)
                                    || submittedAndWaitingTerminationJobIDs.contains(jobID)) {
                                // job with the given jobID is not terminated, yet
                                return FutureUtils.completedExceptionally(
                                        DuplicateJobSubmissionException.of(jobID));
                            /**
                             * 如果jobGraph的部分顶点（vertices）配置了资源，但系统当前不支持这种情况，
                             * 一个顶点有已配置的资源，并且之前存在资源未知的顶点，方法就会返回true
                             * 则创建一个JobSubmissionException异常，并通过FutureUtils.completedExceptionally方法
                             * 返回一个异常完成的CompletableFuture。
                             */
                            } else if (isPartialResourceConfigured(jobGraph)) {
                                return FutureUtils.completedExceptionally(
                                        new JobSubmissionException(
                                                jobID,
                                                "Currently jobs is not supported if parts of the vertices "
                                                        + "have resources configured. The limitation will be "
                                                        + "removed in future versions."));
                            } else {
                                /**
                                 * 如果上述所有条件都不满足，即作业图的状态允许正常提交，
                                 * 调用internalSubmitJob方法来执行实际的作业提交逻辑，
                                 * CompletableFuture。
                                 */
                                return internalSubmitJob(jobGraph);
                            }
                        },
                        getMainThreadExecutor());
    }

    @Override
    public CompletableFuture<Acknowledge> submitFailedJob(
            JobID jobId, String jobName, Throwable exception) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        jobId,
                        jobName,
                        JobStatus.FAILED,
                        exception,
                        null,
                        System.currentTimeMillis());
        ExecutionGraphInfo executionGraphInfo = new ExecutionGraphInfo(archivedExecutionGraph);
        writeToExecutionGraphInfoStore(executionGraphInfo);
        return archiveExecutionGraphToHistoryServer(executionGraphInfo);
    }

    /**
     * Checks whether the given job has already been executed.
     *
     * @param jobId identifying the submitted job
     * @return a successfully completed future with {@code true} if the job has already finished,
     *     either successfully or as a failure
     */
    private CompletableFuture<Boolean> isInGloballyTerminalState(JobID jobId) {
        return jobResultStore.hasJobResultEntryAsync(jobId);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 一个顶点有已配置的资源，并且之前存在资源未知的顶点，方法就会返回true
    */
    private boolean isPartialResourceConfigured(JobGraph jobGraph) {
        /**
         * 定义hasVerticesWithUnknownResource变量用于追踪是否存在资源未知的顶点
         */
        boolean hasVerticesWithUnknownResource = false;
        /**
         * 定义hasVerticesWithUnknownResource资源已配置的顶点
         */
        boolean hasVerticesWithConfiguredResource = false;
        /**
         * 遍历作业图中的每一个顶点（jobVertex）
         */
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            /**
             * 对于每一个顶点，检查其最小资源要求（getMinResources()）。如果返回的资源规格是UNKNOWN，
             * 则将hasVerticesWithUnknownResource设置为true。
             */
            if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
                hasVerticesWithUnknownResource = true;
            } else {
                /**
                 * 如果返回的资源规格不是UNKNOWN，则将hasVerticesWithConfiguredResource设置为true。
                 */
                hasVerticesWithConfiguredResource = true;
            }

            if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                return true;
            }
        }

        return false;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 持久化运行Job,并对运行中的各种情况进行异步处理
    */ 
    private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
        /**
         * 读取pipeline.jobvertex-parallelism-overrides配置
         * 为JobGraph中的JobVertex设置并行度，覆盖以前的并行度
         */
        applyParallelismOverrides(jobGraph);
        log.info("Submitting job '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());

        // track as an outstanding job
        /**
         * Set<JobID> submittedAndWaitingTerminationJobIDs
         * 将当前作业的ID添加到 submittedAndWaitingTerminationJobIDs 列表中，
         * 这个列表用来跟踪所有已经提交但尚未终止的作业。
         */
        submittedAndWaitingTerminationJobIDs.add(jobGraph.getJobID());
        /**
         * 异步地等待作业完成或终止的
         */
        return waitForTerminatingJob(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
                /**
                 * 用于处理作业完成或失败的情况。如果作业成功完成，ignored 参数将包含结果；如果作业失败，throwable 将包含异常。
                 * handleTermination 方法用于处理这些完成或失败的情况，清理资源
                 */
                .handle((ignored, throwable) -> handleTermination(jobGraph.getJobID(), throwable))
                /**
                 * 方法用于处理异步操作的结果。在这里，它似乎只是返回 CompletableFuture 的结果，没有执行任何额外的转换或组合。
                 */
                .thenCompose(Function.identity())
                /**
                 * CompletableFuture 完成时执行一些清理操作。在这里，
                 * 它从 Set<JobID> submittedAndWaitingTerminationJobIDs
                 * 列表中移除已完成（无论成功还是失败）的作业的ID。
                 */
                .whenComplete(
                        (ignored, throwable) ->
                                // job is done processing, whether failed or finished
                                submittedAndWaitingTerminationJobIDs.remove(jobGraph.getJobID()));
    }

    private CompletableFuture<Acknowledge> handleTermination(
            JobID jobId, @Nullable Throwable terminationThrowable) {
        /**
         * 首先，它检查terminationThrowable是否为null。如果terminationThrowable不为null，说明作业提交失败，并有一个异常与之关联。
         */
        if (terminationThrowable != null) {
            /**
             * 如果terminationThrowable不为null，则调用globalResourceCleaner.cleanupAsync(jobId)来异步清理与指定作业ID关联的资源。
             */
            return globalResourceCleaner
                    .cleanupAsync(jobId)
                    .handleAsync(
                            (ignored, cleanupThrowable) -> {
                                if (cleanupThrowable != null) {
                                    log.warn(
                                            "Cleanup didn't succeed after job submission failed for job {}.",
                                            jobId,
                                            cleanupThrowable);
                                    terminationThrowable.addSuppressed(cleanupThrowable);
                                }
                                ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(
                                        terminationThrowable);
                                final Throwable strippedThrowable =
                                        ExceptionUtils.stripCompletionException(
                                                terminationThrowable);
                                log.error("Failed to submit job {}.", jobId, strippedThrowable);
                                throw new CompletionException(
                                        new JobSubmissionException(
                                                jobId, "Failed to submit job.", strippedThrowable));
                            },
                            getMainThreadExecutor());
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 持久化并运行Job
    */
    private void persistAndRunJob(JobGraph jobGraph) throws Exception {
        /**
         * 将JobGraph持久化，standalone内部为空
         */
        jobGraphWriter.putJobGraph(jobGraph);
        /**
         * 设置JobGraph超时时间，内部有定时任务去判断是否超时
         * expiredTimestamp <= currentTimestamp
         */
        initJobClientExpiredTime(jobGraph);
        /**
         * 1.创建JobManagerRunner
         * 2.runJob运行JobManagerRunner
         */
        runJob(createJobMasterRunner(jobGraph), ExecutionType.SUBMISSION);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建JobManagerRunner 用于执行JobMaster
    */
    private JobManagerRunner createJobMasterRunner(JobGraph jobGraph) throws Exception {
        /**
         * 确保作业管理器运行器注册表（jobManagerRunnerRegistry）中尚未注册与给定 JobGraph 的作业ID关联的运行器。
         * 如果已经注册，则会抛出异常。
         * 总结：检查jobId是否已经注册，如果注册了则抛出异常
         */
        Preconditions.checkState(!jobManagerRunnerRegistry.isRegistered(jobGraph.getJobID()));
        /**
         * 接受一个 JobGraph 对象作为参数，并返回一个 JobManagerRunner 对象。
         */
        return jobManagerRunnerFactory.createJobManagerRunner(
                jobGraph,
                configuration,
                getRpcService(),
                highAvailabilityServices,
                heartbeatServices,
                jobManagerSharedServices,
                new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
                fatalErrorHandler,
                failureEnrichers,
                System.currentTimeMillis());
    }

    private JobManagerRunner createJobCleanupRunner(JobResult dirtyJobResult) throws Exception {
        Preconditions.checkState(!jobManagerRunnerRegistry.isRegistered(dirtyJobResult.getJobId()));
        return cleanupRunnerFactory.create(
                dirtyJobResult,
                highAvailabilityServices.getCheckpointRecoveryFactory(),
                configuration,
                ioExecutor);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 该方法负责启动一个作业管理器运行器（JobManagerRunner），注册它，并处理作业完成后的清理工作。
    */
    private void runJob(JobManagerRunner jobManagerRunner, ExecutionType executionType)
            throws Exception {
        /**
         * 调用 start 方法来启动作业管理器运行器。
         * 会跳转到grantLeadership方法
         */
        jobManagerRunner.start();
        /**
         * 将作业管理器运行器注册到 jobManagerRunnerRegistry 中，这通常是为了追踪和管理正在运行的作业。
         */
        jobManagerRunnerRegistry.register(jobManagerRunner);
        /**
         * 从作业管理器运行器中获取作业的ID。
         */
        final JobID jobId = jobManagerRunner.getJobID();

        final CompletableFuture<CleanupJobState> cleanupJobStateFuture =
                /**
                 * 获取jobManagerRunner(CompletableFuture) 它代表jobManagerRunner启动结果。
                 * CompletableFuture<JobManagerRunnerResult> resultFuture
                 */
                jobManagerRunner
                        .getResultFuture()
                        .handleAsync(
                                (jobManagerRunnerResult, throwable) -> {
                                    /** 检查作业是否仍在 jobManagerRunnerRegistry 中注册
                                     *  注册的对象与原始的 jobManagerRunner 是否相同
                                     */
                                    Preconditions.checkState(
                                            jobManagerRunnerRegistry.isRegistered(jobId)
                                                    && jobManagerRunnerRegistry.get(jobId)
                                                            == jobManagerRunner,
                                            "The job entry in runningJobs must be bound to the lifetime of the JobManagerRunner.");
                                    /**
                                     * 	+ 如果作业结果不为空，则调用 `handleJobManagerRunnerResult` 方法处理结果。
                                     */
                                    if (jobManagerRunnerResult != null) {
                                        /**归档等操作*/
                                        return handleJobManagerRunnerResult(
                                                jobManagerRunnerResult, executionType);
                                    } else {
                                        /**
                                         *  完成的CompletableFuture 返回结果为jobManagerRunnerFailed
                                         */
                                        return CompletableFuture.completedFuture(
                                                jobManagerRunnerFailed(
                                                        jobId, JobStatus.FAILED, throwable));
                                    }
                                },
                                /** 主线程中执行 */
                                getMainThreadExecutor())
                        /**
                         * 是为了将异步操作的结果合并到 `cleanupJobStateFuture` 中。
                         */
                        .thenCompose(Function.identity());
        /**
         * 定义了一个 CompletableFuture<Void>，名为 jobTerminationFuture，它代表作业的终止操作。
         */
        final CompletableFuture<Void> jobTerminationFuture =
                cleanupJobStateFuture.thenCompose(
                        cleanupJobState ->
                                /**
                                 * 从ResourceCleaner中一处 jobId
                                 */
                                removeJob(jobId, cleanupJobState)
                                        /**
                                         * 方法用于处理 removeJob 方法可能抛出的异常。如果 removeJob 抛出异常，
                                         * 调用 logCleanupErrorWarning 方法来记录警告信息。
                                         */
                                        .exceptionally(
                                                throwable ->
                                                        logCleanupErrorWarning(jobId, throwable)));
        /**
         * FutureUtils.handleUncaughtException 方法来注册一个异常处理器
         */
        FutureUtils.handleUncaughtException(
                /**
                 * 当异常发生时，它会调用 fatalErrorHandler.onFatalError 方法来处理致命错误。
                 */
                jobTerminationFuture,
                (thread, throwable) -> fatalErrorHandler.onFatalError(throwable));
        /**
         * 注册 jobTerminationFuture。这通常是为了跟踪作业的终止状态，以便在需要时能够查询或等待作业的终止。
         */
        registerJobManagerRunnerTerminationFuture(jobId, jobTerminationFuture);
    }

    @Nullable
    private Void logCleanupErrorWarning(JobID jobId, Throwable cleanupError) {
        log.warn(
                "The cleanup of job {} failed. The job's artifacts in the different directories ('{}', '{}', '{}') and its JobResultStore entry in '{}' (in HA mode) should be checked for manual cleanup.",
                jobId,
                configuration.get(HighAvailabilityOptions.HA_STORAGE_PATH),
                configuration.get(BlobServerOptions.STORAGE_DIRECTORY),
                configuration.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY),
                configuration.get(JobResultStoreOptions.STORAGE_PATH),
                cleanupError);
        return null;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理jobManagerRunnerResult结果
    */
    private CompletableFuture<CleanupJobState> handleJobManagerRunnerResult(
            JobManagerRunnerResult jobManagerRunnerResult, ExecutionType executionType) {
        /**
         * 1.jobManagerRunnerResult是否表示初始化失败、并且executionType是否为RECOVERY
         * 如果jobManagerRunner初始化失败、并且作业执行类型为RECOVERY恢复
         *创建已经完成了的 CompletableFuture 实例并设置值为jobManagerRunnerFailed
         */
        if (jobManagerRunnerResult.isInitializationFailure()
                && executionType == ExecutionType.RECOVERY) {
            // fail fatally to make the Dispatcher fail-over and recover all jobs once more (which
            // can only happen in HA mode)
            /**
             * 经完成的CompletableFuture,返回值为jobManagerRunnerFailed
             * ExecutionType.RECOVERY，致命故障，使Dispatcher故障转移并再次恢复所有作业（HA模式）
             */
            return CompletableFuture.completedFuture(
                    jobManagerRunnerFailed(
                            jobManagerRunnerResult.getExecutionGraphInfo().getJobId(),
                            JobStatus.INITIALIZING,
                            jobManagerRunnerResult.getInitializationFailure()));
        }
        /**
         * 获取ExecutionGraphInfo,处理作业达到终端状态的情况
         */
        return jobReachedTerminalState(jobManagerRunnerResult.getExecutionGraphInfo());
    }

    private static class CleanupJobState {

        private final boolean globalCleanup;
        private final JobStatus jobStatus;

        public static CleanupJobState localCleanup(JobStatus jobStatus) {
            return new CleanupJobState(false, jobStatus);
        }

        public static CleanupJobState globalCleanup(JobStatus jobStatus) {
            return new CleanupJobState(true, jobStatus);
        }

        private CleanupJobState(boolean globalCleanup, JobStatus jobStatus) {
            this.globalCleanup = globalCleanup;
            this.jobStatus = jobStatus;
        }

        public boolean isGlobalCleanup() {
            return globalCleanup;
        }

        public JobStatus getJobStatus() {
            return jobStatus;
        }
    }

    private CleanupJobState jobManagerRunnerFailed(
            JobID jobId, JobStatus jobStatus, Throwable throwable) {
        jobMasterFailed(jobId, throwable);
        return CleanupJobState.localCleanup(jobStatus);
    }

    @Override
    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
        return CompletableFuture.completedFuture(
                Collections.unmodifiableSet(jobManagerRunnerRegistry.getRunningJobIds()));
    }

    @Override
    public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath, Time timeout) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        return CompletableFuture.supplyAsync(
                () -> {
                    log.info("Disposing savepoint {}.", savepointPath);

                    try {
                        Checkpoints.disposeSavepoint(
                                savepointPath, configuration, classLoader, log);
                    } catch (IOException | FlinkException e) {
                        throw new CompletionException(
                                new FlinkException(
                                        String.format(
                                                "Could not dispose savepoint %s.", savepointPath),
                                        e));
                    }

                    return Acknowledge.get();
                },
                jobManagerSharedServices.getIoExecutor());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 取消一个特定的作业
    */
    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        /**
         * 获取与给定作业ID关联的作业管理器运行器
         */
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);
        /**
         * 如果 maybeJob 存在（即 isPresent() 返回 true），则调用其 cancel 方法来取消作业，
         * 并返回取消操作的 CompletableFuture。
          */
        if (maybeJob.isPresent()) {
            /** schedulerNG.cancel(); */
            return maybeJob.get().cancel(timeout);
        }
        /**
         * 如果作业管理器运行器不存在，代码将尝试从 executionGraphInfoStore中获取与作业ID关联的执行图信息。
         */
        final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);
        if (executionGraphInfo != null) {
            final JobStatus jobStatus = executionGraphInfo.getArchivedExecutionGraph().getState();
            /**
             * 如果找到了执行图信息，并且其状态是 CANCELED（即作业已被取消），
             * 则立即返回一个已完成的 CompletableFuture，其中包含一个 Acknowledge 对象。
             */
            if (jobStatus == JobStatus.CANCELED) {
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                /**
                 * 如果作业状态不是 CANCELED，则返回一个异常完成的 CompletableFuture，
                 * 其中包含一个 FlinkJobTerminatedWithoutCancellationException 异常，指示作业在没有被取消的情况下终止。
                 */
                return FutureUtils.completedExceptionally(
                        new FlinkJobTerminatedWithoutCancellationException(jobId, jobStatus));
            }
        }

        log.debug("Dispatcher is unable to cancel job {}: not found", jobId);
        return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
    }

    @Override
    public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
        CompletableFuture<ResourceOverview> taskManagerOverviewFuture =
                runResourceManagerCommand(
                        resourceManagerGateway ->
                                resourceManagerGateway.requestResourceOverview(timeout));

        final List<CompletableFuture<Optional<JobStatus>>> optionalJobInformation =
                queryJobMastersForInformation(
                        jobManagerRunner -> jobManagerRunner.requestJobStatus(timeout));

        CompletableFuture<Collection<Optional<JobStatus>>> allOptionalJobsFuture =
                FutureUtils.combineAll(optionalJobInformation);

        CompletableFuture<Collection<JobStatus>> allJobsFuture =
                allOptionalJobsFuture.thenApply(this::flattenOptionalCollection);

        final JobsOverview completedJobsOverview = executionGraphInfoStore.getStoredJobsOverview();

        return allJobsFuture.thenCombine(
                taskManagerOverviewFuture,
                (Collection<JobStatus> runningJobsStatus, ResourceOverview resourceOverview) -> {
                    final JobsOverview allJobsOverview =
                            JobsOverview.create(runningJobsStatus).combine(completedJobsOverview);
                    return new ClusterOverview(resourceOverview, allJobsOverview);
                });
    }

    @Override
    public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
        List<CompletableFuture<Optional<JobDetails>>> individualOptionalJobDetails =
                queryJobMastersForInformation(
                        jobManagerRunner -> jobManagerRunner.requestJobDetails(timeout));

        CompletableFuture<Collection<Optional<JobDetails>>> optionalCombinedJobDetails =
                FutureUtils.combineAll(individualOptionalJobDetails);

        CompletableFuture<Collection<JobDetails>> combinedJobDetails =
                optionalCombinedJobDetails.thenApply(this::flattenOptionalCollection);

        final Collection<JobDetails> completedJobDetails =
                executionGraphInfoStore.getAvailableJobDetails();

        return combinedJobDetails.thenApply(
                (Collection<JobDetails> runningJobDetails) -> {
                    final Map<JobID, JobDetails> deduplicatedJobs = new HashMap<>();

                    completedJobDetails.forEach(job -> deduplicatedJobs.put(job.getJobId(), job));
                    runningJobDetails.forEach(job -> deduplicatedJobs.put(job.getJobId(), job));

                    return new MultipleJobsDetails(new HashSet<>(deduplicatedJobs.values()));
                });
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(JobID jobId, Time timeout) {
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);
        return maybeJob.map(job -> job.requestJobStatus(timeout))
                .orElseGet(
                        () -> {
                            // is it a completed job?
                            final JobDetails jobDetails =
                                    executionGraphInfoStore.getAvailableJobDetails(jobId);
                            if (jobDetails == null) {
                                return FutureUtils.completedExceptionally(
                                        new FlinkJobNotFoundException(jobId));
                            } else {
                                return CompletableFuture.completedFuture(jobDetails.getStatus());
                            }
                        });
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(
            JobID jobId, Time timeout) {
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);
        return maybeJob.map(job -> job.requestJob(timeout))
                .orElse(FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId)))
                .exceptionally(t -> getExecutionGraphInfoFromStore(t, jobId));
    }

    private ExecutionGraphInfo getExecutionGraphInfoFromStore(Throwable t, JobID jobId) {
        // check whether it is a completed job
        final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);
        if (executionGraphInfo == null) {
            throw new CompletionException(ExceptionUtils.stripCompletionException(t));
        } else {
            return executionGraphInfo;
        }
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(
            JobID jobId, Time timeout) {
        return performOperationOnJobMasterGateway(
                        jobId, gateway -> gateway.requestCheckpointStats(timeout))
                .exceptionally(
                        t ->
                                getExecutionGraphInfoFromStore(t, jobId)
                                        .getArchivedExecutionGraph()
                                        .getCheckpointStatsSnapshot());
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        if (!jobManagerRunnerRegistry.isRegistered(jobId)) {
            final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);

            if (executionGraphInfo == null) {
                return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
            } else {
                return CompletableFuture.completedFuture(
                        JobResult.createFrom(executionGraphInfo.getArchivedExecutionGraph()));
            }
        }

        final JobManagerRunner jobManagerRunner = jobManagerRunnerRegistry.get(jobId);
        return jobManagerRunner
                .getResultFuture()
                .thenApply(
                        jobManagerRunnerResult ->
                                JobResult.createFrom(
                                        jobManagerRunnerResult
                                                .getExecutionGraphInfo()
                                                .getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
        if (metricServiceQueryAddress != null) {
            return CompletableFuture.completedFuture(
                    Collections.singleton(metricServiceQueryAddress));
        } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        return runResourceManagerCommand(
                resourceManagerGateway ->
                        resourceManagerGateway.requestTaskManagerMetricQueryServiceAddresses(
                                timeout));
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        int stackTraceMaxDepth = configuration.get(ClusterOptions.THREAD_DUMP_STACKTRACE_MAX_DEPTH);
        return CompletableFuture.completedFuture(ThreadDumpInfo.dumpAndCreate(stackTraceMaxDepth));
    }

    @Override
    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
        return CompletableFuture.completedFuture(blobServer.getPort());
    }

    @Override
    public CompletableFuture<String> triggerCheckpoint(JobID jobID, Time timeout) {
        return performOperationOnJobMasterGateway(
                jobID, gateway -> gateway.triggerCheckpoint(timeout));
    }

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            AsynchronousJobOperationKey operationKey, CheckpointType checkpointType, Time timeout) {
        return dispatcherCachedOperationsHandler.triggerCheckpoint(
                operationKey, checkpointType, timeout);
    }

    @Override
    public CompletableFuture<OperationResult<Long>> getTriggeredCheckpointStatus(
            AsynchronousJobOperationKey operationKey) {
        return dispatcherCachedOperationsHandler.getCheckpointStatus(operationKey);
    }

    @Override
    public CompletableFuture<Long> triggerCheckpointAndGetCheckpointID(
            final JobID jobID, final CheckpointType checkpointType, final Time timeout) {
        return performOperationOnJobMasterGateway(
                jobID,
                gateway ->
                        gateway.triggerCheckpoint(checkpointType, timeout)
                                .thenApply(CompletedCheckpoint::getCheckpointID));
    }

    @Override
    public CompletableFuture<Acknowledge> triggerSavepoint(
            final AsynchronousJobOperationKey operationKey,
            final String targetDirectory,
            SavepointFormatType formatType,
            final TriggerSavepointMode savepointMode,
            final Time timeout) {
        return dispatcherCachedOperationsHandler.triggerSavepoint(
                operationKey, targetDirectory, formatType, savepointMode, timeout);
    }

    @Override
    public CompletableFuture<String> triggerSavepointAndGetLocation(
            JobID jobId,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.triggerSavepoint(
                                targetDirectory,
                                savepointMode.isTerminalMode(),
                                formatType,
                                timeout));
    }

    @Override
    public CompletableFuture<OperationResult<String>> getTriggeredSavepointStatus(
            AsynchronousJobOperationKey operationKey) {
        return dispatcherCachedOperationsHandler.getSavepointStatus(operationKey);
    }

    @Override
    public CompletableFuture<Acknowledge> stopWithSavepoint(
            AsynchronousJobOperationKey operationKey,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            final Time timeout) {
        return dispatcherCachedOperationsHandler.stopWithSavepoint(
                operationKey, targetDirectory, formatType, savepointMode, timeout);
    }

    @Override
    public CompletableFuture<String> stopWithSavepointAndGetLocation(
            final JobID jobId,
            final String targetDirectory,
            final SavepointFormatType formatType,
            final TriggerSavepointMode savepointMode,
            final Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.stopWithSavepoint(
                                targetDirectory,
                                formatType,
                                savepointMode.isTerminalMode(),
                                timeout));
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster() {
        return shutDownCluster(ApplicationStatus.SUCCEEDED);
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster(
            final ApplicationStatus applicationStatus) {
        shutDownFuture.complete(applicationStatus);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            JobID jobId,
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.deliverCoordinationRequestToCoordinator(
                                operatorId, serializedRequest, timeout));
    }

    @Override
    public CompletableFuture<Void> reportJobClientHeartbeat(
            JobID jobId, long expiredTimestamp, Time timeout) {
        if (!getJobManagerRunner(jobId).isPresent()) {
            log.warn("Fail to find job {} for client.", jobId);
        } else {
            log.debug(
                    "Job {} receives client's heartbeat which expiredTimestamp is {}.",
                    jobId,
                    expiredTimestamp);
            jobClientExpiredTimestamp.put(jobId, expiredTimestamp);
        }
        return FutureUtils.completedVoidFuture();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 检查作业客户端的心跳是否仍然活跃，并根据心跳超时情况来取消作业
    */
    private void checkJobClientAliveness() {
        /** 设置或更新已经初始化的作业的心跳超时时间 */
        setClientHeartbeatTimeoutForInitializedJob();
        /** 使用 System.currentTimeMillis() 获取当前的毫秒级时间戳。 */
        long currentTimestamp = System.currentTimeMillis();
        /**
         * 迭代器 iterator 遍历 jobClientExpiredTimestamp 映射。
         */
        Iterator<Map.Entry<JobID, Long>> iterator = jobClientExpiredTimestamp.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<JobID, Long> entry = iterator.next();
            /**
             * 对于每个映射中的条目，代码首先获取作业ID和对应的心跳超时时间戳。
             */
            JobID jobID = entry.getKey();
            long expiredTimestamp = entry.getValue();
            /**
             * 通过调用 getJobManagerRunner(jobID) 方法并检查返回的 Optional 是否为空，
             * 来确定作业管理器是否仍然在运行。如果不在运行，则使用迭代器从映射中移除该条目。
             */
            if (!getJobManagerRunner(jobID).isPresent()) {
                iterator.remove();
            } else if (expiredTimestamp <= currentTimestamp) {
                /**
                 * 如果作业管理器仍然在运行，但心跳超时时间戳小于或等于当前时间戳，这意味着客户端的心跳已经超时
                 */
                log.warn(
                        "The heartbeat from the job client is timeout and cancel the job {}. "
                                + "You can adjust the heartbeat interval "
                                + "by 'client.heartbeat.interval' and the timeout "
                                + "by 'client.heartbeat.timeout'",
                        jobID);
                /**
                 * 取消作业、executionGraph
                 * jobMasterGateway.cancel 取消请求的slot等
                 */
                cancelJob(jobID, webTimeout);
            }
        }
    }

    @Override
    public CompletableFuture<JobResourceRequirements> requestJobResourceRequirements(JobID jobId) {
        return performOperationOnJobMasterGateway(
                jobId, JobMasterGateway::requestJobResourceRequirements);
    }

    @Override
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) {
        if (!pendingJobResourceRequirementsUpdates.add(jobId)) {
            return FutureUtils.completedExceptionally(
                    new RestHandlerException(
                            "Another update to the job [%s] resource requirements is in progress.",
                            HttpResponseStatus.CONFLICT));
        }
        return performOperationOnJobMasterGateway(jobId, gateway -> gateway.requestJob(webTimeout))
                .thenApply(
                        job -> {
                            final Map<JobVertexID, Integer> maxParallelismPerJobVertex =
                                    new HashMap<>();
                            for (ArchivedExecutionJobVertex vertex :
                                    job.getArchivedExecutionGraph().getVerticesTopologically()) {
                                maxParallelismPerJobVertex.put(
                                        vertex.getJobVertexId(), vertex.getMaxParallelism());
                            }
                            return maxParallelismPerJobVertex;
                        })
                .thenAccept(
                        maxParallelismPerJobVertex ->
                                validateMaxParallelism(
                                        jobResourceRequirements, maxParallelismPerJobVertex))
                .thenRunAsync(
                        () -> {
                            try {
                                jobGraphWriter.putJobResourceRequirements(
                                        jobId, jobResourceRequirements);
                            } catch (Exception e) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "The resource requirements could not be persisted and have not been applied. Please retry.",
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                e));
                            }
                        },
                        ioExecutor)
                .thenComposeAsync(
                        ignored ->
                                performOperationOnJobMasterGateway(
                                        jobId,
                                        jobMasterGateway ->
                                                jobMasterGateway.updateJobResourceRequirements(
                                                        jobResourceRequirements)),
                        getMainThreadExecutor())
                .whenComplete(
                        (ack, error) -> {
                            if (error != null) {
                                log.debug(
                                        "Failed to update requirements for job {}.", jobId, error);
                            }
                            pendingJobResourceRequirementsUpdates.remove(jobId);
                        });
    }

    private static void validateMaxParallelism(
            JobResourceRequirements jobResourceRequirements,
            Map<JobVertexID, Integer> maxParallelismPerJobVertex) {

        final List<String> validationErrors =
                JobResourceRequirements.validate(
                        jobResourceRequirements, maxParallelismPerJobVertex);

        if (!validationErrors.isEmpty()) {
            throw new CompletionException(
                    new RestHandlerException(
                            validationErrors.stream()
                                    .collect(Collectors.joining(System.lineSeparator())),
                            HttpResponseStatus.BAD_REQUEST));
        }
    }

    private void setClientHeartbeatTimeoutForInitializedJob() {
        Iterator<Map.Entry<JobID, Long>> iterator =
                uninitializedJobClientHeartbeatTimeout.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<JobID, Long> entry = iterator.next();
            JobID jobID = entry.getKey();
            Optional<JobManagerRunner> jobManagerRunnerOptional = getJobManagerRunner(jobID);
            if (!jobManagerRunnerOptional.isPresent()) {
                iterator.remove();
            } else if (jobManagerRunnerOptional.get().isInitialized()) {
                jobClientExpiredTimestamp.put(jobID, System.currentTimeMillis() + entry.getValue());
                iterator.remove();
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 注册一个作业管理器运行器的终止 Future
    */
    private void registerJobManagerRunnerTerminationFuture(
            JobID jobId, CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
        /**
         * Preconditions.checkState 方法来确保当前 jobId 不在 jobManagerRunnerTerminationFutures 这个 Map 中。
         * 这通常是为了避免重复注册相同的作业 ID。
         */
        Preconditions.checkState(!jobManagerRunnerTerminationFutures.containsKey(jobId));
        /**
         * 将传入的 jobManagerRunnerTerminationFuture 存入 jobManagerRunnerTerminationFutures 这个 Map 中，以 jobId 作为键。
         */
        jobManagerRunnerTerminationFutures.put(jobId, jobManagerRunnerTerminationFuture);

        // clean up the pending termination future
        jobManagerRunnerTerminationFuture.thenRunAsync(
                () -> {
                    /**
                     * 首先从 jobManagerRunnerTerminationFutures 中移除当前作业的终止 Future。
                     */
                    final CompletableFuture<Void> terminationFuture =
                            jobManagerRunnerTerminationFutures.remove(jobId);

                    //noinspection ObjectEquality
                    /**
                     * 它检查移除的 Future 是否为 null 或者是否与最初注册的 Future 相同。如果不同，则重新将其放入 Map 中。
                     * 这里的逻辑可能用于处理一种特殊情况，即在 Future 完成前，可能有人尝试替换或者重置这个 Future。
                     * 由于 Future 的完成状态是不可逆的，一旦完成就不能被重置，
                     * 所以这段代码可能是一个防止并发修改 Map 的措施。
                     * 如果两个 Future 不同，那么将原始的 Future 放回 Map 中可能是为了保持 Map 中 Future 的正确引用。
                     */
                    if (terminationFuture != null
                            && terminationFuture != jobManagerRunnerTerminationFuture) {
                        jobManagerRunnerTerminationFutures.put(jobId, terminationFuture);
                    }
                },
                getMainThreadExecutor());
    }

    private CompletableFuture<Void> removeJob(JobID jobId, CleanupJobState cleanupJobState) {
        if (cleanupJobState.isGlobalCleanup()) {
            return globalResourceCleaner
                    .cleanupAsync(jobId)
                    .thenCompose(unused -> jobResultStore.markResultAsCleanAsync(jobId))
                    .handle(
                            (unusedVoid, e) -> {
                                if (e == null) {
                                    log.debug(
                                            "Cleanup for the job '{}' has finished. Job has been marked as clean.",
                                            jobId);
                                } else {
                                    log.warn(
                                            "Could not properly mark job {} result as clean.",
                                            jobId,
                                            e);
                                }
                                return null;
                            })
                    .thenRunAsync(
                            () ->
                                    runPostJobGloballyTerminated(
                                            jobId, cleanupJobState.getJobStatus()),
                            getMainThreadExecutor());
        } else {
            return localResourceCleaner.cleanupAsync(jobId);
        }
    }

    protected void runPostJobGloballyTerminated(JobID jobId, JobStatus jobStatus) {
        // no-op: we need to provide this method to enable the MiniDispatcher implementation to do
        // stuff after the job is cleaned up
    }

    /** Terminate all currently running {@link JobManagerRunner}s. */
    private void terminateRunningJobs() {
        log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

        final Set<JobID> jobsToRemove = jobManagerRunnerRegistry.getRunningJobIds();

        for (JobID jobId : jobsToRemove) {
            terminateJob(jobId);
        }
    }

    private void terminateJob(JobID jobId) {
        if (jobManagerRunnerRegistry.isRegistered(jobId)) {
            final JobManagerRunner jobManagerRunner = jobManagerRunnerRegistry.get(jobId);
            jobManagerRunner.closeAsync();
        }
    }

    private CompletableFuture<Void> terminateRunningJobsAndGetTerminationFuture() {
        terminateRunningJobs();
        final Collection<CompletableFuture<Void>> values =
                jobManagerRunnerTerminationFutures.values();
        return FutureUtils.completeAll(values);
    }

    protected void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 归档 executionGraphInfo
    */
    @VisibleForTesting
    protected CompletableFuture<CleanupJobState> jobReachedTerminalState(
            ExecutionGraphInfo executionGraphInfo) {
        /**
         * 获取创建的ArchivedExecutionGraph
         * 存储ExecutionGraphInfo
         */
        final ArchivedExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();
        final JobStatus terminalJobStatus = archivedExecutionGraph.getState();
        /**
         * 校验JobStatus状态NON_TERMINAL(未终止)状态。则抛出异常。
         */
        Preconditions.checkArgument(
                terminalJobStatus.isTerminalState(),
                "Job %s is in state %s which is not terminal.",
                archivedExecutionGraph.getJobID(),
                terminalJobStatus);

        // the failureInfo contains the reason for why job was failed/suspended, but for
        // finished/canceled jobs it may contain the last cause of a restart (if there were any)
        // for finished/canceled jobs we don't want to print it because it is misleading
        /**
         * 判断JobStatus状态
         * 1.作业已挂起，这意味着它已停止，但尚未从潜在的HA作业存储中删除
         * 2.作业失败，出现不可恢复的任务失败
         */
        final boolean isFailureInfoRelatedToJobTermination =
                terminalJobStatus == JobStatus.SUSPENDED || terminalJobStatus == JobStatus.FAILED;
        /**
         * 如果异常异常不为空，并且状态是挂起、失败则打印日志
         */
        if (archivedExecutionGraph.getFailureInfo() != null
                && isFailureInfoRelatedToJobTermination) {
            log.info(
                    "Job {} reached terminal state {}.\n{}",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus,
                    archivedExecutionGraph.getFailureInfo().getExceptionAsString().trim());
        } else {
            /**
             * 打印普通状态
             * 方便用户查看
             */
            log.info(
                    "Job {} reached terminal state {}.",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus);
        }
        /**
         * 调用writeToExecutionGraphInfoStore方法将执行图信息写入存储。
         */
        writeToExecutionGraphInfoStore(executionGraphInfo);
        /**
         * 作业的状态不是全局终端状态
         * 结束状态包括（失败、取消、完成）
         * 返回一个已经完成的CompletableFuture，并将异步变成结果设置为，
         * CleanupJobState状态
         */
        if (!terminalJobStatus.isGloballyTerminalState()) {
            return CompletableFuture.completedFuture(
                    CleanupJobState.localCleanup(terminalJobStatus));
        }

        // do not create an archive for suspended jobs, as this would eventually lead to
        // multiple archive attempts which we currently do not support
        /**
         * 这行代码调用archiveExecutionGraphToHistoryServer方法，
         * 将executionGraphInfo作为参数，以异步方式（返回一个CompletableFuture<Acknowledge>）
         * 将执行图信息归档到历史服务器。
         */
        CompletableFuture<Acknowledge> archiveFuture =
                archiveExecutionGraphToHistoryServer(executionGraphInfo);

        /** 注册全局终止的作业到作业结果存储: */
        return archiveFuture.thenCompose(
                ignored -> registerGloballyTerminatedJobInJobResultStore(executionGraphInfo));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 负责将一个全局终止的作业注册到作业结果存储中
    */
    private CompletableFuture<CleanupJobState> registerGloballyTerminatedJobInJobResultStore(
            ExecutionGraphInfo executionGraphInfo) {
        /** 获取JobId */
        final JobID jobId = executionGraphInfo.getJobId();
        /** 获取ArchivedExecutionGraph 归档对象*/
        final AccessExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();
        /** 获取当前ExecutionGraph 的状态*/
        final JobStatus terminalJobStatus = archivedExecutionGraph.getState();
        /**
         * 并检查这个状态是否是全局终止状态。如果不是，则抛出异常。
         * 状态：已完成、取消、失败。
         */
        Preconditions.checkArgument(
                terminalJobStatus.isGloballyTerminalState(),
                "Job %s is in state %s which is not globally terminal.",
                jobId,
                terminalJobStatus);

        return jobResultStore
                /**
                 * 判断jobResultStore中是否存在jobId返回值 CompletableFuture<Boolean>
                 * 将作业结果存储到干净的目录
                 */
                .hasCleanJobResultEntryAsync(jobId)
                .thenCompose(
                        hasCleanJobResultEntry ->
                                /**
                                 * 如果存在jobId 则返回一个Void的Future
                                 * 将作业结果存储到干净的目录
                                 */
                                createDirtyJobResultEntryIfMissingAsync(
                                        archivedExecutionGraph, hasCleanJobResultEntry))
                /**
                 * 使用`handleAsync`处理之前的异步操作的结果或可能抛出的异常
                 */
                .handleAsync(
                        /**
                         * 判断之前异步操作是否异常，如果异常则处理异常
                         */
                        (ignored, error) -> {
                            /**
                             * 如果出现异常，调用fatalErrorHandler来处理致命
                             */
                            if (error != null) {
                                fatalErrorHandler.onFatalError(
                                        new FlinkException(
                                                String.format(
                                                        "The job %s couldn't be marked as pre-cleanup finished in JobResultStore.",
                                                        executionGraphInfo.getJobId()),
                                                error));
                            }
                            /**
                             * 无论是否出现异常，最终都会返回一个`CleanupJobState`，
                             */
                            return CleanupJobState.globalCleanup(terminalJobStatus);
                        },
                        getMainThreadExecutor());
    }

    /**
     * Creates a dirty entry in the {@link #jobResultStore} if there's no entry at all for the given
     * {@code executionGraph} in the {@code JobResultStore}.
     *
     * @param executionGraph The {@link AccessExecutionGraph} for which the {@link JobResult} shall
     *     be persisted.
     * @param hasCleanJobResultEntry The decision the dirty entry check is based on.
     * @return {@code CompletableFuture} that completes as soon as the entry exists.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 返回藏条目
    */
    private CompletableFuture<Void> createDirtyJobResultEntryIfMissingAsync(
            AccessExecutionGraph executionGraph, boolean hasCleanJobResultEntry) {
        /** 获取JobId */
        final JobID jobId = executionGraph.getJobID();
        /**
         *
         */
        if (hasCleanJobResultEntry) {
            log.warn("Job {} is already marked as clean but clean up was triggered again.", jobId);
            /** 返回一个已经完成的Future对象，这个对象不返回任何结果（即它的泛型类型为Void） */
            return FutureUtils.completedVoidFuture();
        } else {
            return jobResultStore
                    /** 判断藏目录中是否已经存在JobId*/
                    .hasDirtyJobResultEntryAsync(jobId)
                    .thenCompose(
                            hasDirtyJobResultEntry ->
                                    /**
                                     * 如果存在则返回一个已经完成的Void的Future对象
                                     * 否则创建脏目录写入
                                     */
                                    createDirtyJobResultEntryAsync(
                                            executionGraph, hasDirtyJobResultEntry));
        }
    }

    /**
     * Creates a dirty entry in the {@link #jobResultStore} based on the passed {@code
     * hasDirtyJobResultEntry} flag.
     *
     * @param executionGraph The {@link AccessExecutionGraph} that is used to generate the entry.
     * @param hasDirtyJobResultEntry The decision the entry creation is based on.
     * @return {@code CompletableFuture} that completes as soon as the entry exists.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个脏目录，记录标志
    */
    private CompletableFuture<Void> createDirtyJobResultEntryAsync(
            AccessExecutionGraph executionGraph, boolean hasDirtyJobResultEntry) {
        if (hasDirtyJobResultEntry) {
            /** 返回一个已经完成的Future对象，这个对象不返回任何结果（即它的泛型类型为Void） */
            return FutureUtils.completedVoidFuture();
        }
        /** 创建一个脏目录，记录标志 */
        return jobResultStore.createDirtyResultAsync(
                new JobResultEntry(JobResult.createFrom(executionGraph)));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将executionGraphInfo信息写入磁盘
    */
    private void writeToExecutionGraphInfoStore(ExecutionGraphInfo executionGraphInfo) {
        try {
            executionGraphInfoStore.put(executionGraphInfo);
        } catch (IOException e) {
            log.info(
                    "Could not store completed job {}({}).",
                    executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                    executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                    e);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * executionGraphInfo保存到归档历史文件
    */
    private CompletableFuture<Acknowledge> archiveExecutionGraphToHistoryServer(
            ExecutionGraphInfo executionGraphInfo) {

        return historyServerArchivist
                /** 进行归档*/
                .archiveExecutionGraph(executionGraphInfo)
                .handleAsync(
                        (Acknowledge ignored, Throwable throwable) -> {
                            /** 归档过程中如果异常如果throwable != null 打印日志包括异常信息*/
                            if (throwable != null) {
                                log.info(
                                        "Could not archive completed job {}({}) to the history server.",
                                        executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                                        executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                                        throwable);
                            }
                            return Acknowledge.get();
                        },
                        getMainThreadExecutor());
    }

    private void jobMasterFailed(JobID jobId, Throwable cause) {
        // we fail fatally in case of a JobMaster failure in order to restart the
        // dispatcher to recover the jobs again. This only works in HA mode, though
        onFatalError(
                new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
    }

    /** Ensures that the JobMasterGateway is available. */
    private CompletableFuture<JobMasterGateway> getJobMasterGateway(JobID jobId) {
        if (!jobManagerRunnerRegistry.isRegistered(jobId)) {
            return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
        }

        final JobManagerRunner job = jobManagerRunnerRegistry.get(jobId);
        if (!job.isInitialized()) {
            return FutureUtils.completedExceptionally(
                    new UnavailableDispatcherOperationException(
                            "Unable to get JobMasterGateway for initializing job. "
                                    + "The requested operation is not available while the JobManager is initializing."));
        }
        return job.getJobMasterGateway();
    }

    private <T> CompletableFuture<T> performOperationOnJobMasterGateway(
            JobID jobId, Function<JobMasterGateway, CompletableFuture<T>> operation) {
        return getJobMasterGateway(jobId).thenCompose(operation);
    }

    private CompletableFuture<ResourceManagerGateway> getResourceManagerGateway() {
        return resourceManagerGatewayRetriever.getFuture();
    }

    private Optional<JobManagerRunner> getJobManagerRunner(JobID jobId) {
        return jobManagerRunnerRegistry.isRegistered(jobId)
                ? Optional.of(jobManagerRunnerRegistry.get(jobId))
                : Optional.empty();
    }

    private <T> CompletableFuture<T> runResourceManagerCommand(
            Function<ResourceManagerGateway, CompletableFuture<T>> resourceManagerCommand) {
        return getResourceManagerGateway()
                .thenApply(resourceManagerCommand)
                .thenCompose(Function.identity());
    }

    private <T> List<T> flattenOptionalCollection(Collection<Optional<T>> optionalCollection) {
        return optionalCollection.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    @Nonnull
    private <T> List<CompletableFuture<Optional<T>>> queryJobMastersForInformation(
            Function<JobManagerRunner, CompletableFuture<T>> queryFunction) {

        List<CompletableFuture<Optional<T>>> optionalJobInformation =
                new ArrayList<>(jobManagerRunnerRegistry.size());

        for (JobManagerRunner job : jobManagerRunnerRegistry.getJobManagerRunners()) {
            final CompletableFuture<Optional<T>> queryResult =
                    queryFunction
                            .apply(job)
                            .handle((T value, Throwable t) -> Optional.ofNullable(value));
            optionalJobInformation.add(queryResult);
        }
        return optionalJobInformation;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 异步等待某个作业管理器（JobManager）终止的，并在其终止后执行特定的动作（action）
    */
    private CompletableFuture<Void> waitForTerminatingJob(
            JobID jobId, JobGraph jobGraph, ThrowingConsumer<JobGraph, ?> action) {
        /**
         * CompletableFuture<Void> 类型的变量 jobManagerTerminationFuture，它代表了作业管理器的终止操作。
         */
        final CompletableFuture<Void> jobManagerTerminationFuture =
                getJobTerminationFuture(jobId)
                        /**
                         * 方法用于处理 Future 在执行过程中可能发生的异常。当 Future 完成且发生异常时，
                         * 这个方法会接收异常作为参数，并允许你提供一个函数来处理这个异常
                         */
                        .exceptionally(
                                (Throwable throwable) -> {
                                    throw new CompletionException(
                                            new DispatcherException(
                                                    String.format(
                                                            "Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.",
                                                            jobId),
                                                    throwable));
                                });
        /**
         * 异步等待 jobManagerTerminationFuture 完成
         */
        return FutureUtils.thenAcceptAsyncIfNotDone(
                jobManagerTerminationFuture,
                getMainThreadExecutor(),
                FunctionUtils.uncheckedConsumer(
                        /**
                         * 首先从 jobManagerRunnerTerminationFutures 列表中移除当前作业的ID，
                         * 为了清理状态或避免重复处理。然后，调用 action.accept(jobGraph)，
                         */
                        (ignored) -> {
                            jobManagerRunnerTerminationFutures.remove(jobId);
                            /**
                             * 触发this::persistAndRunJob执行
                             */
                            action.accept(jobGraph);
                        }));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    @VisibleForTesting
    CompletableFuture<Void> getJobTerminationFuture(JobID jobId) {
        /**
         * Map<JobID, CompletableFuture<Void>> jobManagerRunnerTerminationFutures 是一个 Map 数据结构，
         * 其键是 JobID 类型，值是 CompletableFuture<Void> 类型。这个 Map 用于存储所有作业管理器的终止 Future
         */
        /**
         * getOrDefault 是 Map 接口中的一个方法，它尝试获取与给定键（在本例中是 jobId）关联的值。
         * 如果 Map 中存在该键，则返回对应的值（即与 jobId 关联的 CompletableFuture<Void>）。
         * 如果 Map 中不存在该键（即没有与 jobId 关联的 Future），则 getOrDefault 方法返回其第二个参数作为默认值。
         */
        return jobManagerRunnerTerminationFutures.getOrDefault(
                jobId, CompletableFuture.completedFuture(null));
    }

    private void registerDispatcherMetrics(MetricGroup jobManagerMetricGroup) {
        jobManagerMetricGroup.gauge(
                MetricNames.NUM_RUNNING_JOBS,
                // metrics can be called from anywhere and therefore, have to run without the main
                // thread safeguard being triggered. For metrics, we can afford to be not 100%
                // accurate
                () -> (long) jobManagerRunnerRegistry.getWrappedDelegate().size());
    }

    public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
        return CompletableFuture.runAsync(() -> terminateJob(jobId), getMainThreadExecutor());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为 JobGraph 中的作业顶点（JobVertex）应用并行度覆盖
    */
    private void applyParallelismOverrides(JobGraph jobGraph) {
        /**
         * 创建一个新的 HashMap 来存储并行度覆盖信息。键是作业顶点的ID，值是对应的并行度值。
         */
        Map<String, String> overrides = new HashMap<>();
        /**
         * pipeline.jobvertex-parallelism-overrides"
         * 从两个地方获取并行度覆盖配置：全局配置 configuration 和作业图 jobGraph 的特定配置。
         */
        overrides.putAll(configuration.get(PipelineOptions.PARALLELISM_OVERRIDES));
        overrides.putAll(jobGraph.getJobConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES));
        /**
         * 遍历作业顶点
         */
        for (JobVertex vertex : jobGraph.getVertices()) {
            /**
             * 对于每个作业顶点，尝试从 overrides 映射中获取其并行度覆盖值。如果找到了覆盖值（即 override 不为 null）
             */
            String override = overrides.get(vertex.getID().toHexString());
            if (override != null) {
                /**
                 * 首先获取当前作业顶点的并行度，然后将覆盖值从字符串转换为整数。
                 */
                int currentParallelism = vertex.getParallelism();
                int overrideParallelism = Integer.parseInt(override);
                /**
                 * 记录一条日志，说明正在更改作业顶点的并行度。
                 */
                log.info(
                        "Changing job vertex {} parallelism from {} to {}",
                        vertex.getID(),
                        currentParallelism,
                        overrideParallelism);
                /**
                 * 使用 setParallelism 方法应用新的并行度覆盖值。
                 */
                vertex.setParallelism(overrideParallelism);
            }
        }
    }
}
