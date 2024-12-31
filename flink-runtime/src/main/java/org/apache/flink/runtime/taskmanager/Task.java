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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotPayload;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TaskManagerExceptionUtils;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The Task represents one execution of a parallel subtask on a TaskManager. A Task wraps a Flink
 * operator (which may be a user function) and runs it, providing all services necessary for example
 * to consume input data, produce its results (intermediate result partitions) and communicate with
 * the JobManager.
 *
 * <p>The Flink operators (implemented as subclasses of {@link TaskInvokable} have only data
 * readers, writers, and certain event callbacks. The task connects those to the network stack and
 * actor messages, and tracks the state of the execution and handles exceptions.
 *
 * <p>Tasks have no knowledge about how they relate to other tasks, or whether they are the first
 * attempt to execute the task, or a repeated attempt. All of that is only known to the JobManager.
 * All the task knows are its own runnable code, the task's configuration, and the IDs of the
 * intermediate results to consume and produce (if any).
 *
 * <p>Each Task is run by one dedicated thread.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * Task表示TaskManager上并行子任务的一次执行。Task包装Flink操作符（可能是用户函数）并运行它，
 * 提供所有必要的服务，例如消费输入数据、生成结果（中间结果分区）以及与JobManager通信。
*/
public class Task
        implements Runnable, TaskSlotPayload, TaskActions, PartitionProducerStateProvider {

    /** The class logger. 户口也如照顾*/
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    /** The thread group that contains all task threads. 包含所有任务线程的线程组。 */
    private static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

    /** For atomic state updates. 用于原子状态更新。*/
    private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    Task.class, ExecutionState.class, "executionState");

    // ------------------------------------------------------------------------
    //  Constant fields that are part of the initial Task construction
    // ------------------------------------------------------------------------

    /** The job that the task belongs to. */
    /** 任务所属的作业Id */
    private final JobID jobId;

    /** The vertex in the JobGraph whose code the task executes. */
    /** 任务执行其代码的JobGraph中的顶点 */
    private final JobVertexID vertexId;

    /** The execution attempt of the parallel subtask. */
    /** 并行子任务的执行尝试Id。 */
    private final ExecutionAttemptID executionId;

    /** ID which identifies the slot in which the task is supposed to run. */
    /** 标识任务应该在其中运行的插槽的ID。 */
    private final AllocationID allocationId;

    /** The meta information of current job. */
    /** 当前作业的元信息。 */
    private final JobInfo jobInfo;

    /** The meta information of current task. */
    /** 当前任务的元信息。 */
    private final TaskInfo taskInfo;

    /** The name of the task, including subtask indexes. */
    /** 任务的名称，包括子任务索引。 */
    private final String taskNameWithSubtask;

    /** The job-wide configuration object. */
    /** 作业范围的配置对象。 */
    private final Configuration jobConfiguration;

    /** The task-specific configuration. */
    /** 指定Task任务的配置 */
    private final Configuration taskConfiguration;

    /** The jar files used by this task. */
    /** 此任务使用的jar文件。jar上传后会有一个唯一的key */
    private final Collection<PermanentBlobKey> requiredJarFiles;

    /** The classpaths used by this task. */
    /** 此任务使用的类路径。 */
    private final Collection<URL> requiredClasspaths;

    /** The name of the class that holds the invokable code. */
    /** 包含可调用代码的类的名称 */
    private final String nameOfInvokableClass;

    /** Access to task manager configuration and host names. */
    /** 访问任务管理器配置和主机名。 */
    private final TaskManagerRuntimeInfo taskManagerConfig;

    /** The memory manager to be used by this task. */
    /** 此任务要使用的内存管理器。 */
    private final MemoryManager memoryManager;

    /** Shared memory manager provided by the task manager. */
    /** 跟踪已获取共享资源并处理其分配处置的映射。 */
    private final SharedResources sharedResources;

    /** The I/O manager to be used by this task. */
    /** 此任务要使用的I/O管理器 */
    private final IOManager ioManager;

    /** The BroadcastVariableManager to be used by this task. */
    /**
     * BroadcastVariableManager用于管理广播变量的具体化。对物化广播变量的引用被缓存，
     * 并在并行子任务之间共享。保持引用计数以跟踪是否可以清理物化。
     */
    private final BroadcastVariableManager broadcastVariableManager;
    /** 任务事件调度器将事件从消耗任务向后调度到产生消耗结果的任务。 */
    private final TaskEventDispatcher taskEventDispatcher;

    /** Information provider for external resources. */
    /** 就是获取外部资源信息 */
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The manager for state of operators running in this task/slot. */
    /** Slot运行任务过程中操作状态的管理器。提供了报告和检索任务状态的方法。 */
    private final TaskStateManager taskStateManager;

    /**
     * Serialized version of the job specific execution configuration (see {@link ExecutionConfig}).
     */
    /** 执行作业的序列化配置*/
    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;
    /**  Task运行时用来存储结果的接口 */
    private final ResultPartitionWriter[] partitionWriters;
    /** 获取单个task产生的中间结果的一个或多个分区的输入。 */
    private final IndexedInputGate[] inputGates;

    /** Connection to the task manager. */
    /** 用于Task与TaskExecutor通信的接口。 */
    private final TaskManagerActions taskManagerActions;

    /** Input split provider for the task. */
    /** 提供任务在执行过程中应该使用的一系列{@link InputSplit}对象。  */
    private final InputSplitProvider inputSplitProvider;

    /** Checkpoint notifier used to communicate with the CheckpointCoordinator. */
    /** 用于与CheckpointCoordinator通信的检查点通知程序  Task中检查点确认和拒绝消息的响应程序*/
    private final CheckpointResponder checkpointResponder;

    /**
     * The gateway for operators to send messages to the operator coordinators on the Job Manager.
     */
    /** 从任务向 OperatorCoordinator JobManager端发送 OperatorEvent、CoordinationRequest的网关。 */
    private final TaskOperatorEventGateway operatorCoordinatorEventGateway;

    /** GlobalAggregateManager used to update aggregates on the JobMaster. */
    /** GlobalAggregateManager用于更新JobMaster上的聚合。 */
    private final GlobalAggregateManager aggregateManager;

    /** The library cache, from which the task can request its class loader. */
    /** 库缓存，任务可以从中请求其类加载器 */
    private final LibraryCacheManager.ClassLoaderHandle classLoaderHandle;

    /** The cache for user-defined files that the invokable requires. */
    /** 可调用程序所需的用户定义文件的缓存 */
    private final FileCache fileCache;

    /** The service for kvState registration of this task. */
    /** 此任务的kvState注册服务。 */
    private final KvStateService kvStateService;

    /** The registry of this task which enables live reporting of accumulators. */
    /** 此任务的注册表，它启用累加器的实时报告。 */
    private final AccumulatorRegistry accumulatorRegistry;

    /** The thread that executes the task. */
    /** 执行任务的线程 */
    private final Thread executingThread;

    /** Parent group for all metrics of this task. */
    /** 此任务的所有度量的Group。 */
    private final TaskMetricGroup metrics;

    /** Partition producer state checker to request partition states from. */
    /** 中间分区状态检查器，用于向JobManager查询结果分区的生产者的状态。  */
    private final PartitionProducerStateChecker partitionProducerStateChecker;

    /** Executor to run future callbacks. */
    /** 用于运行Future 回调的执行器 */
    private final Executor executor;

    /** Future that is completed once {@link #run()} exits. */
    /** 异步变成对象用来保存任务完成的状态*/
    private final CompletableFuture<ExecutionState> terminationFuture = new CompletableFuture<>();

    /** The factory of channel state write request executor. */
    /** 通道状态写入请求执行器的工厂 */
    private final ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory;

    // ------------------------------------------------------------------------
    //  Fields that control the task execution. All these fields are volatile
    //  (which means that they introduce memory barriers), to establish
    //  proper happens-before semantics on parallel modification
    // ------------------------------------------------------------------------

    /** atomic flag that makes sure the invokable is canceled exactly once upon error. */
    /** 原子标志，确保可调用在出错时被取消一次。 */
    private final AtomicBoolean invokableHasBeenCanceled;
    /**
     * The invokable of this task, if initialized. All accesses must copy the reference and check
     * for null, as this field is cleared as part of the disposal logic.
     */
    /** Task线程内部调用TaskInvokable.invoke执行StreamTask */
    @Nullable private volatile TaskInvokable invokable;

    /** The current execution state of the task. */
    /** 任务的当前执行状态。 */
    private volatile ExecutionState executionState = ExecutionState.CREATED;

    /** The observed exception, in case the task execution failed. */
    /** 观察到的异常，以防任务执行失败。 */
    private volatile Throwable failureCause;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    /** 已从Flink配置初始化。也可以在ExecutionConfig中设置 */
    private long taskCancellationInterval;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    /** 已从Flink配置初始化。也可以在ExecutionConfig中设置 */
    private long taskCancellationTimeout;

    /**
     * This class loader should be set as the context class loader for threads that may dynamically
     * load user code.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 这个类加载器应该设置为可以动态加载用户代码的线程的上下文类加载器。
    */
    private UserCodeClassLoader userCodeClassLoader;

    /**
     * <b>IMPORTANT:</b> This constructor may not start any work that would need to be undone in the
     * case of a failing task deployment.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建Task任务
    */
    public Task(
            JobInformation jobInformation,
            TaskInformation taskInformation,
            ExecutionAttemptID executionAttemptID,
            AllocationID slotAllocationId,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
            MemoryManager memManager,
            SharedResources sharedResources,
            IOManager ioManager,
            ShuffleEnvironment<?, ?> shuffleEnvironment,
            KvStateService kvStateService,
            BroadcastVariableManager bcVarManager,
            TaskEventDispatcher taskEventDispatcher,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            TaskStateManager taskStateManager,
            TaskManagerActions taskManagerActions,
            InputSplitProvider inputSplitProvider,
            CheckpointResponder checkpointResponder,
            TaskOperatorEventGateway operatorCoordinatorEventGateway,
            GlobalAggregateManager aggregateManager,
            LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
            FileCache fileCache,
            TaskManagerRuntimeInfo taskManagerConfig,
            @Nonnull TaskMetricGroup metricGroup,
            PartitionProducerStateChecker partitionProducerStateChecker,
            Executor executor,
            ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory) {

        Preconditions.checkNotNull(jobInformation);
        Preconditions.checkNotNull(taskInformation);
        this.jobInfo = new JobInfoImpl(jobInformation.getJobId(), jobInformation.getJobName());
        this.taskInfo =
                new TaskInfoImpl(
                        taskInformation.getTaskName(),
                        taskInformation.getMaxNumberOfSubtasks(),
                        executionAttemptID.getSubtaskIndex(),
                        taskInformation.getNumberOfSubtasks(),
                        executionAttemptID.getAttemptNumber(),
                        String.valueOf(slotAllocationId));

        this.jobId = jobInformation.getJobId();
        this.vertexId = taskInformation.getJobVertexId();
        this.executionId = Preconditions.checkNotNull(executionAttemptID);
        this.allocationId = Preconditions.checkNotNull(slotAllocationId);
        this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
        this.jobConfiguration = jobInformation.getJobConfiguration();
        this.taskConfiguration = taskInformation.getTaskConfiguration();
        this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
        this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
        this.nameOfInvokableClass = taskInformation.getInvokableClassName();
        this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();

        Configuration tmConfig = taskManagerConfig.getConfiguration();
        this.taskCancellationInterval = tmConfig.get(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
        this.taskCancellationTimeout = tmConfig.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);

        this.memoryManager = Preconditions.checkNotNull(memManager);
        this.sharedResources = Preconditions.checkNotNull(sharedResources);
        this.ioManager = Preconditions.checkNotNull(ioManager);
        this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
        this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
        this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
        this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

        this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
        this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
        this.operatorCoordinatorEventGateway =
                Preconditions.checkNotNull(operatorCoordinatorEventGateway);
        this.aggregateManager = Preconditions.checkNotNull(aggregateManager);
        this.taskManagerActions = checkNotNull(taskManagerActions);
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.classLoaderHandle = Preconditions.checkNotNull(classLoaderHandle);
        this.fileCache = Preconditions.checkNotNull(fileCache);
        this.kvStateService = Preconditions.checkNotNull(kvStateService);
        this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

        this.metrics = metricGroup;

        this.partitionProducerStateChecker =
                Preconditions.checkNotNull(partitionProducerStateChecker);
        this.executor = Preconditions.checkNotNull(executor);
        this.channelStateExecutorFactory = channelStateExecutorFactory;

        // create the reader and writer structures
        /** 构造包含子任务和ID的任务名称   */
        final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';
        /**
         * 创建一个Shuffle IO的上下文，用于管理任务的Shuffle I/O操作
         * 参数包括任务名称、执行ID以及I/O度量组
         */
        final ShuffleIOOwnerContext taskShuffleContext =
                shuffleEnvironment.createShuffleIOOwnerContext(
                        taskNameWithSubtaskAndId, executionId, metrics.getIOMetricGroup());

        // produced intermediate result partitions
        /**
         * 生成中间结果分区的写入器
         * 这些写入器用于将任务的中间结果写入到Shuffle环境中
         */
        final ResultPartitionWriter[] resultPartitionWriters =
                shuffleEnvironment
                        .createResultPartitionWriters(
                                taskShuffleContext, resultPartitionDeploymentDescriptors)
                        .toArray(new ResultPartitionWriter[] {});

        this.partitionWriters = resultPartitionWriters;

        // consumed intermediate result partitions
        /**
         * 消耗的中间结果分区
         * 创建输入门，用于消费其他任务的中间结果
         */
        final IndexedInputGate[] gates =
                shuffleEnvironment
                        .createInputGates(taskShuffleContext, this, inputGateDeploymentDescriptors)
                        .toArray(new IndexedInputGate[0]);
        /** 初始化输入门数组   */
        this.inputGates = new IndexedInputGate[gates.length];
        int counter = 0;
        for (IndexedInputGate gate : gates) {
            /** 将输入门与I/O度量组中的字节计数器关联起来 */
            inputGates[counter++] =
                    new InputGateWithMetrics(
                            gate, metrics.getIOMetricGroup().getNumBytesInCounter());
        }
        /** 如果Shuffle环境是NettyShuffleEnvironment的实例 */
        if (shuffleEnvironment instanceof NettyShuffleEnvironment) {
            //noinspection deprecation
            /** 注册遗留的网络度量信息 */
            ((NettyShuffleEnvironment) shuffleEnvironment)
                    .registerLegacyNetworkMetrics(
                            metrics.getIOMetricGroup(), resultPartitionWriters, gates);
        }
        /** 创建一个原子布尔变量，用于表示任务是否已被取消 */
        invokableHasBeenCanceled = new AtomicBoolean(false);

        // finally, create the executing thread, but do not start it
        /**
         * 最后，创建一个执行线程
         */
        executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    @Override
    public JobID getJobID() {
        return jobId;
    }

    public JobVertexID getJobVertexId() {
        return vertexId;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public Configuration getTaskConfiguration() {
        return this.taskConfiguration;
    }

    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    public TaskMetricGroup getMetricGroup() {
        return metrics;
    }

    public Thread getExecutingThread() {
        return executingThread;
    }

    @Override
    public CompletableFuture<ExecutionState> getTerminationFuture() {
        return terminationFuture;
    }

    @VisibleForTesting
    long getTaskCancellationInterval() {
        return taskCancellationInterval;
    }

    @VisibleForTesting
    long getTaskCancellationTimeout() {
        return taskCancellationTimeout;
    }

    @Nullable
    @VisibleForTesting
    TaskInvokable getInvokable() {
        return invokable;
    }

    public boolean isBackPressured() {
        if (invokable == null
                || partitionWriters.length == 0
                || (executionState != ExecutionState.INITIALIZING
                        && executionState != ExecutionState.RUNNING)) {
            return false;
        }
        for (int i = 0; i < partitionWriters.length; ++i) {
            if (!partitionWriters[i].isAvailable()) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------------
    //  Task Execution
    // ------------------------------------------------------------------------

    /**
     * Returns the current execution state of the task.
     *
     * @return The current execution state of the task.
     */
    public ExecutionState getExecutionState() {
        return this.executionState;
    }

    /**
     * Checks whether the task has failed, is canceled, or is being canceled at the moment.
     *
     * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
     */
    public boolean isCanceledOrFailed() {
        return executionState == ExecutionState.CANCELING
                || executionState == ExecutionState.CANCELED
                || executionState == ExecutionState.FAILED;
    }

    /** Starts the task's thread. */
    public void startTaskThread() {
        executingThread.start();
    }

    /**
     * If the task has failed, this method gets the exception that caused this task to fail.
     * Otherwise this method returns null.
     *
     * @return The exception that caused the task to fail, or null, if the task has not failed.
     */
    public Throwable getFailureCause() {
        return failureCause;
    }

    /** The core work method that bootstraps the task and executes its code. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发多线程执行
    */
    @Override
    public void run() {
        try {
            doRun();
        } finally {
            terminationFuture.complete(executionState);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 执行Task任务
    */
    private void doRun() {
        // ----------------------------
        //  Initial State transition
        // ----------------------------
        /** 初始状态转换 */
        while (true) {
            /** 获取当前执行状态Create */
            ExecutionState current = this.executionState;
            /** 如果当前执行状态等于Create */
            if (current == ExecutionState.CREATED) {
                /** 将状态设置为DEPLOYING  更改成功跳出循环 */
                if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
                    // success, we can start our work
                    break;
                }
            /** 状态等于异常 直接结束  */
            } else if (current == ExecutionState.FAILED) {
                // we were immediately failed. tell the TaskManager that we reached our final state
                /** 设置为最终状态 */
                notifyFinalState();
                if (metrics != null) {
                    metrics.close();
                }
                return;
                /** 状态是取消中  */
            } else if (current == ExecutionState.CANCELING) {
                //设置状态为取消状态
                if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
                    // we were immediately canceled. tell the TaskManager that we reached our final
                    // state
                    /** 立即失败了。设置为最终状态 */
                    notifyFinalState();
                    //关闭监控
                    if (metrics != null) {
                        metrics.close();
                    }
                    return;
                }
            } else {
                //关闭监控
                if (metrics != null) {
                    metrics.close();
                }
                //抛出异常
                throw new IllegalStateException(
                        "Invalid state for beginning of operation of task " + this + '.');
            }
        }

        // all resource acquisitions and registrations from here on
        // need to be undone in the end
        // 创建一个HashMap，用于存储分布式缓存条目的映射关系。键是字符串，值是一个Future对象，该对象表示异步计算的结果（在这里，它可能是一个文件路径）。
        Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
        // 创建一个TaskInvokable类型的变量，用于任务执行，但在此处初始化为null。
        TaskInvokable invokable = null;

        try {
            // ----------------------------
            //  Task Bootstrap - We periodically
            //  check for canceling as a shortcut
            // ----------------------------
              // 任务启动引导 - 定期检查任务是否被取消，以作为快速响应的捷径
            // activate safety net for task thread
            // 用于防止FileSystem打开的流泄露
            LOG.debug("Creating FileSystem stream leak safety net for task {}", this);
            FileSystemSafetyNet.initializeSafetyNetForThread();

            // first of all, get a user-code classloader
            // this may involve downloading the job's JAR files and/or classes
            // 首先，获取用户代码类加载器。这可能涉及到下载作业的JAR文件或类。
            LOG.info("Loading JAR files for task {}.", this);
            // 创建一个用户代码类加载器，它允许加载和执行用户提供的代码（如Flink作业的JAR文件）。
            userCodeClassLoader = createUserCodeClassloader();
            // 从序列化的ExecutionConfig中反序列化值，并使用用户代码类加载器作为类加载器。
            // ExecutionConfig是Flink作业的执行配置，它包含了关于作业执行的各种设置。
            final ExecutionConfig executionConfig =
                    serializedExecutionConfig.deserializeValue(userCodeClassLoader.asClassLoader());
            // 将ExecutionConfig转换为Configuration对象，该对象是Flink中用于存储配置键值对的标准方式。
            Configuration executionConfigConfiguration = executionConfig.toConfiguration();

            // override task cancellation interval from Flink config if set in ExecutionConfig
            /** 两次连续任务取消尝试之间的时间间隔（以毫秒为单位） 30000  */
            taskCancellationInterval =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_INTERVAL)
                            .orElse(taskCancellationInterval);

            // override task cancellation timeout from Flink config if set in ExecutionConfig
            /** 任务取消在超过指定的毫秒数后超时 180000*/
            taskCancellationTimeout =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT)
                            .orElse(taskCancellationTimeout);
            // 检查任务是否已被取消或失败。如果是，则抛出一个CancelTaskException异常。
            // 这将导致任务执行流程中断，并通知Flink任务管理器任务已被取消。
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            // register the task with the network stack
            // this operation may fail if the system does not have enough
            // memory to run the necessary data exchanges
            // the registration must also strictly be undone
            // ----------------------------------------------------------------

            LOG.debug("Registering task at network: {}.", this);

            // 设置任务的输入分区 。涉及分配和初始化用于数据传输的组件
            setupPartitionsAndGates(partitionWriters, inputGates);
           // 注册每个结果分区写入器，以便事件分发器可以跟踪它们并处理相关事件
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                /** 注册给定的分区以接收传入的任务事件 */
                taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
            }

            // next, kick off the background copying of files for the distributed cache
            try {
                // 从作业配置中读取分布式缓存文件的信息,文件可以很大吗？
                for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                        DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
                    LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
                    // 使用文件缓存（可能是基于磁盘或内存的缓存系统）为给定的分布式缓存条目创建临时文件
                    Future<Path> cp =
                            fileCache.createTmpFile(
                                    entry.getKey(), entry.getValue(), jobId, executionId);
                    // 将文件路径的Future对象存储在distributedCacheEntries映射中，以便后续使用
                    distributedCacheEntries.put(entry.getKey(), cp);
                }
            } catch (Exception e) {
                //抛出异常
                throw new Exception(
                        String.format(
                                "Exception while adding files to distributed cache of task %s (%s).",
                                taskNameWithSubtask, executionId),
                        e);
            }
            // 检查任务是否已被取消或失败。如果是，则抛出一个CancelTaskException异常
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  call the user code initialization methods
            // ----------------------------------------------------------------
             // 创建一个KV状态注册表，用于管理此任务的KV状态（Key-Value State）
            TaskKvStateRegistry kvStateRegistry =
                    kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
            // 创建一个RuntimeEnvironment，它是Flink中用于执行用户代码的环境
            Environment env =
                    new RuntimeEnvironment(
                            jobId, // 作业ID
                            vertexId,// 顶点ID（即算子ID）
                            executionId,//执行ID
                            executionConfig,// 执行配置
                            jobInfo, // 作业信息
                            taskInfo,// 任务信息
                            jobConfiguration,// 作业配置
                            taskConfiguration,//任务配置
                            userCodeClassLoader,// 用户代码类加载器
                            memoryManager,// 内存管理器
                            sharedResources,//共享资源
                            ioManager,// I/O管理器
                            broadcastVariableManager,//广播变量管理器
                            taskStateManager,// 任务状态管理器
                            aggregateManager,// 聚合管理器
                            accumulatorRegistry,// 累加器注册表
                            kvStateRegistry,// KV状态注册表
                            inputSplitProvider,// inputSplit提供者
                            distributedCacheEntries,// 分布式缓存条目
                            partitionWriters,// 结果分区写入器
                            inputGates,// 输入接口类
                            taskEventDispatcher,// 任务事件分发器
                            checkpointResponder,// 检查点响应器
                            operatorCoordinatorEventGateway,//操作符协调器事件网关
                            taskManagerConfig,//任务管理器配置
                            metrics,//度量系统
                            this,// 当前任务实例
                            externalResourceInfoProvider,//外部资源信息提供者
                            channelStateExecutorFactory,//ChannelState的Executor工厂
                            taskManagerActions);//Task与TaskExecutor通信的接口

            // Make sure the user code classloader is accessible thread-locally.
            // We are setting the correct context class loader before instantiating the invokable
            // so that it is available to the invokable during its entire lifetime.
            //设置上下文类加载器
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            // When constructing invokable, separate threads can be constructed and thus should be
            // monitored for system exit (in addition to invoking thread itself monitored below).
            /**
             * ThreadLocal<Boolean> monitorUserSystemExit
             * 监控用户推出当前线程，比如退出虚拟机System.exit
             * 内部就是设置ThreadLocal设置为true，进行检查
             */
            FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
            try {
                // now load and instantiate the task's invokable code
                //加载和实例化用户代码对应的类如OneInputStreamTask
                invokable =
                        loadAndInstantiateInvokable(
                                userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
            } finally {
                //卸载监控系统 ThreadLocal设置为false
                FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            }

            // ----------------------------------------------------------------
            //  actual task core work
            // ----------------------------------------------------------------

            // we must make strictly sure that the invokable is accessible to the cancel() call
            // by the time we switched to running.
            /** 赋值 */
            this.invokable = invokable;
            /** 恢复并调用 */
             restoreAndInvoke(invokable);

            // make sure, we enter the catch block if the task leaves the invoke() method due
            // to the fact that it has been canceled
            //检查取消或失败状态：
            if (isCanceledOrFailed()) {
                //抛出异常
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  finalization of a successful execution
            // ----------------------------------------------------------------

            // finish the produced partitions. if this fails, we consider the execution failed.
            //// 遍历所有的分区写入器
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                // 如果分区写入器不为空（即它已经被初始化或使用过）
                if (partitionWriter != null) {
                    // 调用它的 finish 方法来完成写入并释放相关资源
                    partitionWriter.finish();
                }
            }

            // try to mark the task as finished
            // if that fails, the task was canceled/failed in the meantime
            // 尝试将任务标记为已完成
            if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
                //抛出异常
                throw new CancelTaskException();
           // 如果标记失败，说明在此期间任务被取消或失败了
            }
        } catch (Throwable t) {
            // ----------------------------------------------------------------
            // the execution failed. either the invokable code properly failed, or
            // an exception was thrown as a side effect of cancelling
            // ----------------------------------------------------------------
            // 对异常进行预处理，可能涉及异常类型的转换或添加额外信息
            t = preProcessException(t);

            try {
                // transition into our final state. we should be either in DEPLOYING, INITIALIZING,
                // RUNNING, CANCELING, or FAILED
                // loop for multiple retries during concurrent state changes via calls to cancel()
                // or to failExternally()
                /**
                 * 循环，将状态设置为结束状态、FINISHED、CANCELED、FAILED
                 */
                while (true) {
                    // 获取当前执行状态
                    ExecutionState current = this.executionState;
                    // 如果当前状态是 RUNNING、INITIALIZING 或 DEPLOYING
                    if (current == ExecutionState.RUNNING
                            || current == ExecutionState.INITIALIZING
                            || current == ExecutionState.DEPLOYING) {
                        // 检查异常链中是否存在 CancelTaskException 类型的异常
                        if (ExceptionUtils.findThrowable(t, CancelTaskException.class)
                                .isPresent()) {
                            // 如果存在，尝试将状态过渡到 CANCELED
                            if (transitionState(current, ExecutionState.CANCELED, t)) {
                                // 如果状态过渡成功，取消 invokable（可能是一个任务或操作）
                                cancelInvokable(invokable);
                                //跳出循环
                                break;
                            }
                        } else {
                            // 如果不存在 CancelTaskException，尝试将状态过渡到 FAILED
                            if (transitionState(current, ExecutionState.FAILED, t)) {
                                // 如果状态过渡成功，取消 invokable
                                cancelInvokable(invokable);
                                //跳出循环
                                break;
                            }
                        }
                        // 如果当前状态是 CANCELING
                    } else if (current == ExecutionState.CANCELING) {
                        // 尝试将状态过渡到 CANCELED
                        if (transitionState(current, ExecutionState.CANCELED)) {
                            //跳出循环
                            break;
                        }
                        // 如果当前状态是 FAILED
                    } else if (current == ExecutionState.FAILED) {
                        // in state failed already, no transition necessary any more
                        break;//跳出循环
                    }
                    // 如果遇到意外的状态，将其过渡到 FAILED
                    // unexpected state, go to failed
                    else if (transitionState(current, ExecutionState.FAILED, t)) {
                        LOG.error(
                                "Unexpected state in task {} ({}) during an exception: {}.",
                                taskNameWithSubtask,
                                executionId,
                                current);
                        break;
                    }
                    // else fall through the loop and
                }
            } catch (Throwable tt) {
                //打印日志抛出异常
                String message =
                        String.format(
                                "FATAL - exception in exception handler of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, tt);
                notifyFatalError(message, tt);
            }
        } finally {
            try {
                // 记录日志，表示正在释放任务的资源
                LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

                // clear the reference to the invokable. this helps guard against holding references
                // to the invokable and its structures in cases where this Task object is still
                // referenced
                // 清除对invokable的引用，有助于防止在Task对象仍被引用时，
                this.invokable = null;

                // free the network resources
                // 释放内存资源
                releaseResources();

                // free memory resources
                // 如果invokable不为空，释放其内存资源
                if (invokable != null) {
                    memoryManager.releaseAll(invokable);
                }

                // remove all of the tasks resources
                // 释放与任务相关的所有文件缓存资源
                fileCache.releaseJob(jobId, executionId);

                // close and de-activate safety net for task thread
                LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
                // 关闭任务线程的FileSystem流，并停用安全网
                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
                // 通知任务的最终状态
                notifyFinalState();
            } catch (Throwable t) {
                // an error in the resource cleanup is fatal
                String message =
                        String.format(
                                "FATAL - exception in resource cleanup of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, t);
                // 通知致命错误
                notifyFatalError(message, t);
            }

            // un-register the metrics at the end so that the task may already be
            // counted as finished when this happens
            // errors here will only be logged
            //在最后注销度量指标，这样当这个过程发生时，任务可能已经被视为完成
            try {
                metrics.close();
            } catch (Throwable t) {
                LOG.error(
                        "Error during metrics de-registration of task {} ({}).",
                        taskNameWithSubtask,
                        executionId,
                        t);
            }
        }
    }

    /** Unwrap, enrich and handle fatal errors. */
    private Throwable preProcessException(Throwable t) {
        // unwrap wrapped exceptions to make stack traces more compact
        if (t instanceof WrappingRuntimeException) {
            t = ((WrappingRuntimeException) t).unwrap();
        }

        TaskManagerExceptionUtils.tryEnrichTaskManagerError(t);

        // check if the exception is unrecoverable
        if (ExceptionUtils.isJvmFatalError(t)
                || (t instanceof OutOfMemoryError
                        && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

            // terminate the JVM immediately
            // don't attempt a clean shutdown, because we cannot expect the clean shutdown
            // to complete
            try {
                LOG.error(
                        "Encountered fatal error {} - terminating the JVM",
                        t.getClass().getName(),
                        t);
            } finally {
                Runtime.getRuntime().halt(-1);
            }
        }

        return t;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 恢复并执行
    */
    private void restoreAndInvoke(TaskInvokable finalInvokable) throws Exception {
        try {
            // switch to the INITIALIZING state, if that fails, we have been canceled/failed in the
            // meantime
            // 尝试将状态从DEPLOYING切换到INITIALIZING，如果切换失败（可能在此时被取消或失败）
            if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
                // 抛出取消任务异常
                throw new CancelTaskException();
            }
            // 更新任务执行状态为INITIALIZING
            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.INITIALIZING));
            // make sure the user code classloader is accessible thread-locally
            //设置类加载器
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());
            // 设置类加载器 确保用户代码类加载器在线程本地是可访问的
            runWithSystemExitMonitoring(finalInvokable::restore);// 恢复任务
            // 尝试将状态从INITIALIZING切换到RUNNING，如果切换失败，抛出取消任务异常
            if (!transitionState(ExecutionState.INITIALIZING, ExecutionState.RUNNING)) {
                throw new CancelTaskException();
            }

            // notify everyone that we switched to running
            // 通知所有人已切换到RUNNING状态
            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.RUNNING));
            // 调用任务的invoke方法
            runWithSystemExitMonitoring(finalInvokable::invoke);
        } catch (Throwable throwable) {
            try {
                // 使用捕获的异常进行清理工作
                runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(throwable));
            } catch (Throwable cleanUpThrowable) {
                // 捕获异常，并尝试进行清理工作
                throwable.addSuppressed(cleanUpThrowable);
            }
            throw throwable;
        }
        runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(null));
    }

    /**
     * Monitor user codes from exiting JVM covering user function invocation. This can be done in a
     * finer-grained way like enclosing user callback functions individually, but as exit triggered
     * by framework is not performed and expected in this invoke function anyhow, we can monitor
     * exiting JVM for entire scope.
     */
    private void runWithSystemExitMonitoring(RunnableWithException action) throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            action.run();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置结果分区和输入
     * @param producedPartitions 产生的结果分区数组
     * @param inputGates         输入门数组
     * @throws IOException 如果在设置分区或输入门时发生I/O错误
    */
    @VisibleForTesting
    public static void setupPartitionsAndGates(
            ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException {

        /** 遍历产生的结果分区数组   */
        for (ResultPartitionWriter partition : producedPartitions) {
            /** 准备每个分区 */
            partition.setup();
        }

        // InputGates must be initialized after the partitions, since during InputGate#setup
        // we are requesting partitions
        /**
         * 遍历InputGate数组，对每个输入门进行设置或初始化。
         * 我们调用InputGate的setup方法来准备每个InputGate。
         */
        for (InputGate gate : inputGates) {
            /** 设置每个InputGate*/
            gate.setup();
        }
    }

    /**
     * Releases resources before task exits. We should also fail the partition to release if the
     * task has failed, is canceled, or is being canceled at the moment.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private void releaseResources() {
        // 记录日志，表示正在释放任务及其子任务的网络资源，并显示当前任务的执行状态
        LOG.debug(
                "Release task {} network resources (state: {}).",
                taskNameWithSubtask,
                getExecutionState());
        // 遍历所有的ResultPartitionWriter，解除它们与任务事件分派器的注册关系
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            // 取消特定分区ID的注册，这样在后续操作中就不会再处理该分区的任何事件
            taskEventDispatcher.unregisterPartition(partitionWriter.getPartitionId());
        }

        // close network resources
        // 如果任务被取消或失败，则对所有结果分区进行失败处理
        // 这可能是为了通知下游任务或其他组件，该任务未能成功完成其工作
        if (isCanceledOrFailed()) {
            failAllResultPartitions();
        }
        // 关闭所有结果分区，释放相关的网络资源和其他资源,内部就是关闭BufferPool
        closeAllResultPartitions();
        //  关闭所有的Input Gates，这通常是数据输入的通道，关闭它们可以停止数据的进一步流入
        closeAllInputGates();

        try {
            // 尝试关闭任务状态管理器，这可能会释放与任务状态管理相关的资源
            taskStateManager.close();
        } catch (Exception e) {
            // 如果在关闭任务状态管理器时发生异常，记录错误日志
            LOG.error("Failed to close task state manager for task {}.", taskNameWithSubtask, e);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置所有的ResultPartition为失败
     * AutoCloseable是一个接口，它定义了一个close()方法，该方法用于释放与该对象关联的资源。通常，
     * 当你处理需要关闭的资源（如文件、数据库连接、网络连接等）时，
     * 实现AutoCloseable接口可以帮助你更方便地管理这些资源。
    */
    private void failAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            //设置异常
            partitionWriter.fail(getFailureCause());
        }
    }

    private void closeAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            try {

                partitionWriter.close();
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                LOG.error(
                        "Failed to release result partition for task {}.", taskNameWithSubtask, t);
            }
        }
    }

    private void closeAllInputGates() {
        TaskInvokable invokable = this.invokable;
        if (invokable == null || !invokable.isUsingNonBlockingInput()) {
            // Cleanup resources instead of invokable if it is null, or prevent it from being
            // blocked on input, or interrupt if it is already blocked. Not needed for StreamTask
            // (which does NOT use blocking input); for which this could cause race conditions
            for (InputGate inputGate : inputGates) {
                try {
                    inputGate.close();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    LOG.error("Failed to release input gate for task {}.", taskNameWithSubtask, t);
                }
            }
        }
    }

    private UserCodeClassLoader createUserCodeClassloader() throws Exception {
        long startDownloadTime = System.currentTimeMillis();

        // triggers the download of all missing jar files from the job manager
        final UserCodeClassLoader userCodeClassLoader =
                classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);

        LOG.debug(
                "Getting user code class loader for task {} at library cache manager took {} milliseconds",
                executionId,
                System.currentTimeMillis() - startDownloadTime);

        return userCodeClassLoader;
    }

    private void notifyFinalState() {
        /** 检查ExecutionState状态是否为完成状态 FINISHED、CANCELED、FAILED */
        checkState(executionState.isTerminal());
        /** 向任务管理器通知任务执行状态更新。 */
        taskManagerActions.updateTaskExecutionState(
                new TaskExecutionState(executionId, executionState, failureCause));
    }

    private void notifyFatalError(String message, Throwable cause) {
        taskManagerActions.notifyFatalError(message, cause);
    }

    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @return true if the transition was successful, otherwise false
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试将执行状态从当前状态转换为新状态。
    */
    private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
        return transitionState(currentState, newState, null);
    }

    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @param cause of the transition change or null
     * @return true if the transition was successful, otherwise false
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试将执行状态从当前状态转换为新状态。
    */
    private boolean transitionState(
            ExecutionState currentState, ExecutionState newState, Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
            if (cause == null) {
                LOG.info(
                        "{} ({}) switched from {} to {}.",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState);
            } else if (ExceptionUtils.findThrowable(cause, CancelTaskException.class).isPresent()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "{} ({}) switched from {} to {} due to CancelTaskException:",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState,
                            cause);
                } else {
                    LOG.info(
                            "{} ({}) switched from {} to {} due to CancelTaskException.",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState);
                }
            } else {
                // proper failure of the task. record the exception as the root
                // cause
                failureCause = cause;
                LOG.warn(
                        "{} ({}) switched from {} to {} with failure cause:",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState,
                        cause);
            }

            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    //  Canceling / Failing the task from the outside
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Cancels the task execution. If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to CANCELING, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     */
    public void cancelExecution() {
        LOG.info("Attempting to cancel task {} ({}).", taskNameWithSubtask, executionId);
        cancelOrFailAndCancelInvokable(ExecutionState.CANCELING, null);
    }

    /**
     * Marks task execution failed for an external reason (a reason other than the task code itself
     * throwing an exception). If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to FAILED, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void failExternally(Throwable cause) {
        LOG.info("Attempting to fail task externally {} ({}).", taskNameWithSubtask, executionId);
        cancelOrFailAndCancelInvokable(ExecutionState.FAILED, cause);
    }

    private void cancelOrFailAndCancelInvokable(ExecutionState targetState, Throwable cause) {
        try {
            cancelOrFailAndCancelInvokableInternal(targetState, cause);
        } catch (Throwable t) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(t)) {
                String message =
                        String.format(
                                "FATAL - exception in cancelling task %s (%s).",
                                taskNameWithSubtask, executionId);
                notifyFatalError(message, t);
            } else {
                throw t;
            }
        }
    }

    @VisibleForTesting
    void cancelOrFailAndCancelInvokableInternal(ExecutionState targetState, Throwable cause) {
        cause = preProcessException(cause);

        while (true) {
            ExecutionState current = executionState;

            // if the task is already canceled (or canceling) or finished or failed,
            // then we need not do anything
            if (current.isTerminal() || current == ExecutionState.CANCELING) {
                LOG.info("Task {} is already in state {}", taskNameWithSubtask, current);
                return;
            }

            if (current == ExecutionState.DEPLOYING || current == ExecutionState.CREATED) {
                if (transitionState(current, targetState, cause)) {
                    // if we manage this state transition, then the invokable gets never called
                    // we need not call cancel on it
                    return;
                }
            } else if (current == ExecutionState.INITIALIZING
                    || current == ExecutionState.RUNNING) {
                if (transitionState(current, targetState, cause)) {
                    // we are canceling / failing out of the running state
                    // we need to cancel the invokable

                    // copy reference to guard against concurrent null-ing out the reference
                    final TaskInvokable invokable = this.invokable;

                    if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
                        LOG.info(
                                "Triggering cancellation of task code {} ({}).",
                                taskNameWithSubtask,
                                executionId);

                        // because the canceling may block on user code, we cancel from a separate
                        // thread
                        // we do not reuse the async call handler, because that one may be blocked,
                        // in which
                        // case the canceling could not continue

                        // The canceller calls cancel and interrupts the executing thread once
                        Runnable canceler =
                                new TaskCanceler(
                                        LOG, invokable, executingThread, taskNameWithSubtask);

                        Thread cancelThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        canceler,
                                        String.format(
                                                "Canceler for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        cancelThread.setDaemon(true);
                        cancelThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        cancelThread.start();

                        // the periodic interrupting thread - a different thread than the canceller,
                        // in case
                        // the application code does blocking stuff in its cancellation paths.
                        Runnable interrupter =
                                new TaskInterrupter(
                                        LOG,
                                        invokable,
                                        executingThread,
                                        taskNameWithSubtask,
                                        taskCancellationInterval);

                        Thread interruptingThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        interrupter,
                                        String.format(
                                                "Canceler/Interrupts for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        interruptingThread.setDaemon(true);
                        interruptingThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        interruptingThread.start();

                        // if a cancellation timeout is set, the watchdog thread kills the process
                        // if graceful cancellation does not succeed
                        if (taskCancellationTimeout > 0) {
                            Runnable cancelWatchdog =
                                    new TaskCancelerWatchDog(
                                            taskInfo,
                                            executingThread,
                                            taskManagerActions,
                                            taskCancellationTimeout);

                            Thread watchDogThread =
                                    new Thread(
                                            executingThread.getThreadGroup(),
                                            cancelWatchdog,
                                            String.format(
                                                    "Cancellation Watchdog for %s (%s).",
                                                    taskNameWithSubtask, executionId));
                            watchDogThread.setDaemon(true);
                            watchDogThread.setUncaughtExceptionHandler(
                                    FatalExitExceptionHandler.INSTANCE);
                            watchDogThread.start();
                        }
                    }
                    return;
                }
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unexpected state: %s of task %s (%s).",
                                current, taskNameWithSubtask, executionId));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Partition State Listeners
    // ------------------------------------------------------------------------

    @Override
    public void requestPartitionProducerState(
            final IntermediateDataSetID intermediateDataSetId,
            final ResultPartitionID resultPartitionId,
            Consumer<? super ResponseHandle> responseConsumer) {

        final CompletableFuture<ExecutionState> futurePartitionState =
                partitionProducerStateChecker.requestPartitionProducerState(
                        jobId, intermediateDataSetId, resultPartitionId);

        FutureUtils.assertNoException(
                futurePartitionState
                        .handle(PartitionProducerStateResponseHandle::new)
                        .thenAcceptAsync(responseConsumer, executor));
    }

    // ------------------------------------------------------------------------
    //  Notifications on the invokable
    // ------------------------------------------------------------------------

    /**
     * Calls the invokable to trigger a checkpoint.
     *
     * @param checkpointID The ID identifying the checkpoint.
     * @param checkpointTimestamp The timestamp associated with the checkpoint.
     * @param checkpointOptions Options for performing this checkpoint.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发Task类中的triggerCheckpointBarrier来进行检查的的操作
     *
     * @param checkpointID 标识检查点的ID。
     * @param checkpointTimestamp 与检查点关联的时间戳。
     * @param checkpointOptions 执行此检查点的选项。
    */
    public void triggerCheckpointBarrier(
            final long checkpointID,
            final long checkpointTimestamp,
            final CheckpointOptions checkpointOptions) {
        /**
         * 获取当前任务的可调用对象实例(StreamTask、DataSourceTask都实现了TaskInvokable)
         */
        final TaskInvokable invokable = this.invokable;
        // 创建检查点元数据，包括检查点ID、时间戳以及当前系统时间
        /**
         *
         * 1.checkpointID checkpoint时间，当前时间作为元数据，创建检查点元数据对象CheckpointMetaData
         */
        final CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(
                        checkpointID, checkpointTimestamp, System.currentTimeMillis());
        // 检查当前任务是否处于运行状态
        if (executionState == ExecutionState.RUNNING) {
            /**
             * 判断当前invokable具体对象实例是否实现了CheckpointableTask接口，即是否支持检查点
             * (StreamTask、DataSourceTask都实现了CheckpointableTask)
             */
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                /**
                 * 2.invokable转换为CheckpointableTask对象调用triggerCheckpointAsync异步触发检查点，返回结果为CompletableFuture
                 */
                ((CheckpointableTask) invokable)
                         //触发CheckPoint
                        .triggerCheckpointAsync(checkpointMetaData, checkpointOptions)
                        //检查点结束后回调，判断异常如果异常了则表示检查点处理失败则向JobMaster汇报此次检查点失败
                        .handle(
                                // 使用CompletableFuture的handle方法处理触发检查点的结果
                                (triggerResult, exception) -> {
                                    // 如果发生异常或触发结果不为真（即触发失败）
                                    if (exception != null || !triggerResult) {
                                        // 拒绝该检查点，并指定失败原因和异常（如果有）
                                        declineCheckpoint(
                                                checkpointID,
                                                CheckpointFailureReason.TASK_FAILURE,
                                                exception);
                                        // 返回false表示处理失败
                                        return false;
                                    }
                                    return true;
                                });
                // 捕获RejectedExecutionException异常
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                // 如果在尝试提交任务到邮箱时被拒绝，可能是因为邮箱已关闭
                // 这通常意味着任务正在关闭中，因此我们只需忽略它
                LOG.debug(
                        "Triggering checkpoint {} for {} ({}) was rejected by the mailbox",
                        checkpointID,
                        taskNameWithSubtask,
                        executionId);
                // 拒绝该检查点，并指定失败原因为任务关闭
                declineCheckpoint(
                        checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING);
                // 捕获所有其他类型的Throwable异常
            } catch (Throwable t) {
                // 如果任务当前处于运行状态
                if (getExecutionState() == ExecutionState.RUNNING) {
                    // 触发检查点过程中发生错误，我们将其视为外部失败
                    failExternally(
                            new Exception(
                                    "Error while triggering checkpoint "
                                            + checkpointID
                                            + " for "
                                            + taskNameWithSubtask,
                                    t));
                } else {
                    // 如果任务不在运行状态，则记录调试日志
                    LOG.debug(
                            "Encountered error while triggering checkpoint {} for "
                                    + "{} ({}) while being not in state running.",
                            checkpointID,
                            taskNameWithSubtask,
                            executionId,
                            t);
                }
            }
            // 如果任务不在运行状态，则不需要触发检查点
        } else {
            // 记录调试日志，表示拒绝了非运行状态的任务的检查点请求
            LOG.debug(
                    "Declining checkpoint request for non-running task {} ({}).",
                    taskNameWithSubtask,
                    executionId);

            // send back a message that we did not do the checkpoint
            // 发送消息表明我们没有进行这个检查点
            // 并指定失败原因为任务未就绪
            declineCheckpoint(
                    checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
    }

    private void declineCheckpoint(long checkpointID, CheckpointFailureReason failureReason) {
        declineCheckpoint(checkpointID, failureReason, null);
    }

    private void declineCheckpoint(
            long checkpointID,
            CheckpointFailureReason failureReason,
            @Nullable Throwable failureCause) {
        checkpointResponder.declineCheckpoint(
                jobId,
                executionId,
                checkpointID,
                new CheckpointException(
                        "Task name with subtask : " + taskNameWithSubtask,
                        failureReason,
                        failureCause));
    }

    public void notifyCheckpointComplete(final long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.COMPLETE);
    }

    public void notifyCheckpointAborted(
            final long checkpointID, final long latestCompletedCheckpointId) {
        notifyCheckpoint(
                checkpointID, latestCompletedCheckpointId, NotifyCheckpointOperation.ABORT);
    }

    public void notifyCheckpointSubsumed(long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.SUBSUME);
    }

    private void notifyCheckpoint(
            long checkpointId,
            long latestCompletedCheckpointId,
            NotifyCheckpointOperation notifyCheckpointOperation) {
        TaskInvokable invokable = this.invokable;

        if (executionState == ExecutionState.RUNNING && invokable != null) {
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointAbortAsync(
                                        checkpointId, latestCompletedCheckpointId);
                        break;
                    case COMPLETE:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointCompleteAsync(checkpointId);
                        break;
                    case SUBSUME:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointSubsumedAsync(checkpointId);
                }
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                LOG.debug(
                        "Notify checkpoint {}} {} for {} ({}) was rejected by the mailbox.",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskNameWithSubtask,
                        executionId);
            } catch (Throwable t) {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                    case COMPLETE:
                        if (getExecutionState() == ExecutionState.RUNNING) {
                            failExternally(
                                    new RuntimeException(
                                            String.format(
                                                    "Error while notify checkpoint %s.",
                                                    notifyCheckpointOperation),
                                            t));
                        }
                        break;
                    case SUBSUME:
                        // just rethrow the throwable out as we do not expect notification of
                        // subsume could fail the task.
                        ExceptionUtils.rethrow(t);
                }
            }
        } else {
            LOG.info(
                    "Ignoring checkpoint {} notification for non-running task {}.",
                    notifyCheckpointOperation,
                    taskNameWithSubtask);
        }
    }

    /**
     * Dispatches an operator event to the invokable task.
     *
     * <p>If the event delivery did not succeed, this method throws an exception. Callers can use
     * that exception for error reporting, but need not react with failing this task (this method
     * takes care of that).
     *
     * @throws FlinkException This method throws exceptions indicating the reason why delivery did
     *     not succeed.
     */
    public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt)
            throws FlinkException {
        final TaskInvokable invokable = this.invokable;
        final ExecutionState currentState = this.executionState;

        if (invokable == null
                || (currentState != ExecutionState.RUNNING
                        && currentState != ExecutionState.INITIALIZING)) {
            throw new TaskNotRunningException("Task is not running, but in state " + currentState);
        }

        if (invokable instanceof CoordinatedTask) {
            try {
                ((CoordinatedTask) invokable).dispatchOperatorEvent(operator, evt);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                if (getExecutionState() == ExecutionState.RUNNING
                        || getExecutionState() == ExecutionState.INITIALIZING) {
                    FlinkException e = new FlinkException("Error while handling operator event", t);
                    failExternally(e);
                    throw e;
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void cancelInvokable(TaskInvokable invokable) {
        // in case of an exception during execution, we still call "cancel()" on the task
        if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
            try {
                invokable.cancel();
            } catch (Throwable t) {
                LOG.error("Error while canceling task {}.", taskNameWithSubtask, t);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
    }

    @VisibleForTesting
    class PartitionProducerStateResponseHandle implements ResponseHandle {
        private final Either<ExecutionState, Throwable> result;

        PartitionProducerStateResponseHandle(
                @Nullable ExecutionState producerState, @Nullable Throwable t) {
            this.result = producerState != null ? Either.Left(producerState) : Either.Right(t);
        }

        @Override
        public ExecutionState getConsumerExecutionState() {
            return executionState;
        }

        @Override
        public Either<ExecutionState, Throwable> getProducerExecutionState() {
            return result;
        }

        @Override
        public void cancelConsumption() {
            cancelExecution();
        }

        @Override
        public void failConsumption(Throwable cause) {
            failExternally(cause);
        }
    }

    /**
     * Instantiates the given task invokable class, passing the given environment (and possibly the
     * initial task state) to the task's constructor.
     *
     * <p>The method will first try to instantiate the task via a constructor accepting both the
     * Environment and the TaskStateSnapshot. If no such constructor exists, and there is no initial
     * state, the method will fall back to the stateless convenience constructor that accepts only
     * the Environment.
     *
     * @param classLoader The classloader to load the class through.
     * @param className The name of the class to load.
     * @param environment The task environment.
     * @return The instantiated invokable task object.
     * @throws Throwable Forwards all exceptions that happen during initialization of the task. Also
     *     throws an exception if the task class misses the necessary constructor.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 加载并实例化一个可调用的任务类（TaskInvokable）。
     * @param classLoader 类加载器，用于加载指定类
     * @param className   要加载的类的全限定名
     * @param environment 执行环境，用于传递给任务的构造函数
     * @return 实例化后的 TaskInvokable 对象
     * @throws Throwable 当加载或实例化过程中发生错误时抛出
    */
    private static TaskInvokable loadAndInstantiateInvokable(
            ClassLoader classLoader, String className, Environment environment) throws Throwable {

        // 声明一个 TaskInvokable 类型的子类变量
        final Class<? extends TaskInvokable> invokableClass;
        try {
            // 使用类加载器加载指定类，并强制转换为 TaskInvokable 的子类
            invokableClass =
                    Class.forName(className, true, classLoader).asSubclass(TaskInvokable.class);
        } catch (Throwable t) {
            // 如果加载类失败，则抛出异常，说明无法加载任务的可调用类
            throw new Exception("Could not load the task's invokable class.", t);
        }
        // 声明一个 TaskInvokable 类型的无状态构造函数
        Constructor<? extends TaskInvokable> statelessCtor;

        try {
            // 获取 TaskInvokable 子类的构造函数，该构造函数接收一个 Environment 类型的参数
            statelessCtor = invokableClass.getConstructor(Environment.class);
        } catch (NoSuchMethodException ee) {
            // 如果找不到匹配的构造函数，则抛出 FlinkException 异常，说明任务缺少正确的构造函数
            throw new FlinkException("Task misses proper constructor", ee);
        }

        // instantiate the class
        try {
            //noinspection ConstantConditions  --> cannot happen
            // 实例化 TaskInvokable 子类，并传入环境参数
            return statelessCtor.newInstance(environment);
        } catch (InvocationTargetException e) {
            // directly forward exceptions from the eager initialization
            // 如果构造函数调用时发生异常，则直接转发该异常（即构造函数内部抛出的异常）
            throw e.getTargetException();
        } catch (Exception e) {
            // 如果实例化过程中发生其他异常，则抛出 FlinkException 异常，说明无法实例化任务的可调用类
            throw new FlinkException("Could not instantiate the task's invokable class.", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Task cancellation
    //
    //  The task cancellation uses in total three threads, as a safety net
    //  against various forms of user- and JVM bugs.
    //
    //    - The first thread calls 'cancel()' on the invokable and closes
    //      the input and output connections, for fast thread termination
    //    - The second thread periodically interrupts the invokable in order
    //      to pull the thread out of blocking wait and I/O operations
    //    - The third thread (watchdog thread) waits until the cancellation
    //      timeout and then performs a hard cancel (kill process, or let
    //      the TaskManager know)
    //
    //  Previously, thread two and three were in one thread, but we needed
    //  to separate this to make sure the watchdog thread does not call
    //  'interrupt()'. This is a workaround for the following JVM bug
    //   https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8138622
    // ------------------------------------------------------------------------

    /**
     * This runner calls cancel() on the invokable, closes input-/output resources, and initially
     * interrupts the task thread.
     */
    private class TaskCanceler implements Runnable {

        private final Logger logger;
        private final TaskInvokable invokable;
        private final Thread executor;
        private final String taskName;

        TaskCanceler(Logger logger, TaskInvokable invokable, Thread executor, String taskName) {
            this.logger = logger;
            this.invokable = invokable;
            this.executor = executor;
            this.taskName = taskName;
        }

        @Override
        public void run() {
            try {
                // the user-defined cancel method may throw errors.
                // we need do continue despite that
                try {
                    invokable.cancel();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    logger.error("Error while canceling the task {}.", taskName, t);
                }

                // Early release of input and output buffer pools. We do this
                // in order to unblock async Threads, which produce/consume the
                // intermediate streams outside of the main Task Thread (like
                // the Kafka consumer).
                // Notes: 1) This does not mean to release all network resources,
                // the task thread itself will release them; 2) We can not close
                // ResultPartitions here because of possible race conditions with
                // Task thread so we just call the fail here.
                failAllResultPartitions();
                closeAllInputGates();

                invokable.maybeInterruptOnCancel(executor, null, null);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                logger.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /** This thread sends the delayed, periodic interrupt calls to the executing thread. */
    private static final class TaskInterrupter implements Runnable {

        /** The logger to report on the fatal condition. */
        private final Logger log;

        /** The invokable task. */
        private final TaskInvokable task;

        /** The executing task thread that we wait for to terminate. */
        private final Thread executorThread;

        /** The name of the task, for logging purposes. */
        private final String taskName;

        /** The interval in which we interrupt. */
        private final long interruptIntervalMillis;

        TaskInterrupter(
                Logger log,
                TaskInvokable task,
                Thread executorThread,
                String taskName,
                long interruptIntervalMillis) {

            this.log = log;
            this.task = task;
            this.executorThread = executorThread;
            this.taskName = taskName;
            this.interruptIntervalMillis = interruptIntervalMillis;
        }

        @Override
        public void run() {
            try {
                // we initially wait for one interval
                // in most cases, the threads go away immediately (by the cancellation thread)
                // and we need not actually do anything
                executorThread.join(interruptIntervalMillis);

                // log stack trace where the executing thread is stuck and
                // interrupt the running thread periodically while it is still alive
                while (executorThread.isAlive()) {
                    task.maybeInterruptOnCancel(executorThread, taskName, interruptIntervalMillis);
                    try {
                        executorThread.join(interruptIntervalMillis);
                    } catch (InterruptedException e) {
                        // we ignore this and fall through the loop
                    }
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                log.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /**
     * Watchdog for the cancellation. If the task thread does not go away gracefully within a
     * certain time, we trigger a hard cancel action (notify TaskManager of fatal error, which in
     * turn kills the process).
     */
    private static class TaskCancelerWatchDog implements Runnable {

        /** The executing task thread that we wait for to terminate. */
        private final Thread executorThread;

        /** The TaskManager to notify if cancellation does not happen in time. */
        private final TaskManagerActions taskManager;

        /** The timeout for cancellation. */
        private final long timeoutMillis;

        private final TaskInfo taskInfo;

        TaskCancelerWatchDog(
                TaskInfo taskInfo,
                Thread executorThread,
                TaskManagerActions taskManager,
                long timeoutMillis) {

            checkArgument(timeoutMillis > 0);

            this.taskInfo = taskInfo;
            this.executorThread = executorThread;
            this.taskManager = taskManager;
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public void run() {
            try {
                Deadline timeout = Deadline.fromNow(Duration.ofMillis(timeoutMillis));
                while (executorThread.isAlive() && timeout.hasTimeLeft()) {
                    try {
                        executorThread.join(Math.max(1, timeout.timeLeft().toMillis()));
                    } catch (InterruptedException ignored) {
                        // we don't react to interrupted exceptions, simply fall through the loop
                    }
                }

                if (executorThread.isAlive()) {
                    logTaskThreadStackTrace(
                            executorThread,
                            taskInfo.getTaskNameWithSubtasks(),
                            timeoutMillis,
                            "notifying TM");
                    String msg =
                            "Task did not exit gracefully within "
                                    + (timeoutMillis / 1000)
                                    + " + seconds.";
                    taskManager.notifyFatalError(msg, new FlinkRuntimeException(msg));
                }
            } catch (Throwable t) {
                throw new FlinkRuntimeException("Error in Task Cancellation Watch Dog", t);
            }
        }
    }

    public static void logTaskThreadStackTrace(
            Thread thread, String taskName, long timeoutMs, String action) {
        StackTraceElement[] stack = thread.getStackTrace();
        StringBuilder stackTraceStr = new StringBuilder();
        for (StackTraceElement e : stack) {
            stackTraceStr.append(e).append('\n');
        }

        LOG.warn(
                "Task '{}' did not react to cancelling signal - {}; it is stuck for {} seconds in method:\n {}",
                taskName,
                action,
                timeoutMs / 1000,
                stackTraceStr);
    }

    /** Various operation of notify checkpoint. */
    public enum NotifyCheckpointOperation {
        ABORT,
        COMPLETE,
        SUBSUME
    }
}
