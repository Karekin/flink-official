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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.IndexedCombinedWatermarkStatus;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.CheckpointedStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.LatencyStats;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * 所有流运算符的基类。包含用户函数的运算符应扩展 {@link AbstractUdfStreamOperator}（它是该类的专门子类）。
 *
 * <p>具体实现类必须同时实现以下两个接口之一，以标记运算符是单输入（unary）还是双输入（binary）：
 * {@link OneInputStreamOperator} 或 {@link TwoInputStreamOperator}。
 *
 * <p>{@code StreamOperator} 的方法保证不会被并发调用。此外，如果使用定时器服务，
 * 定时器回调也保证不会与 {@code StreamOperator} 上的方法并发调用。
 *
 * <p>请注意，本类未来将被 {@link AbstractStreamOperatorV2} 取代。
 * 但由于 {@link AbstractStreamOperatorV2} 目前仍是实验性的，因此 {@link AbstractStreamOperator} 仍未被标记为废弃。
 *
 * @param <OUT> 运算符的输出类型。
 */
@PublicEvolving
public abstract class AbstractStreamOperator<OUT>
        implements StreamOperator<OUT>,
        SetupableStreamOperator<OUT>,
        CheckpointedStreamOperator,
        KeyContextHandler,
        Serializable {
    private static final long serialVersionUID = 1L;

    /** 当前运算符类及其子类使用的日志记录器。 */
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

    // ----------- 配置属性 -------------

    /**
     * 用于指定当前运算符与上下游算子的链接策略（默认是HEAD策略）。
     * HEAD 表示该运算符不会与前一个运算符链在一起，而是独立运行。
     */
    protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

    // ---------------- 运行时字段 ------------------

    /**
     * 包含当前运算符的 StreamTask（以及同一链中的其他运算符）。
     * StreamTask 负责管理运算符的生命周期和执行过程。
     */
    private transient StreamTask<?, ?> container;

    /** StreamTask 解析 JobGraph 任务配置信息后封装的 StreamConfig 对象。 */
    protected transient StreamConfig config;

    /**
     * 当前 StreamOperator 的输出组件。
     * 该组件用于在运算符执行完转换逻辑后，将数据写入下游算子。
     */
    protected transient Output<StreamRecord<OUT>> output;

    /** 维护输入水位线状态的组件，处理水印的传播。 */
    private transient IndexedCombinedWatermarkStatus combinedWatermark;

    /**
     * UDF（用户自定义函数）的运行时上下文。
     * 主要用于获取任务信息、累加器和状态数据。
     */
    private transient StreamingRuntimeContext runtimeContext;

    // ---------------- 键/值状态 ------------------

    /**
     * 用于从输入元素中提取 Key 的 KeySelector。
     * 该 KeySelector 用于对 Keyed State 进行作用域限定。
     * 如果运算符不是 Keyed 类型，则此字段为 null。
     *
     * 适用于第一输入流的 KeySelector。
     */
    private transient KeySelector<?, ?> stateKeySelector1;

    /**
     * 用于第二输入流的 KeySelector（仅适用于双输入算子）。
     */
    private transient KeySelector<?, ?> stateKeySelector2;

    /** 处理流运算符的状态管理，包括 Checkpoint 机制的集成。 */
    private transient StreamOperatorStateHandler stateHandler;

    /** 内部时间服务管理器，用于管理定时器的注册与触发。 */
    private transient InternalTimeServiceManager<?> timeServiceManager;

    // --------------- 监控指标 ---------------------------

    /** 该运算符的度量指标组。 */
    protected transient InternalOperatorMetricGroup metrics;

    /** 计算数据处理的延迟统计信息。 */
    protected transient LatencyStats latencyStats;

    // ---------------- 时间处理 ------------------

    /** 处理时间服务组件，负责触发定时任务（如定时器回调）。 */
    protected transient ProcessingTimeService processingTimeService;

    /** 记录第一输入流的最后一条记录的属性。 */
    protected transient RecordAttributes lastRecordAttributes1 =
            RecordAttributes.EMPTY_RECORD_ATTRIBUTES;

    /** 记录第二输入流的最后一条记录的属性（仅适用于双输入算子）。 */
    protected transient RecordAttributes lastRecordAttributes2 =
            RecordAttributes.EMPTY_RECORD_ATTRIBUTES;

    // ------------------------------------------------------------------------
    //  生命周期管理
    // ------------------------------------------------------------------------

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,  // 关联的流任务（StreamTask）
            StreamConfig config,  // 操作符的配置信息
            Output<StreamRecord<OUT>> output) {  // 输出数据流

        // 获取 StreamTask 的执行环境（Environment）
        final Environment environment = containingTask.getEnvironment();

        // 赋值当前任务容器
        this.container = containingTask;
        this.config = config;
        this.output = output;

        // 获取操作符的度量指标组（Metrics）
        this.metrics = environment
                .getMetricGroup()
                .getOrAddOperator(config.getOperatorID(), config.getOperatorName());

        // 初始化水位线状态管理，假设该算子有两个输入
        this.combinedWatermark = IndexedCombinedWatermarkStatus.forInputsCount(2);

        try {
            // 获取 TaskManager 级别的配置
            Configuration taskManagerConfig = environment.getTaskManagerInfo().getConfiguration();

            // 获取历史延迟指标的最大记录数
            int historySize = taskManagerConfig.get(MetricOptions.LATENCY_HISTORY_SIZE);
            if (historySize <= 0) {
                LOG.warn(
                        "{} 配置值为 {}，小于等于 0，使用默认值。",
                        MetricOptions.LATENCY_HISTORY_SIZE,
                        historySize);
                historySize = MetricOptions.LATENCY_HISTORY_SIZE.defaultValue();
            }

            // 解析延迟统计的粒度（granularity）
            final String configuredGranularity =
                    taskManagerConfig.get(MetricOptions.LATENCY_SOURCE_GRANULARITY);
            LatencyStats.Granularity granularity;
            try {
                // 解析字符串为 LatencyStats.Granularity 枚举值
                granularity = LatencyStats.Granularity.valueOf(configuredGranularity.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                // 如果解析失败，默认使用 OPERATOR 级别的统计粒度
                granularity = LatencyStats.Granularity.OPERATOR;
                LOG.warn(
                        "配置的 {} 值 {} 无效，使用默认值 {}。",
                        MetricOptions.LATENCY_SOURCE_GRANULARITY.key(),
                        configuredGranularity,
                        granularity);
            }

            // 获取当前作业（Job）的度量组
            MetricGroup jobMetricGroup = this.metrics.getJobMetricGroup();

            // 初始化延迟统计对象
            this.latencyStats = new LatencyStats(
                    jobMetricGroup.addGroup("latency"),
                    historySize,
                    container.getIndexInSubtaskGroup(),
                    getOperatorID(),
                    granularity);
        } catch (Exception e) {
            LOG.warn("初始化延迟度量指标时发生异常。", e);

            // 在异常情况下，使用默认的延迟统计配置
            this.latencyStats = new LatencyStats(
                    UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup()
                            .addGroup("latency"),
                    1,
                    0,
                    new OperatorID(),
                    LatencyStats.Granularity.SINGLE);
        }

        // 初始化运行时上下文
        this.runtimeContext = new StreamingRuntimeContext(
                environment,
                environment.getAccumulatorRegistry().getUserMap(), // 获取累加器（Accumulators）
                getMetricGroup(),
                getOperatorID(),
                getProcessingTimeService(),
                null,  // 这里可以传入 OperatorStateStore
                environment.getExternalResourceInfoProvider());

        // 获取 Keyed State 的分区器（State KeySelector）
        stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
        stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());
    }

    /**
     * @deprecated 该方法已被弃用，建议在算子构造函数中直接传入 {@link ProcessingTimeService}。
     */
    @Deprecated
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return metrics;
    }

    /**
     * 初始化 StreamOperator 的所有状态（State）
     */
    @Override
    public final void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {

        // 获取状态键的序列化器（State Key Serializer）
        final TypeSerializer<?> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());

        // 获取包含该操作的 StreamTask 实例，并进行非空检查
        final StreamTask<?, ?> containingTask = Preconditions.checkNotNull(getContainingTask());

        // 获取可关闭资源的注册器（CloseableRegistry），用于管理可关闭资源
        final CloseableRegistry streamTaskCloseableRegistry =
                Preconditions.checkNotNull(containingTask.getCancelables());

        // 创建 StreamOperatorStateContext，用于管理操作符状态的上下文信息
        final StreamOperatorStateContext context =
                streamTaskStateManager.streamOperatorStateContext(
                        getOperatorID(), // 操作符 ID
                        getClass().getSimpleName(), // 操作符的类名
                        getProcessingTimeService(), // 处理时间服务
                        this, // 当前操作符实例
                        keySerializer, // 状态键的序列化器
                        streamTaskCloseableRegistry, // 可关闭资源管理器
                        metrics, // 关联的度量指标
                        // 获取当前操作符在 Slot 级别的托管内存比例
                        config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.STATE_BACKEND,
                                runtimeContext.getJobConfiguration(),
                                runtimeContext.getTaskManagerRuntimeInfo().getConfiguration(),
                                runtimeContext.getUserCodeClassLoader()),
                        isUsingCustomRawKeyedState()); // 是否使用自定义原始 KeyedState

        // 创建 StreamOperatorStateHandler 进行状态管理
        stateHandler = new StreamOperatorStateHandler(
                context, getExecutionConfig(), streamTaskCloseableRegistry);

        // 获取内部时间服务管理器（InternalTimerServiceManager）
        timeServiceManager = context.internalTimerServiceManager();

        // 初始化操作符状态
        stateHandler.initializeOperatorState(this);

        // 如果 stateHandler 存在 KeyedStateStore，则设置到运行时上下文中，否则设置为 null
        runtimeContext.setKeyedStateStore(stateHandler.getKeyedStateStore().orElse(null));
    }


    /**
     * 指示该算子是否在进行快照时写入原始的 Keyed State 数据流。
     *
     * <p>如果子类需要写入原始的 Keyed State 数据流，应当重写该方法并返回 {@code true}。
     *
     * <p>子类需要显式声明是否使用原始的 Keyed State，因为 `AbstractStreamOperator`
     * 在恢复时可能会尝试从中读取 Heap-Based Timers（基于堆的定时器），
     * 如果这些数据不是定时器服务写入的，可能会导致读取失败。
     *
     * <p>通过设置该标志为 `true`，可以让 `AbstractStreamOperator` 知道原始的 Keyed State
     * 数据不是定时器服务写入的，从而跳过定时器的恢复。
     *
     * <p>参考 FLINK-19741 了解更多细节。
     *
     * <p>TODO: 当所有定时器都迁移到 StateBackend 管理后，该方法可以被移除。
     *
     * @return 是否使用了自定义的 Keyed State
     */
    @Internal
    protected boolean isUsingCustomRawKeyedState() {
        return false;
    }

    /**
     * `open()` 方法会在算子开始处理元素之前被调用，可用于初始化逻辑（如状态初始化）。
     *
     * <p>默认实现为空方法，子类可以重写该方法进行初始化操作。
     *
     * @throws Exception 如果发生异常，会导致算子启动失败。
     */
    @Override
    public void open() throws Exception {}

    /**
     * `finish()` 方法会在算子完成所有数据处理后被调用，表示算子即将终止。
     *
     * <p>默认实现为空方法，子类可以重写该方法来执行终止前的操作，例如刷新数据。
     *
     * @throws Exception 如果发生异常，会导致算子执行失败。
     */
    @Override
    public void finish() throws Exception {}

    /**
     * `close()` 方法在算子生命周期结束时调用，用于释放资源。
     *
     * <p>这里主要用于清理 `stateHandler`，防止资源泄漏。
     *
     * @throws Exception 如果关闭过程中出现异常，会记录日志但不会影响任务结束。
     */
    @Override
    public void close() throws Exception {
        if (stateHandler != null) {
            stateHandler.dispose();
        }
    }

    /**
     * 在检查点快照创建之前调用。
     *
     * <p>默认实现为空方法，子类可以重写该方法以在检查点前执行特定操作。
     *
     * @param checkpointId 进行中的检查点 ID
     * @throws Exception 如果发生异常，会导致任务失败。
     */
    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // 默认实现不执行任何操作，仅供子类重写
    }

    /**
     * 对当前算子进行状态快照，并将其持久化到外部存储（如 RocksDB）。
     *
     * @param checkpointId        检查点的唯一标识符
     * @param timestamp           检查点的时间戳
     * @param checkpointOptions   检查点的选项配置，定义了快照类型（如同步或异步）
     * @param factory             用于创建检查点流的工厂
     * @return                    返回 `OperatorSnapshotFutures`，用于跟踪快照状态和结果
     * @throws Exception          如果发生异常，会导致检查点失败
     */
    @Override
    public final OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        return stateHandler.snapshotState(
                this,  // 当前算子实例
                Optional.ofNullable(timeServiceManager),  // 时间服务管理器（如果存在）
                getOperatorName(),  // 获取算子名称
                checkpointId,  // 检查点 ID
                timestamp,  // 时间戳
                checkpointOptions,  // 检查点配置选项
                factory,  // 检查点数据流工厂
                isUsingCustomRawKeyedState());  // 是否使用了自定义的 Keyed State
    }

    /**
     * 如果算子有状态，并且希望参与检查点快照，则需要重写该方法。
     *
     * @param context 提供创建检查点快照所需的上下文信息
     * @throws Exception 如果发生异常，会导致检查点失败。
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {}

    /**
     * 如果算子有状态，并且需要支持状态恢复，则需要重写该方法。
     *
     * @param context 允许注册不同类型状态的上下文
     * @throws Exception 如果状态恢复失败，会导致任务失败。
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {}

    /**
     * 当检查点完成时会被回调，通知算子检查点已经成功。
     *
     * <p>此方法会调用 `stateHandler` 处理检查点完成的逻辑。
     *
     * @param checkpointId 已完成的检查点 ID
     * @throws Exception 如果处理过程中发生异常，可能会导致任务失败。
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        stateHandler.notifyCheckpointComplete(checkpointId);
    }

    /**
     * 当检查点被中止时会被回调，通知算子检查点失败。
     *
     * <p>此方法会调用 `stateHandler` 处理检查点中止的逻辑。
     *
     * @param checkpointId 被中止的检查点 ID
     * @throws Exception 如果处理过程中发生异常，可能会导致任务失败。
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        stateHandler.notifyCheckpointAborted(checkpointId);
    }


    // ------------------------------------------------------------------------
//  算子属性（Properties）和服务（Services）
// ------------------------------------------------------------------------

    /**
     * 获取该算子所属作业的执行配置（ExecutionConfig）。
     *
     * <p>执行配置包含全局作业设置，例如并行度、序列化方式等。它是 Flink 作业的核心配置信息之一。
     *
     * @return 作业的执行配置。
     */
    public ExecutionConfig getExecutionConfig() {
        return container.getExecutionConfig();
    }

    /**
     * 获取当前算子的流处理配置信息（StreamConfig）。
     *
     * @return 算子的 StreamConfig 对象。
     */
    public StreamConfig getOperatorConfig() {
        return config;
    }

    /**
     * 获取当前算子所属的 StreamTask（流任务）。
     *
     * <p>StreamTask 代表了 Flink 任务的基本执行单元，它包含了任务的生命周期管理、
     * 状态管理、算子调用等核心逻辑。
     *
     * @return 包含此算子的 StreamTask。
     */
    public StreamTask<?, ?> getContainingTask() {
        return container;
    }

    /**
     * 获取用户代码（UDF）类加载器。
     *
     * <p>Flink 允许用户提供自定义类加载器来加载 UDF（用户自定义函数），以支持动态加载 JAR 依赖。
     *
     * @return 用户代码类加载器。
     */
    public ClassLoader getUserCodeClassloader() {
        return container.getUserCodeClassLoader();
    }

    /**
     * 获取算子名称。
     *
     * <p>如果运行时上下文（RuntimeContext）已初始化，则返回任务名称（包含子任务索引）。
     * 否则，返回该算子的简单类名（Simple Class Name）。
     *
     * @return 任务名称（包含子任务索引），如果上下文未初始化，则返回类名。
     */
    protected String getOperatorName() {
        if (runtimeContext != null) {
            return runtimeContext.getTaskInfo().getTaskNameWithSubtasks();
        } else {
            return getClass().getSimpleName();
        }
    }

    /**
     * 获取运行时上下文（RuntimeContext）。
     *
     * <p>运行时上下文提供了 Flink 作业的执行信息，例如作业 ID、任务名称、并行度等。
     * 它还允许操作符与广播变量（Broadcast Variables）和托管状态（Managed State）交互，
     * 并且可以注册定时器。
     *
     * @return StreamingRuntimeContext 对象。
     */
    @VisibleForTesting
    public StreamingRuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    /**
     * 获取 Keyed State 后端（KeyedStateBackend）。
     *
     * <p>KeyedStateBackend 是 Flink 用于管理 Keyed State（键控状态）的核心组件。
     * 它支持不同的状态存储后端，如 HeapStateBackend 和 RocksDBStateBackend。
     *
     * @return KeyedStateBackend 实例。
     */
    public <K> KeyedStateBackend<K> getKeyedStateBackend() {
        return stateHandler.getKeyedStateBackend();
    }

    /**
     * 获取 Operator State 后端（OperatorStateBackend）。
     *
     * <p>OperatorStateBackend 用于管理无 Keyed 状态（Operator State）。
     * 它适用于不需要按键分区的状态，例如 ListState 和 UnionListState。
     *
     * @return OperatorStateBackend 实例。
     */
    @VisibleForTesting
    public OperatorStateBackend getOperatorStateBackend() {
        return stateHandler.getOperatorStateBackend();
    }

    /**
     * 获取当前任务的处理时间服务（ProcessingTimeService）。
     *
     * <p>ProcessingTimeService 提供了获取当前处理时间的方法，并允许注册定时器。
     * 这个组件对实现定时器相关逻辑的操作符（如 `ProcessFunction`）至关重要。
     *
     * @return ProcessingTimeService 实例。
     */
    @VisibleForTesting
    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }

    /**
     * 获取分区状态（Partitioned State）。
     *
     * <p>该方法使用当前任务配置的 StateBackend（状态后端）来创建一个键控状态（Keyed State）。
     * 如果 Keyed State 还未初始化，则会抛出 `IllegalStateException` 异常。
     *
     * @param stateDescriptor 状态描述符，定义状态的名称和序列化方式。
     * @param <S> 状态类型。
     * @return 分区状态实例。
     * @throws IllegalStateException 如果 Keyed State 已初始化，则抛出异常。
     * @throws Exception 如果状态后端无法创建 Keyed State，则抛出异常。
     */
    protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    /**
     * 获取或创建 Keyed State。
     *
     * <p>该方法会检查 Keyed State 是否已经存在，如果不存在，则创建新的 Keyed State。
     *
     * @param namespaceSerializer 命名空间的序列化器。
     * @param stateDescriptor 状态描述符，定义状态的名称和序列化方式。
     * @param <N> 命名空间类型。
     * @param <S> 状态类型。
     * @param <T> 状态存储值的类型。
     * @return Keyed State 实例。
     * @throws Exception 如果创建 Keyed State 失败，则抛出异常。
     */
    protected <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        return stateHandler.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
    }

    /**
     * 获取分区状态（Partitioned State）。
     *
     * <p>该方法使用当前任务的 StateBackend（状态后端）来创建一个带命名空间的 Keyed State。
     * 如果 Keyed State 已经初始化，则会抛出 `IllegalStateException` 异常。
     *
     * @param namespace 命名空间，指定状态作用范围。
     * @param namespaceSerializer 命名空间的序列化器。
     * @param stateDescriptor 状态描述符，定义状态的名称和序列化方式。
     * @param <S> 状态类型。
     * @param <N> 命名空间类型。
     * @return 分区状态实例。
     * @throws IllegalStateException 如果 Keyed State 已初始化，则抛出异常。
     * @throws Exception 如果状态后端无法创建 Keyed State，则抛出异常。
     */
    protected <S extends State, N> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return stateHandler.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
    }

    /**
     * 设置当前算子的 Keyed Context（第一组）。
     *
     * <p>Keyed Context 影响算子状态的作用范围，使算子可以按 Key 访问状态。
     *
     * @param record 处理的流数据记录。
     * @throws Exception 如果设置失败，则抛出异常。
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setKeyContextElement1(StreamRecord record) throws Exception {
        setKeyContextElement(record, stateKeySelector1);
    }

    /**
     * 设置当前算子的 Keyed Context（第二组）。
     *
     * @param record 处理的流数据记录。
     * @throws Exception 如果设置失败，则抛出异常。
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setKeyContextElement2(StreamRecord record) throws Exception {
        setKeyContextElement(record, stateKeySelector2);
    }

    /**
     * 检查是否有 Keyed Context（第一组）。
     *
     * @return 如果 stateKeySelector1 不为空，则返回 true。
     */
    @Internal
    @Override
    public boolean hasKeyContext1() {
        return stateKeySelector1 != null;
    }

    /**
     * 检查是否有 Keyed Context（第二组）。
     *
     * @return 如果 stateKeySelector2 不为空，则返回 true。
     */
    @Internal
    @Override
    public boolean hasKeyContext2() {
        return stateKeySelector2 != null;
    }


    // ------------------------- 键控状态上下文管理 -------------------------

    /**
     * 设置当前处理记录的键控上下文
     * @param record 输入记录（包含值和时间戳）
     * @param selector 键选择器，用于提取记录键
     * @param <T> 记录值类型
     *
     * 实现机制：
     * - 通过KeySelector提取记录的业务键
     * - 将键设置到运行时上下文中，用于后续状态操作
     * 注意事项：
     * - 必须在状态访问前调用
     * - 影响KeyedStateStore的操作范围
     */
    private <T> void setKeyContextElement(StreamRecord<T> record, KeySelector<T, ?> selector)
            throws Exception {
        if (selector != null) {
            Object key = selector.getKey(record.getValue());  // 提取业务逻辑键
            setCurrentKey(key);  // 更新线程本地键存储
        }
    }

    /**
     * 设置当前处理键（线程本地存储）
     * @param key 业务键对象（需实现正确hashCode和equals）
     */
    public void setCurrentKey(Object key) {
        stateHandler.setCurrentKey(key);  // 委托给状态处理器
    }

    public Object getCurrentKey() {
        return stateHandler.getCurrentKey();
    }

// ------------------------- 状态存储访问 -------------------------

    /**
     * 获取当前键控状态存储
     * @return 可能为null（未初始化时）
     *
     * 典型应用场景：
     * - 访问KeyedState（ValueState/ListState等）
     * - 注册状态有效期TTL
     */
    public KeyedStateStore getKeyedStateStore() {
        return stateHandler != null
                ? stateHandler.getKeyedStateStore().orElse(null)
                : null;
    }

// ------------------------- 链式策略配置 -------------------------

    /**
     * 设置操作符链式策略
     * @param strategy 链式策略枚举（HEAD/ALWAYS/NEVER等）
     *
     * 策略说明：
     * - HEAD：只能作为链头节点
     * - ALWAYS：允许前后链接（默认）
     * - NEVER：禁止链式连接
     */
    @Override
    public final void setChainingStrategy(ChainingStrategy strategy) {
        this.chainingStrategy = strategy;
    }

    @Override
    public final ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

// ------------------------- 延迟指标处理 -------------------------

    /**
     * 处理单输入流的延迟标记（默认实现）
     * @param latencyMarker 延迟跟踪标记
     */
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        reportOrForwardLatencyMarker(latencyMarker);
    }

    // ------- Two input stream
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        reportOrForwardLatencyMarker(latencyMarker);
    }

    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        reportOrForwardLatencyMarker(latencyMarker);
    }

    /**
     * 统一延迟处理逻辑：
     * 1. 记录本地延迟统计
     * 2. 向下游转发标记（sink节点除外）
     */
    protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
        latencyStats.reportLatency(marker);   // 更新本地延迟直方图
        output.emitLatencyMarker(marker);     // 传播至下游操作符
    }

// ------------------------- 定时器服务管理 -------------------------

    /**
     * Returns a {@link InternalTimerService} that can be used to query current processing time and
     * event time and to set timers. An operator can have several timer services, where each has its
     * own namespace serializer. Timer services are differentiated by the string key that is given
     * when requesting them, if you call this method with the same key multiple times you will get
     * the same timer service instance in subsequent requests.
     *
     * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
     * When a timer fires, this key will also be set as the currently active key.
     *
     * <p>Each timer has attached metadata, the namespace. Different timer services can have a
     * different namespace type. If you don't need namespace differentiation you can use {@link
     * VoidNamespaceSerializer} as the namespace serializer.
     *
     * @param name The name of the requested timer service. If no service exists under the given
     *     name a new one will be created and returned.
     * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
     * @param triggerable The {@link Triggerable} that should be invoked when timers fire
     * @param <N> The type of the timer namespace.
     */
    public <K, N> InternalTimerService<N> getInternalTimerService(
            String name, TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerable) {
        if (timeServiceManager == null) {
            throw new RuntimeException("The timer service has not been initialized.");
        }
        @SuppressWarnings("unchecked")
        InternalTimeServiceManager<K> keyedTimeServiceHandler =
                (InternalTimeServiceManager<K>) timeServiceManager;
        KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();
        checkState(keyedStateBackend != null, "Timers can only be used on keyed operators.");
        return keyedTimeServiceHandler.getInternalTimerService(
                name, keyedStateBackend.getKeySerializer(), namespaceSerializer, triggerable);
    }

    public void processWatermark(Watermark mark) throws Exception {
        if (timeServiceManager != null) {
            timeServiceManager.advanceWatermark(mark);
        }
        output.emitWatermark(mark);
    }

    /**
     * 组合水印处理核心逻辑：
     * 1. 更新对应输入通道的水印状态
     * 2. 计算全局最小水印并传播
     * 3. 处理空闲状态转换
     *
     * @param index 输入流索引（0-based）
     * @param mark 当前水印值
     */
    private void processWatermark(Watermark mark, int index) throws Exception {
        // 更新组合水印状态
        if (combinedWatermark.updateWatermark(index, mark.getTimestamp())) {
            // 当组合水印前进时，触发全局处理
            processWatermark(new Watermark(combinedWatermark.getCombinedWatermark()));
        }
    }

    public void processWatermark1(Watermark mark) throws Exception {
        processWatermark(mark, 0);
    }

    public void processWatermark2(Watermark mark) throws Exception {
        processWatermark(mark, 1);
    }

    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        output.emitWatermarkStatus(watermarkStatus);
    }

    private void processWatermarkStatus(WatermarkStatus watermarkStatus, int index)
            throws Exception {
        boolean wasIdle = combinedWatermark.isIdle();
        if (combinedWatermark.updateStatus(index, watermarkStatus.isIdle())) {
            processWatermark(new Watermark(combinedWatermark.getCombinedWatermark()));
        }
        if (wasIdle != combinedWatermark.isIdle()) {
            output.emitWatermarkStatus(watermarkStatus);
        }
    }

    public final void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        processWatermarkStatus(watermarkStatus, 0);
    }

    public final void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        processWatermarkStatus(watermarkStatus, 1);
    }

    @Override
    public OperatorID getOperatorID() {
        return config.getOperatorID();
    }

    protected Optional<InternalTimeServiceManager<?>> getTimeServiceManager() {
        return Optional.ofNullable(timeServiceManager);
    }

    @Experimental
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        output.emitRecordAttributes(
                new RecordAttributesBuilder(Collections.singletonList(recordAttributes)).build());
    }

    /**
     * 处理记录属性（实验性API）
     * @param recordAttributes 包含数据特征描述（如数据倾斜标记）
     *
     * 当前实现：
     * - 收集所有输入通道的最新属性
     * - 组合后向下游广播
     */
    @Experimental
    public void processRecordAttributes1(RecordAttributes recordAttributes) {
        lastRecordAttributes1 = recordAttributes;  // 更新通道1属性缓存
        output.emitRecordAttributes(
                new RecordAttributesBuilder(
                        Arrays.asList(lastRecordAttributes1, lastRecordAttributes2)
                ).build()  // 组合双流属性
        );
    }

}
