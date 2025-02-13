/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.DynamicFilteringInfo;
import org.apache.flink.api.connector.source.DynamicParallelismInference;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SupportsHandleExecutionAttemptSourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.IsProcessingBacklogEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.AbstractHeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readAndVerifyCoordinatorSerdeVersion;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readBytes;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.writeCoordinatorSerdeVersion;
import static org.apache.flink.util.IOUtils.closeQuietly;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link Source} 的默认 {@link OperatorCoordinator} 实现。
 *
 * <p>SourceCoordinator 提供了一种基于事件循环的线程模型，与 Flink 运行时进行交互。
 * 该协调器确保所有状态操作都由其事件循环线程执行。它还负责跟踪每个子任务的拆分（Split）分配历史，
 * 以简化 {@link SplitEnumerator} 的实现。
 *
 * <p>协调器维护一个 {@link org.apache.flink.api.connector.source.SplitEnumeratorContext}，
 * 并与枚举器（Enumerator）共享。当协调器从 Flink 运行时接收到操作请求时，
 * 它会设置上下文，并调用 SplitEnumerator 的相应方法来执行操作。
 */
@Internal
public class SourceCoordinator<SplitT extends SourceSplit, EnumChkT>
        implements OperatorCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinator.class);

    // 用于存储全局水位线（Watermark）信息的聚合器
    private final WatermarkAggregator<Integer> combinedWatermark = new WatermarkAggregator<>();

    // 水位线对齐参数
    private final WatermarkAlignmentParams watermarkAlignmentParams;

    /** 此 SourceCoordinator 关联的算子名称 */
    private final String operatorName;
    /** 该 SourceCoordinator 关联的数据源 */
    private final Source<?, SplitT, EnumChkT> source;
    /** 处理 SplitEnumerator 检查点的序列化与反序列化（serde）对象 */
    private final SimpleVersionedSerializer<EnumChkT> enumCheckpointSerializer;
    /** 存储协调器的状态上下文 */
    private final SourceCoordinatorContext<SplitT> context;

    private final CoordinatorStore coordinatorStore;

    /**
     * 由关联的 Source 创建的 SplitEnumerator ？？？ 此对象会在恢复检查点或启动协调器时创建。
     */
    private SplitEnumerator<SplitT, EnumChkT> enumerator;

    /** 标识协调器是否已启动 */
    private boolean started;

    /**
     * 协调器的监听 ID，用于在协调器存储中注册自己。
     * 其他协调器可以通过此 ID 向当前协调器发送事件。
     */
    @Nullable private final String coordinatorListeningID;

    /**
     * 构造 SourceCoordinator 对象（不包含水位线对齐配置）
     */
    public SourceCoordinator(
            String operatorName,
            Source<?, SplitT, EnumChkT> source,
            SourceCoordinatorContext<SplitT> context,
            CoordinatorStore coordinatorStore) {
        this(
                operatorName,
                source,
                context,
                coordinatorStore,
                WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED,
                null);
    }

    /**
     * 构造 SourceCoordinator 对象，允许指定水位线对齐参数
     */
    public SourceCoordinator(
            String operatorName,
            Source<?, SplitT, EnumChkT> source,
            SourceCoordinatorContext<SplitT> context,
            CoordinatorStore coordinatorStore,
            WatermarkAlignmentParams watermarkAlignmentParams,
            @Nullable String coordinatorListeningID) {
        this.operatorName = operatorName;
        this.source = source;
        this.enumCheckpointSerializer = source.getEnumeratorCheckpointSerializer();
        this.context = context;
        this.coordinatorStore = coordinatorStore;
        this.watermarkAlignmentParams = watermarkAlignmentParams;
        this.coordinatorListeningID = coordinatorListeningID;

        // 当启用水位线对齐时，不允许并发执行尝试（例如启用了推测执行）
        if (watermarkAlignmentParams.isEnabled()
                && context.isConcurrentExecutionAttemptsSupported()) {
            throw new IllegalArgumentException(
                    "水位线对齐不支持并发执行尝试（例如启用了推测执行）");
        }
    }

    /**
     * 计算并发布全局合并水位线（Combined Watermark）。
     * 该方法会获取当前任务的水位线数据，计算最小水位线并分发给各个子任务。
     */
    @VisibleForTesting
    void announceCombinedWatermark() {
        checkState(
                watermarkAlignmentParams != WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED);

        // 计算全局合并水位线
        Watermark globalCombinedWatermark =
                coordinatorStore.apply(
                        watermarkAlignmentParams.getWatermarkGroup(),
                        (value) -> {
                            WatermarkAggregator aggregator = (WatermarkAggregator) value;
                            return new Watermark(
                                    aggregator.getAggregatedWatermark().getTimestamp());
                        });

        long maxAllowedWatermark;
        try {
            // 计算允许的最大水位线（即当前水位线 + 允许的最大偏差）
            maxAllowedWatermark =
                    Math.addExact(
                            globalCombinedWatermark.getTimestamp(),
                            watermarkAlignmentParams.getMaxAllowedWatermarkDrift());
        } catch (ArithmeticException e) {
            // 当 source 处于空闲状态时，globalCombinedWatermark.getTimestamp() 可能为 Long.MAX_VALUE，
            // 计算 maxAllowedWatermark 可能会发生溢出，因此需要特殊处理
            maxAllowedWatermark = Watermark.MAX_WATERMARK.getTimestamp();
        }

        Set<Integer> subTaskIds = combinedWatermark.keySet();
        LOG.info(
                "为源 {} 分发最大允许水位线 maxAllowedWatermark={}，属于组 {}，涉及子任务 {}。",
                operatorName,
                maxAllowedWatermark,
                watermarkAlignmentParams.getWatermarkGroup(),
                subTaskIds);

        // 由于子任务可能处于部署或重启过程中，因此仅向已准备就绪的任务发送水位线对齐事件
        for (Integer subtaskId : subTaskIds) {
            context.sendEventToSourceOperatorIfTaskReady(
                    subtaskId, new WatermarkAlignmentEvent(maxAllowedWatermark));
        }
    }

    @Override
    public void start() throws Exception {
        LOG.info("启动源 {} 的 SplitEnumerator。", operatorName);

        // 标记 SourceCoordinator 已启动，这样后续可以区分
        // 1) `start()` 方法未被调用
        // 2) `start()` 方法调用失败
        started = true;

        /**
         * SplitEnumerator 的创建方式有两种：
         * (1) 通过 Source.restoreEnumerator() 进行恢复，在 `resetToCheckpoint()` 方法中创建
         * (2) 通过 Source.createEnumerator() 进行创建，此时它还未被创建，需要在此方法中创建
         */
        if (enumerator == null) {
            final ClassLoader userCodeClassLoader =
                    context.getCoordinatorContext().getUserCodeClassloader();
            try (TemporaryClassLoaderContext ignored =
                         TemporaryClassLoaderContext.of(userCodeClassLoader)) {
                enumerator = source.createEnumerator(context);
            } catch (Throwable t) {
                // 重新抛出致命错误或 OOM（OutOfMemoryError）
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                LOG.error("为源 {} 创建 SplitEnumerator 失败。", operatorName, t);
                context.failJob(t);
                return;
            }
        }

        // 调用 `runInEventLoop` 确保 `enumerator.start()` 在协调器的单线程事件循环中执行
        runInEventLoop(() -> enumerator.start(), "启动 SplitEnumerator。");

        // 处理协调器监听 ID（coordinatorListeningID），确保它能够正确注册事件
        if (coordinatorListeningID != null) {
            coordinatorStore.compute(
                    coordinatorListeningID,
                    (key, oldValue) -> {
                        // 监听 ID 可能是 SourceCoordinator 监听的事件，也可能是未处理的事件
                        if (oldValue == null || oldValue instanceof OperatorCoordinator) {
                            // 若协调器未注册或在全局失败恢复后需要重新创建，则注册当前实例
                            return this;
                        } else {
                            // 确保旧值是一个 `OperatorEvent`
                            checkState(
                                    oldValue instanceof OperatorEvent,
                                    "协调器存储中已有的值应该是一个 OperatorEvent，实际是：" + oldValue);

                            LOG.info(
                                    "在注册源协调器 ID {} 之前收到事件 {}，正在处理。",
                                    coordinatorListeningID,
                                    oldValue);
                            handleEventFromOperator(0, 0, (OperatorEvent) oldValue);

                            // 在非全局失败的情况下，协调器不会被重新创建；
                            // 在全局失败的情况下，发送方和接收方都会重启，
                            // 事件只会收到一次，处理完后即可安全删除，无需重新注册协调器
                            return null;
                        }
                    });
        }

        // 若启用了水位线对齐，则启动定期广播水位线的任务
        if (watermarkAlignmentParams.isEnabled()) {
            LOG.info("启动周期性水位线广播任务。");
            coordinatorStore.putIfAbsent(
                    watermarkAlignmentParams.getWatermarkGroup(), new WatermarkAggregator<>());
            context.schedulePeriodTask(
                    this::announceCombinedWatermark,
                    watermarkAlignmentParams.getUpdateInterval(),
                    watermarkAlignmentParams.getUpdateInterval(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("关闭源协调器 {}。", operatorName);
        if (started) {
            closeQuietly(enumerator);
        }
        closeQuietly(context);
        LOG.info("源协调器 {} 已关闭。", operatorName);
    }

    /**
     * 处理来自 Source Operator（任务执行算子）的事件
     */
    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    // 处理请求数据分片的事件
                    if (event instanceof RequestSplitEvent) {
                        handleRequestSplitEvent(subtask, attemptNumber, (RequestSplitEvent) event);
                    }
                    // 处理自定义 Source 事件
                    else if (event instanceof SourceEventWrapper) {
                        handleSourceEvent(
                                subtask,
                                attemptNumber,
                                ((SourceEventWrapper) event).getSourceEvent());
                    }
                    // 处理 Source Reader 注册事件
                    else if (event instanceof ReaderRegistrationEvent) {
                        handleReaderRegistrationEvent(
                                subtask, attemptNumber, (ReaderRegistrationEvent) event);
                    }
                    // 处理子任务报告的水位线
                    else if (event instanceof ReportedWatermarkEvent) {
                        handleReportedWatermark(
                                subtask,
                                new Watermark(((ReportedWatermarkEvent) event).getWatermark()));
                    }
                    // 未识别的事件，抛出异常
                    else {
                        throw new FlinkException("无法识别的算子事件：" + event);
                    }
                },
                "处理来自子任务 %d（#%d）的算子事件 %s",
                subtask,
                attemptNumber,
                event);
    }

    /**
     * 处理子任务执行尝试失败的情况
     */
    @Override
    public void executionAttemptFailed(
            int subtaskId, int attemptNumber, @Nullable Throwable reason) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "子任务 {}（#{}）执行失败，移除已注册的 Reader，所属源 {}。",
                            subtaskId,
                            attemptNumber,
                            operatorName);

                    // 取消注册已失败的 Source Reader
                    context.unregisterSourceReader(subtaskId, attemptNumber);
                    // 记录失败信息
                    context.attemptFailed(subtaskId, attemptNumber);
                },
                "处理子任务 %d（#%d）失败",
                subtaskId,
                attemptNumber);
    }

    /**
     * 处理子任务恢复到检查点
     */
    @Override
    public void subtaskReset(int subtaskId, long checkpointId) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "恢复子任务 {} 到检查点 {}，所属源 {}。",
                            subtaskId,
                            checkpointId,
                            operatorName);

                    // 让上下文处理子任务重置
                    context.subtaskReset(subtaskId);

                    // 获取并删除未持久化的分片分配
                    final List<SplitT> splitsToAddBack =
                            context.getAndRemoveUncheckpointedAssignment(subtaskId, checkpointId);
                    LOG.debug(
                            "将以下拆分重新添加到源 {} 的 SplitEnumerator：{}",
                            operatorName,
                            splitsToAddBack);
                    // 让 SplitEnumerator 重新分配未提交的 Split
                    enumerator.addSplitsBack(splitsToAddBack, subtaskId);
                },
                "处理子任务 %d 恢复到检查点 %d",
                subtaskId,
                checkpointId);
    }


    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        // 确保子任务 ID 与 gateway 中的 ID 匹配
        checkArgument(subtask == gateway.getSubtask());
        // 确保执行尝试号（attemptNumber）与 gateway 中的执行号匹配
        checkArgument(attemptNumber == gateway.getExecution().getAttemptNumber());

        // 在事件循环中运行，使得子任务的事件网关可用
        runInEventLoop(
                () -> context.attemptReady(gateway),
                "使子任务 %d（#%d）的事件网关可用",
                subtask,
                attemptNumber);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        // 在事件循环中运行检查点逻辑
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "在算子 {} 上为检查点 {} 进行状态快照。",
                            operatorName,
                            checkpointId);
                    try {
                        // 通知上下文执行检查点操作
                        context.onCheckpoint(checkpointId);
                        // 序列化检查点数据，并将结果存入 CompletableFuture
                        result.complete(toBytes(checkpointId));
                    } catch (Throwable e) {
                        // 重新抛出致命错误或 OOM
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                        result.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "为源 %s 的 SplitEnumerator 进行检查点失败",
                                                operatorName),
                                        e));
                    }
                },
                "进行检查点 %d",
                checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // 在事件循环中执行检查点完成通知
        runInEventLoop(
                () -> {
                    LOG.info(
                            "将检查点 {} 标记为已完成，所属源 {}。",
                            checkpointId,
                            operatorName);
                    // 通知上下文检查点完成
                    context.onCheckpointComplete(checkpointId);
                    // 通知 SplitEnumerator 检查点完成
                    enumerator.notifyCheckpointComplete(checkpointId);
                },
                "通知枚举器检查点 %d 完成",
                checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // 在事件循环中执行检查点终止通知
        runInEventLoop(
                () -> {
                    LOG.info(
                            "将检查点 {} 标记为已终止，所属源 {}。",
                            checkpointId,
                            operatorName);
                    // 通知 SplitEnumerator 检查点已终止
                    enumerator.notifyCheckpointAborted(checkpointId);
                },
                "调用 notifyCheckpointAborted()");
    }

    @Override
    public void resetToCheckpoint(final long checkpointId, @Nullable final byte[] checkpointData)
            throws Exception {

        // 确保协调器未启动，否则无法重置
        checkState(!started, "只能在协调器未启动时执行检查点重置");
        // 确保 SplitEnumerator 为空
        assert enumerator == null;

        // 如果检查点数据为空，则表示没有已完成的检查点
        // 此时不执行恢复操作，直接等待 `start()` 创建新的 SplitEnumerator
        if (checkpointData == null) {
            return;
        }

        LOG.info("从检查点恢复源 {} 的 SplitEnumerator。", operatorName);

        // 获取用户代码类加载器
        final ClassLoader userCodeClassLoader =
                context.getCoordinatorContext().getUserCodeClassloader();
        try (TemporaryClassLoaderContext ignored =
                     TemporaryClassLoaderContext.of(userCodeClassLoader)) {
            // 反序列化检查点数据，恢复 SplitEnumerator
            final EnumChkT enumeratorCheckpoint = deserializeCheckpoint(checkpointData);
            enumerator = source.restoreEnumerator(context, enumeratorCheckpoint);
        }
    }

    /**
     * 在事件循环中执行指定的操作。
     *
     * @param action 要执行的操作
     * @param actionName 操作的描述
     * @param actionNameFormatParameters 操作描述的格式化参数
     */
    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {

        // 确保协调器已启动
        ensureStarted();

        // 可能会遇到未启动的 enumerator（例如实例化失败或发生 failover 时），需要忽略这些情况
        if (enumerator == null) {
            return;
        }

        // 在协调器线程中运行
        context.runInCoordinatorThread(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        // 若遇到 JVM 致命错误或 OOM，则立即重新抛出
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        // 格式化操作名称，记录错误日志
                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "在 Source {} 的 SplitEnumerator 中执行 {} 时发生未捕获的异常，触发作业失败。",
                                operatorName,
                                actionString,
                                t);
                        // 触发作业失败
                        context.failJob(t);
                    }
                });
    }

    /**
     * 获取 SplitEnumerator 实例（仅用于测试）。
     */
    @VisibleForTesting
    SplitEnumerator<SplitT, EnumChkT> getEnumerator() {
        return enumerator;
    }

    /**
     * 获取 SourceCoordinatorContext 实例（仅用于测试）。
     */
    @VisibleForTesting
    SourceCoordinatorContext<SplitT> getContext() {
        return context;
    }


    // --------------------- 序列化（Serde） -----------------------

    /**
     * 将 SourceCoordinator 的状态序列化成字节数组。
     *
     * 当前实现可能不是最优的，但考虑到大多数状态数据较小，因此影响不大。
     * 若状态数据本身较大，可能会导致额外的存储与传输问题，而不仅仅是序列化效率问题。
     *
     * @return 包含 SourceCoordinator 状态的字节数组
     * @throws Exception 如果序列化过程中发生错误
     */
    private byte[] toBytes(long checkpointId) throws Exception {
        return writeCheckpointBytes(
                enumerator.snapshotState(checkpointId), enumCheckpointSerializer);
    }

    /**
     * 将 SplitEnumerator 的检查点数据序列化为字节数组。
     *
     * @param enumeratorCheckpoint 枚举器的检查点状态
     * @param enumeratorCheckpointSerializer 用于序列化的版本化序列化器
     * @return 序列化后的字节数组
     * @throws Exception 如果序列化过程中发生错误
     */
    static <EnumChkT> byte[] writeCheckpointBytes(
            final EnumChkT enumeratorCheckpoint,
            final SimpleVersionedSerializer<EnumChkT> enumeratorCheckpointSerializer)
            throws Exception {

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {

            // 写入协调器的序列化版本号
            writeCoordinatorSerdeVersion(out);
            // 写入枚举器检查点序列化器的版本号
            out.writeInt(enumeratorCheckpointSerializer.getVersion());

            // 对枚举器检查点数据进行序列化
            byte[] serializedEnumChkpt = enumeratorCheckpointSerializer.serialize(enumeratorCheckpoint);
            out.writeInt(serializedEnumChkpt.length); // 先写入数据长度
            out.write(serializedEnumChkpt);           // 写入实际的序列化数据
            out.flush(); // 确保数据完整写入
            return baos.toByteArray();
        }
    }

    /**
     * 从序列化数据中恢复 SourceCoordinator 的状态。
     *
     * @param bytes 之前 {@link #toBytes(long)} 返回的检查点字节数据
     * @return 反序列化得到的 SplitEnumerator 检查点数据
     * @throws Exception 如果反序列化失败
     */
    private EnumChkT deserializeCheckpoint(byte[] bytes) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream in = new DataInputViewStreamWrapper(bais)) {

            // 读取并验证协调器的序列化版本
            final int coordinatorSerdeVersion = readAndVerifyCoordinatorSerdeVersion(in);

            // 读取 SplitEnumerator 的序列化版本
            int enumSerializerVersion = in.readInt();
            // 读取序列化数据的大小
            int serializedEnumChkptSize = in.readInt();
            // 读取序列化数据
            byte[] serializedEnumChkpt = readBytes(in, serializedEnumChkptSize);

            // 确保没有多余的字节数据（防止数据损坏）
            if (coordinatorSerdeVersion != SourceCoordinatorSerdeUtils.VERSION_0
                    && bais.available() > 0) {
                throw new IOException("在枚举器检查点数据中发现多余的字节，可能数据损坏");
            }

            // 反序列化枚举器的检查点数据
            return enumCheckpointSerializer.deserialize(enumSerializerVersion, serializedEnumChkpt);
        }
    }

// --------------------- 事件处理（Event Handling） -------------

    /**
     * 处理子任务的拆分（Split）请求事件
     *
     * @param subtask 子任务 ID
     * @param attemptNumber 尝试编号
     * @param event 请求拆分的事件
     */
    private void handleRequestSplitEvent(int subtask, int attemptNumber, RequestSplitEvent event) {
        LOG.info(
                "源 {} 收到并行任务 {}（#{}）的拆分请求。",
                operatorName,
                subtask,
                attemptNumber);

        // 仅当枚举器（Enumerator）仍有未分配的拆分（Split）时才请求拆分
        // 这样可以减少不必要的拆分请求，提高效率
        if (!context.hasNoMoreSplits(subtask)) {
            enumerator.handleSplitRequest(subtask, event.hostName());
        }
    }

    /**
     * 处理来自 Source Operator 的自定义事件。
     *
     * @param subtask 子任务 ID
     * @param attemptNumber 尝试编号
     * @param event 自定义 Source 事件
     */
    private void handleSourceEvent(int subtask, int attemptNumber, SourceEvent event) {
        LOG.debug(
                "源 {} 收到来自并行任务 {}（#{}）的自定义事件：{}",
                operatorName,
                subtask,
                attemptNumber,
                event);

        // 检查是否支持并发执行尝试（speculative execution）
        if (context.isConcurrentExecutionAttemptsSupported()) {
            checkState(
                    enumerator instanceof SupportsHandleExecutionAttemptSourceEvent,
                    "SplitEnumerator %s 必须实现 SupportsHandleExecutionAttemptSourceEvent 接口，"
                            + "才能用于并发执行尝试（例如启用了推测执行）。",
                    enumerator.getClass().getCanonicalName());

            // 以支持执行尝试的方式处理事件
            ((SupportsHandleExecutionAttemptSourceEvent) enumerator)
                    .handleSourceEvent(subtask, attemptNumber, event);
        } else {
            // 直接处理事件
            enumerator.handleSourceEvent(subtask, event);
        }
    }

    /**
     * 处理 Source Reader（数据读取器）的注册事件。
     *
     * @param subtask 子任务 ID
     * @param attemptNumber 尝试编号
     * @param event 读取器注册事件
     */
    private void handleReaderRegistrationEvent(
            int subtask, int attemptNumber, ReaderRegistrationEvent event) {
        checkArgument(subtask == event.subtaskId());

        LOG.info(
                "源 {} 注册来自并行任务 {}（#{}）的 Reader，位置 @ {}",
                operatorName,
                subtask,
                attemptNumber,
                event.location());

        // 检查子任务的读取器是否已存在
        final boolean subtaskReaderExisted =
                context.registeredReadersOfAttempts().containsKey(subtask);
        // 在上下文中注册读取器
        context.registerSourceReader(subtask, attemptNumber, event.location());

        // 如果该子任务的 Reader 之前未注册，则需要让 SplitEnumerator 知道
        if (!subtaskReaderExisted) {
            enumerator.addReader(event.subtaskId());

            // 检查当前是否在处理 backlog（积压数据）
            final Boolean isBacklog = context.isBacklog().getAsBoolean();
            if (isBacklog != null) {
                // 如果 backlog 存在，则通知 Source Operator
                context.sendEventToSourceOperatorIfTaskReady(
                        subtask, new IsProcessingBacklogEvent(isBacklog));
            }
        }
    }


    /**
     * 处理子任务报告的水位线（Watermark）。
     *
     * @param subtask 子任务 ID
     * @param watermark 子任务报告的水位线
     * @throws FlinkException 如果当前上下文不支持水位线报告
     */
    private void handleReportedWatermark(int subtask, Watermark watermark) throws FlinkException {
        // 如果支持并发执行尝试（推测执行），则不支持处理水位线报告
        if (context.isConcurrentExecutionAttemptsSupported()) {
            throw new FlinkException(
                    "在支持并发执行尝试（例如推测执行）的情况下，不支持 ReportedWatermarkEvent");
        }

        LOG.debug(
                "子任务 {} 报告新的水位线={}，所属源 {}。",
                subtask,
                watermark,
                operatorName);

        // 确保水位线对齐参数已启用
        checkState(watermarkAlignmentParams.isEnabled());

        // 更新当前子任务的水位线，并聚合全局水位线
        combinedWatermark
                .aggregate(subtask, watermark)
                .ifPresent(
                        newCombinedWatermark ->
                                coordinatorStore.computeIfPresent(
                                        watermarkAlignmentParams.getWatermarkGroup(),
                                        (key, oldValue) -> {
                                            // 通过 WatermarkAggregator 聚合全局水位线
                                            WatermarkAggregator<String> watermarkAggregator =
                                                    (WatermarkAggregator<String>) oldValue;
                                            watermarkAggregator.aggregate(
                                                    operatorName, newCombinedWatermark);
                                            return watermarkAggregator;
                                        }));
    }

    /**
     * 确保 SourceCoordinator 已启动，否则抛出异常。
     */
    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("协调器尚未启动。");
        }
    }

    /**
     * 获取动态过滤信息（Dynamic Filtering Info）。
     *
     * @return 包含动态过滤信息的 Optional 对象
     */
    private Optional<DynamicFilteringInfo> getSourceDynamicFilteringInfo() {
        // 如果协调器有监听 ID 并且存储中包含该 ID，则尝试获取对应的事件
        if (coordinatorListeningID != null
                && coordinatorStore.containsKey(coordinatorListeningID)) {
            Object event = coordinatorStore.get(coordinatorListeningID);
            if (event instanceof SourceEventWrapper) {
                SourceEvent sourceEvent = ((SourceEventWrapper) event).getSourceEvent();
                if (sourceEvent instanceof DynamicFilteringInfo) {
                    return Optional.of((DynamicFilteringInfo) sourceEvent);
                }
            }
        }

        return Optional.empty();
    }

    /**
     * 异步推理数据源的最优并行度。
     *
     * @param parallelismInferenceUpperBound 并行度推理的上限
     * @param dataVolumePerTask 每个任务的数据量
     * @return 包含推理出的并行度的 CompletableFuture
     */
    public CompletableFuture<Integer> inferSourceParallelismAsync(
            int parallelismInferenceUpperBound, long dataVolumePerTask) {
        return context.supplyAsync(
                        () -> {
                            // 如果数据源不支持动态并行度推理，则返回默认并行度
                            if (!(source instanceof DynamicParallelismInference)) {
                                return ExecutionConfig.PARALLELISM_DEFAULT;
                            }

                            DynamicParallelismInference parallelismInference =
                                    (DynamicParallelismInference) source;
                            try {
                                // 计算最优并行度
                                return parallelismInference.inferParallelism(
                                        new DynamicParallelismInference.Context() {
                                            @Override
                                            public Optional<DynamicFilteringInfo>
                                            getDynamicFilteringInfo() {
                                                return getSourceDynamicFilteringInfo();
                                            }

                                            @Override
                                            public int getParallelismInferenceUpperBound() {
                                                return parallelismInferenceUpperBound;
                                            }

                                            @Override
                                            public long getDataVolumePerTask() {
                                                return dataVolumePerTask;
                                            }
                                        });
                            } catch (Throwable e) {
                                LOG.error(
                                        "推理数据源并行度时发生异常。",
                                        e);
                                return ExecutionConfig.PARALLELISM_DEFAULT;
                            }
                        })
                .thenApply(future -> (Integer) future);
    }

    /**
     * WatermarkElement 用于 {@link HeapPriorityQueue}，表示一个可比较的水位线元素。
     */
    public static class WatermarkElement extends AbstractHeapPriorityQueueElement
            implements PriorityComparable<WatermarkElement> {

        private final Watermark watermark;

        public WatermarkElement(Watermark watermark) {
            this.watermark = watermark;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof WatermarkElement) {
                return watermark.equals(((WatermarkElement) o).watermark);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return watermark.hashCode();
        }

        @Override
        public int comparePriorityTo(@Nonnull WatermarkElement other) {
            return Long.compare(watermark.getTimestamp(), other.watermark.getTimestamp());
        }
    }

    /**
     * WatermarkAggregator 用于管理多个子任务的水位线，并计算最小水位线作为全局水位线。
     *
     * @param <T> 任务 ID 或算子的标识类型
     */
    static class WatermarkAggregator<T> {

        // 存储每个子任务的水位线
        private final Map<T, WatermarkElement> watermarks = new HashMap<>();

        // 维护按时间顺序排列的水位线队列
        private final HeapPriorityQueue<WatermarkElement> orderedWatermarks =
                new HeapPriorityQueue<>(PriorityComparator.forPriorityComparableObjects(), 10);

        private static final Watermark DEFAULT_WATERMARK = new Watermark(Long.MIN_VALUE);

        /**
         * 更新指定 key（子任务或算子）的水位线，并返回更新后的全局水位线（如果发生变化）。
         *
         * @param key 子任务或算子 ID
         * @param watermark 新的水位线
         * @return 如果全局水位线发生变化，则返回新的全局水位线；否则返回空
         */
        public Optional<Watermark> aggregate(T key, Watermark watermark) {
            // 记录旧的全局水位线
            Watermark oldAggregatedWatermark = getAggregatedWatermark();

            // 创建新的水位线元素
            WatermarkElement watermarkElement = new WatermarkElement(watermark);
            WatermarkElement oldWatermarkElement = watermarks.put(key, watermarkElement);
            if (oldWatermarkElement != null) {
                orderedWatermarks.remove(oldWatermarkElement);
            }
            orderedWatermarks.add(watermarkElement);

            // 计算新的全局水位线
            Watermark newAggregatedWatermark = getAggregatedWatermark();
            if (newAggregatedWatermark.equals(oldAggregatedWatermark)) {
                return Optional.empty();
            }
            return Optional.of(newAggregatedWatermark);
        }

        /**
         * 获取当前存储的所有子任务 ID 或算子 ID。
         *
         * @return 存储的 key 集合
         */
        public Set<T> keySet() {
            return watermarks.keySet();
        }

        /**
         * 计算全局水位线，即当前所有任务的最小水位线。
         *
         * @return 全局水位线
         */
        public Watermark getAggregatedWatermark() {
            WatermarkElement aggregatedWatermarkElement = orderedWatermarks.peek();
            return aggregatedWatermarkElement == null
                    ? DEFAULT_WATERMARK
                    : aggregatedWatermarkElement.watermark;
        }
    }
}
