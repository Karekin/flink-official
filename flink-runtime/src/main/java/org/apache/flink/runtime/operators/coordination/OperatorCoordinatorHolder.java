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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@code OperatorCoordinatorHolder} 负责管理 {@link OperatorCoordinator} 的生命周期，
 * 并处理它与其他组件的交互。它提供执行环境，并确保检查点（Checkpoint）和精确一次（Exactly-Once）语义的实现。
 *
 * <h3>精确一次（Exactly-once）语义</h3>
 *
 * <p>具体的精确一次语义请参考 {@link OperatorCoordinator#checkpointCoordinator(long, CompletableFuture)}。
 *
 * <h3>精确一次（Exactly-once）机制</h3>
 *
 * <p>Flink 通过以下机制保证 OperatorCoordinator 的精确一次语义：
 *
 * <ul>
 *   <li>事件通过一个特殊的通道 {@link SubtaskGatewayImpl} 传输。如果当前没有正在进行的检查点，
 *       事件会直接传输。
 *   <li>当协调器的检查点 Future 完成时，子任务网关（subtask gateway）会关闭。此后到达的事件会被缓冲，
 *       因为它们属于检查点后的新 epoch。
 *   <li>一旦作业中的所有协调器都完成检查点，Flink 会向数据源注入检查点屏障（barriers）。
 *       如果某个协调器收到来自子任务的 {@link AcknowledgeCheckpointEvent}（表示该子任务已接收检查点屏障并完成检查点），
 *       则重新打开该子任务的网关，并发送缓冲的事件。
 *   <li>如果在此期间某个任务失败，则事件会从网关中丢弃。对于协调器来说，这些事件被视为丢失的，
 *       因为它们在最新完整检查点之后被发送到了一个失败的子任务。
 * </ul>
 *
 * <p><b>重要：</b> 一个关键假设是，从调度器（Scheduler）到任务（Tasks）的所有事件都严格有序传输。
 * 协调器在检查点屏障注入后发送的事件，不能越过检查点屏障。这一点由 Flink 的 RPC 机制保证。
 *
 * <h3>并发控制和线程模型</h3>
 *
 * <p>该组件严格运行在调度器的主线程执行器（main-thread-executor）中。
 * 所有 "外部调用"（如来自 Scheduler 或 CheckpointCoordinator）都必须确保在主线程执行器中执行，
 * 以维护严格的调用顺序。
 *
 * <p>从协调器到外部世界的所有操作（如完成检查点、发送事件等）也会按照顺序提交到主线程执行器中执行。
 */
public class OperatorCoordinatorHolder
        implements OperatorCoordinatorCheckpointContext, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorCoordinatorHolder.class);

    /** 关联的 OperatorCoordinator */
    private final OperatorCoordinator coordinator;

    /** Operator 的唯一标识符 */
    private final OperatorID operatorId;

    /** OperatorCoordinator 的上下文，提供各种操作所需的环境 */
    private final LazyInitializedCoordinatorContext context;

    /** 管理子任务访问（SubtaskAccess）的工厂类 */
    private final SubtaskAccess.SubtaskAccessFactory taskAccesses;

    /**
     * 子任务网关映射（subtaskGatewayMap）。
     * 该映射用于在检查点期间控制每个子任务网关的开关。
     * 在并发执行尝试（concurrent execution attempt）被禁用时，该映射才可被读写。
     * 需要注意的是，当检查点启用时，当前保证并发执行尝试是被禁用的。
     */
    private final Map<Integer, SubtaskGatewayImpl> subtaskGatewayMap;

    /** 追踪未确认的事件，确保事件按顺序正确处理 */
    private final IncompleteFuturesTracker unconfirmedEvents;

    /** Operator 的最大并行度 */
    private final int operatorMaxParallelism;

    /** 处理全局失败的全局失败处理器 */
    private GlobalFailureHandler globalFailureHandler;

    /** Flink 调度器的主线程执行器，确保所有操作在主线程上执行 */
    private ComponentMainThreadExecutor mainThreadExecutor;

    /** Operator 的当前并行度 */
    private int operatorParallelism;

    /**
     * 构造函数，初始化 OperatorCoordinatorHolder。
     *
     * @param operatorId Operator 的唯一 ID
     * @param coordinator 关联的 OperatorCoordinator
     * @param context OperatorCoordinator 的上下文
     * @param taskAccesses 子任务访问的工厂类
     * @param operatorParallelism Operator 的当前并行度
     * @param operatorMaxParallelism Operator 的最大并行度
     */
    private OperatorCoordinatorHolder(
            final OperatorID operatorId,
            final OperatorCoordinator coordinator,
            final LazyInitializedCoordinatorContext context,
            final SubtaskAccess.SubtaskAccessFactory taskAccesses,
            final int operatorParallelism,
            final int operatorMaxParallelism) {

        this.operatorId = checkNotNull(operatorId);
        this.coordinator = checkNotNull(coordinator);
        this.context = checkNotNull(context);
        this.taskAccesses = checkNotNull(taskAccesses);
        this.operatorParallelism = operatorParallelism;
        this.operatorMaxParallelism = operatorMaxParallelism;

        this.subtaskGatewayMap = new HashMap<>();
        this.unconfirmedEvents = new IncompleteFuturesTracker();
    }

    /**
     * 懒加载初始化，包括全局失败处理器、主线程执行器和检查点协调器。
     *
     * @param globalFailureHandler 处理全局故障的全局失败处理器
     * @param mainThreadExecutor 主线程执行器
     * @param checkpointCoordinator 关联的检查点协调器
     */
    public void lazyInitialize(
            GlobalFailureHandler globalFailureHandler,
            ComponentMainThreadExecutor mainThreadExecutor,
            @Nullable CheckpointCoordinator checkpointCoordinator) {
        lazyInitialize(
                globalFailureHandler,
                mainThreadExecutor,
                checkpointCoordinator,
                operatorParallelism);
    }

    /**
     * 带并行度参数的懒加载初始化。
     */
    public void lazyInitialize(
            GlobalFailureHandler globalFailureHandler,
            ComponentMainThreadExecutor mainThreadExecutor,
            @Nullable CheckpointCoordinator checkpointCoordinator,
            final int operatorParallelism) {
        this.globalFailureHandler = globalFailureHandler;
        this.mainThreadExecutor = mainThreadExecutor;
        checkState(operatorParallelism != ExecutionConfig.PARALLELISM_DEFAULT);
        this.operatorParallelism = operatorParallelism;
        context.lazyInitialize(
                globalFailureHandler,
                mainThreadExecutor,
                checkpointCoordinator,
                operatorParallelism);
        setupAllSubtaskGateways();
    }

    // ------------------------------------------------------------------------
    //  获取 OperatorCoordinator 的属性
    // ------------------------------------------------------------------------

    /**
     * 获取 OperatorCoordinator 实例
     *
     * @return OperatorCoordinator 实例
     */
    public OperatorCoordinator coordinator() {
        return coordinator;
    }

    /**
     * 获取 OperatorID
     *
     * @return OperatorID
     */
    @Override
    public OperatorID operatorId() {
        return operatorId;
    }

    /**
     * 获取 Operator 的最大并行度
     *
     * @return 最大并行度
     */
    @Override
    public int maxParallelism() {
        return operatorMaxParallelism;
    }

    /**
     * 获取 Operator 的当前并行度
     *
     * @return 当前并行度
     */
    @Override
    public int currentParallelism() {
        return operatorParallelism;
    }


    // ------------------------------------------------------------------------
    //  OperatorCoordinator Interface
    // ------------------------------------------------------------------------

    /**
     * 启动 OperatorCoordinator。
     * 该方法确保在调度器的主线程执行，并初始化协调器的运行环境。
     *
     * @throws Exception 如果启动过程中发生异常
     */
    public void start() throws Exception {
        // 确保当前线程是主线程
        mainThreadExecutor.assertRunningInMainThread();
        // 确保协调器的上下文已被初始化
        checkState(context.isInitialized(), "Coordinator Context is not yet initialized");
        // 启动协调器
        coordinator.start();
    }

    /**
     * 关闭 OperatorCoordinator。
     * 关闭协调器，并释放相关资源。
     *
     * @throws Exception 如果关闭过程中发生异常
     */
    @Override
    public void close() throws Exception {
        coordinator.close();
        context.unInitialize();
    }

    /**
     * 处理来自 Operator 发送的事件。
     *
     * @param subtask 事件来源的子任务索引
     * @param attemptNumber 当前执行尝试编号
     * @param event 事件对象
     * @throws Exception 如果事件处理失败
     */
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        // 确保当前线程是主线程
        mainThreadExecutor.assertRunningInMainThread();

        // 检查事件是否是 AcknowledgeCheckpointEvent（检查点确认事件）
        if (event instanceof AcknowledgeCheckpointEvent) {
            // 通过子任务网关处理该事件
            // 从子任务网关映射（subtaskGatewayMap）中获取对应子任务的网关
            // 解除该子任务对检查点的标记，并重新开放网关
            subtaskGatewayMap
                    .get(subtask)
                    .openGatewayAndUnmarkCheckpoint(
                            ((AcknowledgeCheckpointEvent) event).getCheckpointID());
            return;
        }

        // 如果事件不是 AcknowledgeCheckpointEvent，则将其传递给协调器（coordinator）进行处理
        coordinator.handleEventFromOperator(subtask, attemptNumber, event);
    }

    /**
     * 处理子任务执行失败事件。
     *
     * @param subtask 失败的子任务索引
     * @param attemptNumber 失败的执行尝试编号
     * @param reason 失败的异常原因（可能为空）
     */
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        // 确保当前线程是主线程
        mainThreadExecutor.assertRunningInMainThread();
        // 通知协调器子任务执行失败
        coordinator.executionAttemptFailed(subtask, attemptNumber, reason);
    }

    /**
     * 处理子任务重置事件，用于回滚到指定检查点。
     *
     * @param subtask 需要重置的子任务索引
     * @param checkpointId 需要恢复到的检查点 ID
     */
    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // 确保当前线程是主线程
        mainThreadExecutor.assertRunningInMainThread();

        // 先创建或恢复子任务网关，以便协调器可以访问
        setupSubtaskGateway(subtask);

        // 通知协调器子任务重置
        coordinator.subtaskReset(subtask, checkpointId);
    }

    /**
     * 触发 OperatorCoordinator 的检查点，并在检查点完成后将结果存入 CompletableFuture。
     *
     * @param checkpointId 检查点 ID
     * @param result 存储检查点结果的 CompletableFuture
     */
    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        // 该方法由 CheckpointCoordinator 调用，而 CheckpointCoordinator 运行在自己的线程中，
        // 因此需要通过 mainThreadExecutor 提交任务，确保在主线程中执行。
        mainThreadExecutor.execute(() -> checkpointCoordinatorInternal(checkpointId, result));
    }

    /**
     * 通知协调器检查点完成。
     *
     * @param checkpointId 检查点 ID
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // 该方法由 CheckpointCoordinator 调用，需要通过 mainThreadExecutor 提交任务，确保在主线程执行。
        mainThreadExecutor.execute(() -> {
            // 重新打开所有子任务网关，并解除检查点标记
            subtaskGatewayMap.values().forEach(x -> x.openGatewayAndUnmarkCheckpoint(checkpointId));
            // 通知协调器检查点完成
            coordinator.notifyCheckpointComplete(checkpointId);
        });
    }

    /**
     * 通知协调器检查点被中止。
     *
     * @param checkpointId 检查点 ID
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // 该方法由 CheckpointCoordinator 调用，需要通过 mainThreadExecutor 提交任务，确保在主线程执行。
        mainThreadExecutor.execute(() -> {
            // 重新打开所有子任务网关，并解除检查点标记
            subtaskGatewayMap.values().forEach(x -> x.openGatewayAndUnmarkCheckpoint(checkpointId));
            // 通知协调器检查点被中止
            coordinator.notifyCheckpointAborted(checkpointId);
        });
    }

    /**
     * 让协调器重置到指定的检查点。
     *
     * @param checkpointId 需要恢复到的检查点 ID
     * @param checkpointData 存储的检查点数据
     * @throws Exception 如果重置过程中发生异常
     */
    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        // 如果是 ExecutionGraph 初始化时的调用，主线程执行器可能还未设置，因此需要进行检查。
        if (mainThreadExecutor != null) {
            mainThreadExecutor.assertRunningInMainThread();
        }

        // 清理所有子任务的网关状态，并解除所有检查点标记
        subtaskGatewayMap.values().forEach(SubtaskGatewayImpl::openGatewayAndUnmarkAllCheckpoint);

        // 重置上下文中的失败标记
        context.resetFailed();

        // 当恢复初始 savepoint 时，可能发生主线程执行器尚未可用的情况，
        // 由于恢复过程中需要子任务网关，因此必须确保 setupAllSubtaskGateways 方法被调用。
        if (mainThreadExecutor != null) {
            setupAllSubtaskGateways();
        }

        // 通知协调器恢复到指定的检查点
        coordinator.resetToCheckpoint(checkpointId, checkpointData);
    }


    /**
     * 在主线程执行器中执行协调器的检查点逻辑。
     * 该方法会通知所有子任务标记检查点，并调用协调器的 checkpointCoordinator 方法触发检查点。
     * 当协调器的检查点完成后，该方法会关闭子任务网关并完成检查点结果的提交。
     *
     * @param checkpointId 检查点的唯一标识符
     * @param result 用于存储检查点结果的 CompletableFuture 对象
     */
    private void checkpointCoordinatorInternal(
            final long checkpointId, final CompletableFuture<byte[]> result) {
        // 确保当前操作在调度器的主线程执行器中执行
        mainThreadExecutor.assertRunningInMainThread();

        // 创建一个新的 CompletableFuture 对象，用于存储协调器检查点的结果
        final CompletableFuture<byte[]> coordinatorCheckpoint = new CompletableFuture<>();

        // 处理协调器检查点完成的逻辑（无论成功或失败）
        FutureUtils.assertNoException(
                coordinatorCheckpoint.handleAsync(
                        (success, failure) -> {
                            if (failure != null) {
                                // 如果检查点失败，则将异常传递给 result
                                result.completeExceptionally(failure);
                            } else if (closeGateways(checkpointId)) {
                                // 如果成功关闭所有子任务网关，则等待所有未完成的事件处理完毕后再完成检查点
                                completeCheckpointOnceEventsAreDone(checkpointId, result, success);
                            } else {
                                // 如果无法关闭网关，说明检查点已被中止
                                // 这里再次尝试将 result 置为异常状态，确保一致性
                                result.completeExceptionally(
                                        new FlinkException("Cannot close gateway"));
                            }
                            return null;
                        },
                        // 通过主线程执行器异步处理检查点结果，确保执行顺序
                        mainThreadExecutor));

        try {
            // 遍历所有子任务网关，并为每个子任务网关标记检查点
            subtaskGatewayMap.forEach(
                    (subtask, gateway) -> gateway.markForCheckpoint(checkpointId));

            // 调用协调器的 checkpointCoordinator 方法，触发实际的协调器检查点
            coordinator.checkpointCoordinator(checkpointId, coordinatorCheckpoint);
        } catch (Throwable t) {
            // 发生异常时，检查是否为致命错误或内存溢出，并进行异常处理
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            result.completeExceptionally(t);
            globalFailureHandler.handleGlobalFailure(t);
        }
    }

    /**
     * 关闭所有子任务网关，以确保检查点可以顺利完成。
     *
     * @param checkpointId 当前检查点的 ID
     * @return 如果所有网关都成功关闭，则返回 true；否则返回 false
     */
    private boolean closeGateways(final long checkpointId) {
        int closedGateways = 0;
        for (SubtaskGatewayImpl gateway : subtaskGatewayMap.values()) {
            if (gateway.tryCloseGateway(checkpointId)) {
                closedGateways++;
            }
        }

        // 确保所有网关的关闭状态一致，防止部分网关关闭，部分未关闭的情况
        if (closedGateways != 0 && closedGateways != subtaskGatewayMap.values().size()) {
            throw new IllegalStateException(
                    "Some subtask gateways can be closed while others cannot. There might be a bug here.");
        }

        return closedGateways != 0;
    }

    /**
     * 等待所有未完成的事件处理完成后，再最终提交检查点结果。
     *
     * @param checkpointId 检查点 ID
     * @param checkpointFuture 存储检查点结果的 Future
     * @param checkpointResult 检查点结果数据
     */
    private void completeCheckpointOnceEventsAreDone(
            final long checkpointId,
            final CompletableFuture<byte[]> checkpointFuture,
            final byte[] checkpointResult) {

        // 获取所有当前未完成的事件，并进行重置
        final Collection<CompletableFuture<?>> pendingEvents =
                unconfirmedEvents.getCurrentIncompleteAndReset();

        // 如果没有待处理的事件，则直接完成检查点
        if (pendingEvents.isEmpty()) {
            checkpointFuture.complete(checkpointResult);
            return;
        }

        // 记录日志，说明当前检查点需要等待未完成事件
        LOG.info(
                "Coordinator checkpoint {} for coordinator {} is awaiting {} pending events",
                checkpointId,
                operatorId,
                pendingEvents.size());

        // 等待所有未完成事件完成
        final CompletableFuture<?> conjunct = FutureUtils.waitForAll(pendingEvents);
        conjunct.whenComplete(
                (success, failure) -> {
                    if (failure == null) {
                        // 如果所有事件成功处理，则完成检查点
                        checkpointFuture.complete(checkpointResult);
                    } else {
                        // 如果有失败的事件，则检查点无法完成
                        // 可能的原因：
                        // (a) 目标任务已经崩溃
                        // (b) 由于丢失了 RPC 消息，需要执行任务的故障恢复，以确保一致性
                        checkpointFuture.completeExceptionally(
                                new FlinkException(
                                        "Failing OperatorCoordinator checkpoint because some OperatorEvents "
                                                + "before this checkpoint barrier were not received by the target tasks."));
                    }
                });
    }

    // ------------------------------------------------------------------------
    //  检查点回调（Checkpoint Callbacks）
    // ------------------------------------------------------------------------

    /**
     * 取消当前正在进行的检查点触发。
     * 该方法确保检查点中止时，所有子任务网关的状态都能正确恢复。
     */
    @Override
    public void abortCurrentTriggering() {
        // 该方法由 CheckpointCoordinator 线程调用，因此需要切换到主线程执行器执行
        mainThreadExecutor.execute(
                () ->
                        subtaskGatewayMap
                                .values()
                                .forEach(
                                        SubtaskGatewayImpl
                                                ::openGatewayAndUnmarkLastCheckpointIfAny));
    }

    // ------------------------------------------------------------------------
    //  其他辅助方法（Miscellaneous Helpers）
    // ------------------------------------------------------------------------

    /**
     * 初始化所有子任务的网关。
     */
    private void setupAllSubtaskGateways() {
        for (int i = 0; i < operatorParallelism; i++) {
            setupSubtaskGateway(i);
        }
    }

    /**
     * 为指定的子任务创建或恢复子任务网关。
     *
     * @param subtask 需要初始化网关的子任务索引
     */
    private void setupSubtaskGateway(int subtask) {
        for (SubtaskAccess sta : taskAccesses.getAccessesForSubtask(subtask)) {
            setupSubtaskGateway(sta);
        }
    }

    /**
     * 为特定的执行尝试（attempt）初始化子任务网关。
     *
     * @param subtask 需要初始化的子任务索引
     * @param attemptNumbers 该子任务的执行尝试编号集合
     */
    public void setupSubtaskGatewayForAttempts(int subtask, Set<Integer> attemptNumbers) {
        for (int attemptNumber : attemptNumbers) {
            setupSubtaskGateway(taskAccesses.getAccessForAttempt(subtask, attemptNumber));
        }
    }


    /**
     * 为指定的子任务创建或恢复子任务网关（SubtaskGateway）。
     * 该方法会为指定的子任务创建一个新的 {@link SubtaskGatewayImpl}，并根据是否支持并发执行尝试来决定是否存储该网关。
     *
     * @param sta 子任务访问接口 {@link SubtaskAccess}，用于获取子任务的状态和执行信息
     */
    private void setupSubtaskGateway(final SubtaskAccess sta) {
        // 创建一个新的子任务网关实例，管理该子任务的消息传输
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(sta, mainThreadExecutor, unconfirmedEvents);

        // 如果不支持并发执行尝试（Concurrent Execution Attempts），则需要维护 subtaskGatewayMap
        // 否则，检查点必须被禁用，因此无需维护子任务网关映射
        if (!context.isConcurrentExecutionAttemptsSupported()) {
            subtaskGatewayMap.put(gateway.getSubtask(), gateway);
        }

        // 需要同步执行此操作，以确保 'subtaskFailed()' 事件不会在 'subtaskReady()' 之前触发
        // ---
        // 存在一种情况，当任务执行被调度器恢复（restore）时，任务可能已经处于非运行状态（例如任务失败后恢复的早期阶段）。
        // 由于调度器可能在全局失败处理（global failure handling）期间处理多个失败并行恢复的任务，
        // 因此可能会调用 'subtaskReady()' 时该任务已经不在运行状态。
        // 为了避免不正确的任务状态，我们在这里进行检查，确保任务仍然在运行时才调用 'subtaskReady()'。
        FutureUtils.assertNoException(
                sta.hasSwitchedToRunning()
                        .thenAccept(
                                (ignored) -> {
                                    // 确保该操作在调度器的主线程执行器中执行
                                    mainThreadExecutor.assertRunningInMainThread();

                                    // 只有当任务仍然处于运行状态时，才通知协调器任务已准备好
                                    if (sta.isStillRunning()) {
                                        notifySubtaskReady(gateway);
                                    }
                                }));
    }

    /**
     * 通知 OperatorCoordinator 某个子任务已经准备就绪，并可进行协调。
     *
     * @param gateway 需要通知的子任务网关
     */
    private void notifySubtaskReady(OperatorCoordinator.SubtaskGateway gateway) {
        try {
            // 通知 OperatorCoordinator 该子任务的执行尝试（execution attempt）已准备就绪
            coordinator.executionAttemptReady(
                    gateway.getSubtask(), gateway.getExecution().getAttemptNumber(), gateway);
        } catch (Throwable t) {
            // 处理致命异常，如 JVM OOM（内存溢出）或不可恢复的错误
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

            // 发生错误时，触发全局失败处理器
            globalFailureHandler.handleGlobalFailure(
                    new FlinkException("Error from OperatorCoordinator", t));
        }
    }

    // ------------------------------------------------------------------------
    //  工厂方法（Factories）
    // ------------------------------------------------------------------------

    /**
     * 创建一个新的 OperatorCoordinatorHolder 实例。
     * 该方法会从序列化数据中恢复 OperatorCoordinator，并初始化相关上下文。
     *
     * @param serializedProvider 序列化的 OperatorCoordinator.Provider 实例
     * @param jobVertex 关联的执行任务顶点（ExecutionJobVertex）
     * @param classLoader 用户代码类加载器
     * @param coordinatorStore 协调器存储
     * @param supportsConcurrentExecutionAttempts 是否支持并发执行尝试
     * @param taskInformation 任务相关信息
     * @param metricGroup 任务的度量指标组
     * @return 创建的 OperatorCoordinatorHolder 实例
     * @throws Exception 如果创建过程中发生错误
     */
    public static OperatorCoordinatorHolder create(
            SerializedValue<OperatorCoordinator.Provider> serializedProvider,
            ExecutionJobVertex jobVertex,
            ClassLoader classLoader,
            CoordinatorStore coordinatorStore,
            boolean supportsConcurrentExecutionAttempts,
            TaskInformation taskInformation,
            JobManagerJobMetricGroup metricGroup)
            throws Exception {

        // 使用用户代码类加载器，确保反序列化过程中使用正确的 ClassLoader
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            // 反序列化 OperatorCoordinator.Provider
            final OperatorCoordinator.Provider provider =
                    serializedProvider.deserializeValue(classLoader);
            // 获取 OperatorID
            final OperatorID opId = provider.getOperatorId();

            // 创建子任务访问工厂（SubtaskAccessFactory），用于管理 OperatorCoordinator 对任务的访问
            final SubtaskAccess.SubtaskAccessFactory taskAccesses =
                    new ExecutionSubtaskAccess.ExecutionJobVertexSubtaskAccess(jobVertex, opId);

            // 调用工厂方法，创建 OperatorCoordinatorHolder 实例
            return create(
                    opId,
                    provider,
                    coordinatorStore,
                    jobVertex.getName(),
                    jobVertex.getGraph().getUserClassLoader(),
                    jobVertex.getParallelism(),
                    jobVertex.getMaxParallelism(),
                    taskAccesses,
                    supportsConcurrentExecutionAttempts,
                    taskInformation,
                    metricGroup);
        }
    }

    /**
     * 创建一个新的 OperatorCoordinatorHolder 实例，并初始化相关上下文。
     * 该方法用于测试，也可被主 create 方法调用。
     *
     * @param opId Operator 的唯一标识符
     * @param coordinatorProvider OperatorCoordinator.Provider 实例
     * @param coordinatorStore 协调器存储
     * @param operatorName Operator 名称
     * @param userCodeClassLoader 用户代码类加载器
     * @param operatorParallelism Operator 的当前并行度
     * @param operatorMaxParallelism Operator 的最大并行度
     * @param taskAccesses 任务访问工厂
     * @param supportsConcurrentExecutionAttempts 是否支持并发执行尝试
     * @param taskInformation 任务相关信息
     * @param jobManagerJobMetricGroup JobManager 级别的度量指标组
     * @return 创建的 OperatorCoordinatorHolder 实例
     * @throws Exception 如果创建过程中发生错误
     */
    @VisibleForTesting
    static OperatorCoordinatorHolder create(
            final OperatorID opId,
            final OperatorCoordinator.Provider coordinatorProvider,
            final CoordinatorStore coordinatorStore,
            final String operatorName,
            final ClassLoader userCodeClassLoader,
            final int operatorParallelism,
            final int operatorMaxParallelism,
            final SubtaskAccess.SubtaskAccessFactory taskAccesses,
            final boolean supportsConcurrentExecutionAttempts,
            final TaskInformation taskInformation,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws Exception {
        // 创建 Operator 的度量指标组
        final MetricGroup parentMetricGroup =
                jobManagerJobMetricGroup.getOrAddOperator(
                        taskInformation.getJobVertexId(),
                        taskInformation.getTaskName(),
                        opId,
                        operatorName);

        // 创建 OperatorCoordinator 的上下文对象
        final LazyInitializedCoordinatorContext context =
                new LazyInitializedCoordinatorContext(
                        opId,
                        operatorName,
                        userCodeClassLoader,
                        operatorParallelism,
                        coordinatorStore,
                        supportsConcurrentExecutionAttempts,
                        new InternalOperatorCoordinatorMetricGroup(parentMetricGroup));

        // 通过 Provider 创建 OperatorCoordinator 实例
        final OperatorCoordinator coordinator = coordinatorProvider.create(context);

        // 返回创建的 OperatorCoordinatorHolder 实例
        return new OperatorCoordinatorHolder(
                opId,
                coordinator,
                context,
                taskAccesses,
                operatorParallelism,
                operatorMaxParallelism);
    }


    // ------------------------------------------------------------------------
    //  嵌套类（Nested Classes）
    // ------------------------------------------------------------------------

    /**
     * {@link OperatorCoordinator.Context} 的具体实现。
     *
     * <p>该类提供了 OperatorCoordinator 运行所需的上下文信息，例如操作符 ID、用户代码类加载器、
     * 并行度、检查点协调器等。它允许 OperatorCoordinator 访问必要的信息，同时确保线程安全性。
     *
     * <p>所有方法都可以安全地从调度器（Scheduler）和 JobMaster 以外的线程调用。
     *
     * <p>实现注意事项：
     * 理想情况下，我们希望该类完全基于调度器接口进行操作，但目前调度器接口尚未暴露足够的信息，
     * 因此该类直接存储了一些必要的组件，如 `CheckpointCoordinator` 和 `GlobalFailureHandler`。
     */
    private static final class LazyInitializedCoordinatorContext
            implements OperatorCoordinator.Context {

        private static final Logger LOG =
                LoggerFactory.getLogger(LazyInitializedCoordinatorContext.class);

        /** Operator 的唯一标识符 */
        private final OperatorID operatorId;

        /** Operator 名称 */
        private final String operatorName;

        /** 用户代码类加载器 */
        private final ClassLoader userCodeClassLoader;

        /** OperatorCoordinator 共享存储，用于存储状态或共享数据 */
        private final CoordinatorStore coordinatorStore;

        /** 是否支持并发执行尝试（Concurrent Execution Attempts） */
        private final boolean supportsConcurrentExecutionAttempts;

        /** Operator 相关的指标组（Metrics） */
        private final OperatorCoordinatorMetricGroup metricGroup;

        /** 处理全局失败的处理器 */
        private GlobalFailureHandler globalFailureHandler;

        /** 调度器执行器（Scheduler Executor），用于在主线程执行任务 */
        private Executor schedulerExecutor;

        /** 关联的检查点协调器（CheckpointCoordinator），可能为空 */
        @Nullable private CheckpointCoordinator checkpointCoordinator;

        /** Operator 的当前并行度 */
        private int operatorParallelism;

        /** 是否已发生失败（volatile 关键字保证可见性） */
        private volatile boolean failed;

        /**
         * 构造方法，初始化上下文信息。
         *
         * @param operatorId Operator 的唯一 ID
         * @param operatorName Operator 名称
         * @param userCodeClassLoader 用户代码类加载器
         * @param operatorParallelism Operator 的当前并行度
         * @param coordinatorStore 共享存储
         * @param supportsConcurrentExecutionAttempts 是否支持并发执行尝试
         * @param metricGroup Operator 相关的指标组
         */
        public LazyInitializedCoordinatorContext(
                final OperatorID operatorId,
                final String operatorName,
                final ClassLoader userCodeClassLoader,
                final int operatorParallelism,
                final CoordinatorStore coordinatorStore,
                final boolean supportsConcurrentExecutionAttempts,
                final OperatorCoordinatorMetricGroup metricGroup) {
            this.operatorId = checkNotNull(operatorId);
            this.operatorName = checkNotNull(operatorName);
            this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
            this.operatorParallelism = operatorParallelism;
            this.coordinatorStore = checkNotNull(coordinatorStore);
            this.supportsConcurrentExecutionAttempts = supportsConcurrentExecutionAttempts;
            this.metricGroup = checkNotNull(metricGroup);
        }

        /**
         * 延迟初始化 OperatorCoordinator 上下文。
         * 该方法会设置全局失败处理器、调度器执行器和检查点协调器，并更新当前的并行度。
         *
         * @param globalFailureHandler 全局失败处理器
         * @param schedulerExecutor 调度器执行器
         * @param checkpointCoordinator 关联的检查点协调器（可能为空）
         * @param operatorParallelism Operator 的并行度
         */
        void lazyInitialize(
                GlobalFailureHandler globalFailureHandler,
                Executor schedulerExecutor,
                @Nullable CheckpointCoordinator checkpointCoordinator,
                final int operatorParallelism) {
            this.globalFailureHandler = checkNotNull(globalFailureHandler);
            this.schedulerExecutor = checkNotNull(schedulerExecutor);
            this.checkpointCoordinator = checkpointCoordinator;
            this.operatorParallelism = operatorParallelism;
        }

        /**
         * 释放上下文中的资源，解除引用，防止内存泄漏。
         */
        void unInitialize() {
            this.globalFailureHandler = null;
            this.schedulerExecutor = null;
            this.checkpointCoordinator = null;
        }

        /**
         * 判断上下文是否已完成初始化。
         *
         * @return 如果调度器执行器不为空，则返回 true，表示已初始化
         */
        boolean isInitialized() {
            return schedulerExecutor != null;
        }

        /**
         * 检查上下文是否已初始化，若未初始化，则抛出异常。
         */
        private void checkInitialized() {
            checkState(isInitialized(), "Context was not yet initialized");
        }

        /**
         * 重置失败状态，允许后续的失败检测。
         */
        void resetFailed() {
            failed = false;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinatorMetricGroup metricGroup() {
            return metricGroup;
        }

        /**
         * 触发作业失败。
         * 该方法用于在 OperatorCoordinator 发生致命错误时，通知调度器终止作业。
         *
         * @param cause 失败原因
         */
        @Override
        public void failJob(final Throwable cause) {
            // 确保上下文已初始化
            checkInitialized();

            // 创建 FlinkException 并附带 Operator 名称信息
            final FlinkException e =
                    new FlinkException(
                            "Global failure triggered by OperatorCoordinator for '"
                                    + operatorName
                                    + "' (operator "
                                    + operatorId
                                    + ").",
                            cause);

            // 如果作业已经处于失败状态，则忽略重复的失败请求
            if (failed) {
                LOG.debug(
                        "Ignoring the request to fail job because the job is already failing. "
                                + "The ignored failure cause is",
                        e);
                return;
            }
            failed = true;

            // 将全局失败处理任务提交到调度器执行器
            schedulerExecutor.execute(() -> globalFailureHandler.handleGlobalFailure(e));
        }

        /**
         * 获取当前 Operator 的并行度。
         *
         * @return Operator 的当前并行度
         */
        @Override
        public int currentParallelism() {
            return operatorParallelism;
        }

        /**
         * 获取用户代码类加载器。
         *
         * @return 用户代码类加载器
         */
        @Override
        public ClassLoader getUserCodeClassloader() {
            return userCodeClassLoader;
        }

        /**
         * 获取 OperatorCoordinator 的共享存储。
         *
         * @return CoordinatorStore 实例
         */
        @Override
        public CoordinatorStore getCoordinatorStore() {
            return coordinatorStore;
        }

        /**
         * 判断当前 Operator 是否支持并发执行尝试（Concurrent Execution Attempts）。
         *
         * @return 如果支持并发执行尝试，则返回 true；否则返回 false
         */
        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return supportsConcurrentExecutionAttempts;
        }

        /**
         * 获取与该 OperatorCoordinator 关联的检查点协调器。
         *
         * @return 关联的 CheckpointCoordinator 实例，可能为空
         */
        @Override
        @Nullable
        public CheckpointCoordinator getCheckpointCoordinator() {
            return checkpointCoordinator;
        }
    }

}
