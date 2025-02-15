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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.runtime.util.Runnables;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * {@link OperatorCoordinator.SubtaskGateway} 接口的实现类，允许 OperatorCoordinator 通过
 * {@link SubtaskAccess} 访问子任务的状态，并发送操作符事件（OperatorEvent）。
 *
 * <p>该类的实例可以被关闭，关闭后所有事件都会被阻塞并缓冲，待稍后释放：
 * - 如果实例因特定检查点（Checkpoint）被关闭，则此后到达的事件会被暂时阻塞，
 *   直到该检查点完成后再释放。
 * - 如果多个检查点正在进行，并且事件在其中一个检查点期间被阻塞，则该事件会在所有检查点完成后释放。
 *
 * <p>在关键通信路径上的方法（如关闭/重新打开网关、发送操作符事件）必须在单线程环境中运行，
 * 该单线程环境由 {@link #mainThreadExecutor} 在 {@link #SubtaskGatewayImpl} 构造函数中指定，
 * 以避免竞争条件（race conditions）。
 */
class SubtaskGatewayImpl implements OperatorCoordinator.SubtaskGateway {

    /** 操作符事件丢失时的错误消息模板 */
    private static final String EVENT_LOSS_ERROR_MESSAGE =
            "An OperatorEvent from an OperatorCoordinator to a task was lost. "
                    + "Triggering task failover to ensure consistency. Event: '%s', targetTask: %s";

    /** 表示没有有效检查点的默认值 */
    private static final long NO_CHECKPOINT = Long.MIN_VALUE;

    /** 子任务访问接口，用于获取状态信息并执行操作 */
    private final SubtaskAccess subtaskAccess;

    /** Flink 调度器的主线程执行器，确保所有操作都在主线程中执行 */
    private final ComponentMainThreadExecutor mainThreadExecutor;

    /** 追踪未完成的 Future 任务 */
    private final IncompleteFuturesTracker incompleteFuturesTracker;

    /** 记录因检查点而被阻塞的事件 */
    private final TreeMap<Long, List<BlockedEvent>> blockedEventsMap;

    /** 当前已标记但尚未解除标记的检查点 ID 集合 */
    private final TreeSet<Long> currentMarkedCheckpointIds;

    /** 记录最近一次尝试的检查点 ID */
    private long latestAttemptedCheckpointId;

    /**
     * 构造方法，初始化子任务网关。
     *
     * @param subtaskAccess 子任务访问接口
     * @param mainThreadExecutor 调度器主线程执行器
     * @param incompleteFuturesTracker 未完成 Future 追踪器
     */
    SubtaskGatewayImpl(
            SubtaskAccess subtaskAccess,
            ComponentMainThreadExecutor mainThreadExecutor,
            IncompleteFuturesTracker incompleteFuturesTracker) {
        this.subtaskAccess = subtaskAccess;
        this.mainThreadExecutor = mainThreadExecutor;
        this.incompleteFuturesTracker = incompleteFuturesTracker;
        this.blockedEventsMap = new TreeMap<>();
        this.currentMarkedCheckpointIds = new TreeSet<>();
        this.latestAttemptedCheckpointId = NO_CHECKPOINT;
    }

    /**
     * 发送操作符事件（OperatorEvent）到子任务。
     *
     * @param evt 需要发送的操作符事件
     * @return 事件成功发送的 CompletableFuture
     */
    @Override
    public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt) {
        // 检查子任务是否已准备好接收事件
        if (!isReady()) {
            throw new FlinkRuntimeException("SubtaskGateway is not ready, task not yet running.");
        }

        // 序列化事件，以便通过网络传输
        final SerializedValue<OperatorEvent> serializedEvent;
        try {
            serializedEvent = new SerializedValue<>(evt);
        } catch (IOException e) {
            // 由于序列化失败不可恢复，因此抛出运行时异常
            throw new FlinkRuntimeException("Cannot serialize operator event", e);
        }

        // 创建用于发送事件的操作
        final Callable<CompletableFuture<Acknowledge>> sendAction =
                subtaskAccess.createEventSendAction(serializedEvent);

        // 创建一个 CompletableFuture 用于存储发送结果
        final CompletableFuture<Acknowledge> sendResult = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> result =
                sendResult.whenCompleteAsync(
                        (success, failure) -> {
                            // 如果发送失败，并且子任务仍然运行，则触发任务失败
                            if (failure != null && subtaskAccess.isStillRunning()) {
                                String msg =
                                        String.format(
                                                EVENT_LOSS_ERROR_MESSAGE,
                                                evt,
                                                subtaskAccess.subtaskName());
                                Runnables.assertNoException(
                                        () ->
                                                subtaskAccess.triggerTaskFailover(
                                                        new FlinkException(msg, failure)));
                            }
                        },
                        mainThreadExecutor);

        // 在主线程执行器中执行事件发送任务，并追踪未完成的 Future
        mainThreadExecutor.execute(
                () -> {
                    sendEventInternal(sendAction, sendResult);
                    incompleteFuturesTracker.trackFutureWhileIncomplete(result);
                });

        return result;
    }

    /**
     * 处理发送事件的内部逻辑。
     * 如果当前有未完成的检查点，则事件会被暂存；否则，直接发送事件。
     *
     * @param sendAction 发送事件的任务
     * @param result 存储发送结果的 CompletableFuture
     */
    private void sendEventInternal(
            Callable<CompletableFuture<Acknowledge>> sendAction,
            CompletableFuture<Acknowledge> result) {
        checkRunsInMainThread();

        // 如果当前存在未完成的检查点，则事件会被暂存到最后一个检查点的缓冲区中
        if (!blockedEventsMap.isEmpty()) {
            blockedEventsMap.lastEntry().getValue().add(new BlockedEvent(sendAction, result));
        } else {
            // 否则，直接发送事件
            callSendAction(sendAction, result);
        }
    }

    /**
     * 调用实际的事件发送逻辑。
     *
     * @param sendAction 发送事件的任务
     * @param result 存储发送结果的 CompletableFuture
     */
    private void callSendAction(
            Callable<CompletableFuture<Acknowledge>> sendAction,
            CompletableFuture<Acknowledge> result) {
        try {
            final CompletableFuture<Acknowledge> sendResult = sendAction.call();
            FutureUtils.forward(sendResult, result);
        } catch (Throwable t) {
            // 如果发生致命错误（如 OOM），立即重新抛出
            ExceptionUtils.rethrowIfFatalError(t);
            // 否则，标记 Future 失败
            result.completeExceptionally(t);
        }
    }

    /**
     * 获取当前执行尝试（ExecutionAttemptID）。
     *
     * @return 当前子任务的执行尝试 ID
     */
    @Override
    public ExecutionAttemptID getExecution() {
        return subtaskAccess.currentAttempt();
    }

    /**
     * 获取当前子任务的索引。
     *
     * @return 子任务索引
     */
    @Override
    public int getSubtask() {
        return subtaskAccess.getSubtaskIndex();
    }

    /**
     * 检查子任务是否已经切换到运行状态。
     *
     * @return 如果任务已切换到运行状态，则返回 true，否则返回 false
     */
    private boolean isReady() {
        return subtaskAccess.hasSwitchedToRunning().isDone();
    }

    /**
     * 标记子任务网关，使其参与指定检查点。
     * 该方法会记录检查点 ID，并确保仅允许在该检查点下关闭网关。
     *
     * <p>该机制可用于检测多个协调器检查点可能发生重叠的情况（当前不支持）。
     * 它还能帮助识别某个检查点在网关关闭之前就已被中止的情况。
     *
     * @param checkpointId 需要标记的检查点 ID
     */
    void markForCheckpoint(long checkpointId) {
        checkRunsInMainThread();

        if (checkpointId > latestAttemptedCheckpointId) {
            currentMarkedCheckpointIds.add(checkpointId);
            latestAttemptedCheckpointId = checkpointId;
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Regressing checkpoint IDs. Previous checkpointId = %d, new checkpointId = %d",
                            latestAttemptedCheckpointId, checkpointId));
        }
    }

    /**
     * 关闭子任务网关，阻止事件发送，直到网关重新打开。
     *
     * @param checkpointId 需要关闭的检查点 ID
     * @return 如果成功关闭网关，则返回 true；如果 checkpointId 不正确，则返回 false
     */
    boolean tryCloseGateway(long checkpointId) {
        checkRunsInMainThread();

        if (currentMarkedCheckpointIds.contains(checkpointId)) {
            blockedEventsMap.putIfAbsent(checkpointId, new LinkedList<>());
            return true;
        }

        return false;
    }

    /**
     * 重新打开子任务网关，并解除对指定检查点的标记，使得先前因该检查点而阻塞的事件得以释放。
     *
     * @param checkpointId 需要解除标记的检查点 ID
     */
    void openGatewayAndUnmarkCheckpoint(long checkpointId) {
        checkRunsInMainThread();

        // 如果尝试打开一个未知的检查点 ID，则抛出异常
        if (latestAttemptedCheckpointId < checkpointId) {
            throw new IllegalStateException(
                    String.format(
                            "Trying to open gateway for unseen checkpoint: "
                                    + "latest known checkpoint = %d, incoming checkpoint = %d",
                            latestAttemptedCheckpointId, checkpointId));
        }

        // 如果当前检查点 ID 不在已标记的集合中，说明它已经被取消或已处理，因此直接返回
        if (!currentMarkedCheckpointIds.contains(checkpointId)) {
            return;
        }

        // 处理缓冲的事件
        if (blockedEventsMap.containsKey(checkpointId)) {
            // 如果该检查点 ID 是最早的检查点 ID，则直接执行缓冲事件
            if (blockedEventsMap.firstKey() == checkpointId) {
                for (BlockedEvent blockedEvent : blockedEventsMap.firstEntry().getValue()) {
                    callSendAction(blockedEvent.sendAction, blockedEvent.future);
                }
            } else {
                // 否则，将当前检查点 ID 的阻塞事件合并到前一个检查点 ID 的事件列表中
                blockedEventsMap
                        .floorEntry(checkpointId - 1)
                        .getValue()
                        .addAll(blockedEventsMap.get(checkpointId));
            }
            // 移除已处理的检查点 ID
            blockedEventsMap.remove(checkpointId);
        }

        // 从当前已标记的检查点集合中移除该检查点 ID
        currentMarkedCheckpointIds.remove(checkpointId);
    }

    /**
     * 重新打开网关并解除所有检查点的阻塞状态，使所有被缓冲的事件得以释放。
     */
    void openGatewayAndUnmarkAllCheckpoint() {
        checkRunsInMainThread();

        // 遍历所有被阻塞的事件，依次执行
        for (List<BlockedEvent> blockedEvents : blockedEventsMap.values()) {
            for (BlockedEvent blockedEvent : blockedEvents) {
                callSendAction(blockedEvent.sendAction, blockedEvent.future);
            }
        }

        // 清空所有已阻塞的事件和检查点 ID 记录
        blockedEventsMap.clear();
        currentMarkedCheckpointIds.clear();
    }

    /**
     * 重新打开网关，并解除最近一个未完成的检查点的阻塞状态。
     */
    void openGatewayAndUnmarkLastCheckpointIfAny() {
        if (!currentMarkedCheckpointIds.isEmpty()) {
            openGatewayAndUnmarkCheckpoint(currentMarkedCheckpointIds.last());
        }
    }

    /**
     * 确保当前操作在主线程中执行。
     */
    private void checkRunsInMainThread() {
        mainThreadExecutor.assertRunningInMainThread();
    }

    /**
     * 内部类，表示一个被阻塞的事件及其发送结果。
     */
    private static final class BlockedEvent {

        /** 发送事件的操作 */
        private final Callable<CompletableFuture<Acknowledge>> sendAction;

        /** 事件发送结果的 Future */
        private final CompletableFuture<Acknowledge> future;

        BlockedEvent(
                Callable<CompletableFuture<Acknowledge>> sendAction,
                CompletableFuture<Acknowledge> future) {
            this.sendAction = sendAction;
            this.future = future;
        }
    }

}
