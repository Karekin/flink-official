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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.runtime.operators.coordination.ComponentClosingUtils.closeAsyncWithTimeout;

/**
 * 此类在重置到检查点时会重新创建 {@link OperatorCoordinator} 实例。
 */
public class RecreateOnResetOperatorCoordinator implements OperatorCoordinator {
    private static final Logger LOG =
            LoggerFactory.getLogger(RecreateOnResetOperatorCoordinator.class);
    private static final long CLOSING_TIMEOUT_MS = 60000L; // 关闭超时时间（毫秒）
    private final Provider provider; // 提供器，用于创建新的 OperatorCoordinator 实例
    private final long closingTimeoutMs; // 关闭超时时间
    private final OperatorCoordinator.Context context; // OperatorCoordinator 的上下文
    private DeferrableCoordinator coordinator; // 可延迟的协调器
    private boolean started; // 是否已启动
    private volatile boolean closed; // 是否已关闭

    /**
     * 构造函数。
     *
     * @param context 上下文
     * @param provider 提供器
     * @param closingTimeoutMs 关闭超时时间
     * @throws Exception 如果创建过程中发生异常
     */
    private RecreateOnResetOperatorCoordinator(
            OperatorCoordinator.Context context, Provider provider, long closingTimeoutMs)
            throws Exception {
        this.context = context;
        this.provider = provider;
        this.coordinator = new DeferrableCoordinator(context.getOperatorId()); // 创建可延迟的协调器
        this.coordinator.createNewInternalCoordinator(context, provider); // 创建新的内部协调器
        this.coordinator.processPendingCalls(); // 处理挂起的调用
        this.closingTimeoutMs = closingTimeoutMs;
        this.started = false;
        this.closed = false;
    }

    @Override
    public void start() throws Exception {
        Preconditions.checkState(!started, "coordinator already started"); // 检查是否已启动
        started = true;
        coordinator.applyCall("start", OperatorCoordinator::start); // 执行启动操作
    }

    @Override
    public void close() throws Exception {
        closed = true;
        coordinator.closeAsync(closingTimeoutMs); // 异步关闭协调器
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        coordinator.applyCall(
                "handleEventFromOperator",
                c -> c.handleEventFromOperator(subtask, attemptNumber, event)); // 处理来自操作符的事件
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        coordinator.applyCall(
                "executionAttemptFailed",
                c -> c.executionAttemptFailed(subtask, attemptNumber, reason)); // 处理执行尝试失败
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        coordinator.applyCall("subtaskReset", c -> c.subtaskReset(subtask, checkpointId)); // 重置子任务
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        coordinator.applyCall(
                "executionAttemptReady",
                c -> c.executionAttemptReady(subtask, attemptNumber, gateway)); // 执行尝试准备就绪
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        coordinator.applyCall(
                "checkpointCoordinator", c -> c.checkpointCoordinator(checkpointId, resultFuture)); // 检查点协调器
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        coordinator.applyCall("checkpointComplete", c -> c.notifyCheckpointComplete(checkpointId)); // 通知检查点完成
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        coordinator.applyCall("checkpointAborted", c -> c.notifyCheckpointAborted(checkpointId)); // 通知检查点中止
    }

    @Override
    public void resetToCheckpoint(final long checkpointId, @Nullable final byte[] checkpointData) {
        // 首先增加协调器的 epoch（纪元），以隔离活动中的协调器
        LOG.info("Resetting coordinator to checkpoint.");
        // 用一个新的 DeferrableCoordinator 实例替换协调器变量
        // 在这一点上，新协调器的内部协调器尚未创建
        // 之后的所有调用都将被发送到新的协调器
        final DeferrableCoordinator oldCoordinator = coordinator;
        final DeferrableCoordinator newCoordinator =
                new DeferrableCoordinator(context.getOperatorId());
        coordinator = newCoordinator;
        // 异步关闭旧协调器
        CompletableFuture<Void> closingFuture = oldCoordinator.closeAsync(closingTimeoutMs);

        // 创建并可能启动协调器，并应用所有挂起的调用
        final boolean wasStarted = this.started;

        closingFuture.whenComplete(
                (ignored, e) -> {
                    if (e != null) {
                        LOG.warn(
                                String.format(
                                        "Received exception when closing "
                                                + "operator coordinator for %s.",
                                        oldCoordinator.operatorId),
                                e);
                    }
                    if (!closed) {
                        // 之前的协调器已关闭。创建一个新的协调器。
                        newCoordinator.createNewInternalCoordinator(context, provider);
                        newCoordinator.resetAndStart(checkpointId, checkpointData, wasStarted);
                        newCoordinator.processPendingCalls();
                    }
                });
    }

    /**
     * 获取内部协调器。
     *
     * @return 内部协调器
     * @throws Exception 如果等待过程中发生异常
     */
    public OperatorCoordinator getInternalCoordinator() throws Exception {
        waitForAllAsyncCallsFinish();
        return coordinator.internalCoordinator;
    }

    @VisibleForTesting
    QuiesceableContext getQuiesceableContext() throws Exception {
        waitForAllAsyncCallsFinish();
        return coordinator.internalQuiesceableContext;
    }

    @VisibleForTesting
    void waitForAllAsyncCallsFinish() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        coordinator.applyCall("waitForAllAsyncCallsFinish", c -> future.complete(null));
        future.get();
    }

    /**
     * OperatorCoordinator 的提供器。
     */
    public abstract static class Provider implements OperatorCoordinator.Provider {
        private static final long serialVersionUID = 3002837631612629071L;
        private final OperatorID operatorID;

        public Provider(OperatorID operatorID) {
            this.operatorID = operatorID;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            return create(context, CLOSING_TIMEOUT_MS);
        }

        @VisibleForTesting
        protected OperatorCoordinator create(Context context, long closingTimeoutMs)
                throws Exception {
            return new RecreateOnResetOperatorCoordinator(context, this, closingTimeoutMs);
        }

        protected abstract OperatorCoordinator getCoordinator(OperatorCoordinator.Context context)
                throws Exception;
    }

    /**
     * 一个可静默的上下文包装类，用于在创建新的 OperatorCoordinator 时静默旧的 OperatorCoordinator，
     * 以防止其对作业主节点产生进一步的影响。
     */
    @VisibleForTesting
    static class QuiesceableContext implements OperatorCoordinator.Context {
        private final OperatorCoordinator.Context context;
        private volatile boolean quiesced;

        QuiesceableContext(OperatorCoordinator.Context context) {
            this.context = context;
            quiesced = false;
        }

        @Override
        public OperatorID getOperatorId() {
            return context.getOperatorId();
        }

        @Override
        public OperatorCoordinatorMetricGroup metricGroup() {
            return context.metricGroup();
        }

        @Override
        public synchronized void failJob(Throwable cause) {
            if (quiesced) {
                return;
            }
            context.failJob(cause);
        }

        @Override
        public int currentParallelism() {
            return context.currentParallelism();
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return context.getUserCodeClassloader();
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return context.getCoordinatorStore();
        }

        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return context.isConcurrentExecutionAttemptsSupported();
        }

        @Override
        @Nullable
        public CheckpointCoordinator getCheckpointCoordinator() {
            return context.getCheckpointCoordinator();
        }

        @VisibleForTesting
        synchronized void quiesce() {
            quiesced = true;
        }

        @VisibleForTesting
        boolean isQuiesced() {
            return quiesced;
        }

        private OperatorCoordinator.Context getContext() {
            return context;
        }
    }

    /**
     * 一个帮助类，用于实现完全异步的 {@link #resetToCheckpoint(long, byte[])} 行为。
     * 该类包装了一个 {@link OperatorCoordinator} 实例。它将被两个不同的线程访问：
     * 调度线程和在 {@link #closeAsync(long)} 中创建的关闭线程。
     * DeferrableCoordinator 可以处于三种状态：
     *
     * <ul>
     *   <li><b>挂起：</b> 内部 {@link OperatorCoordinator} 尚未创建，所有对 RecreateOnResetOperatorCoordinator 的方法调用都被添加到队列中。
     *   <li><b>追赶：</b> 内部 {@link OperatorCoordinator} 已经创建并正在处理挂起的方法调用。在这种状态下，所有对 RecreateOnResetOperatorCoordinator 的方法调用仍然会被入队，以确保正确的执行顺序。
     *   <li><b>追赶完成：</b> 内部 {@link OperatorCoordinator} 已经处理完所有挂起的方法调用。从这一点开始，对协调器的方法调用将直接在调用线程中执行，而不是被放入队列。
     * </ul>
     */
    private static class DeferrableCoordinator {
        private final OperatorID operatorId;
        private final BlockingQueue<NamedCall> pendingCalls;
        private QuiesceableContext internalQuiesceableContext;
        private OperatorCoordinator internalCoordinator;
        private boolean hasCaughtUp;
        private boolean closed;
        private volatile boolean failed;

        private DeferrableCoordinator(OperatorID operatorId) {
            this.operatorId = operatorId;
            this.pendingCalls = new LinkedBlockingQueue<>();
            this.hasCaughtUp = false;
            this.closed = false;
            this.failed = false;
        }

        synchronized <T extends Exception> void applyCall(
                String name, ThrowingConsumer<OperatorCoordinator, T> call) throws T {
            synchronized (this) {
                if (hasCaughtUp) {
                    call.accept(internalCoordinator);
                } else {
                    pendingCalls.add(new NamedCall(name, call));
                }
            }
        }

        synchronized void createNewInternalCoordinator(
                OperatorCoordinator.Context context, Provider provider) {
            if (closed) {
                return;
            }
            try {
                internalQuiesceableContext = new QuiesceableContext(context);
                internalCoordinator = provider.getCoordinator(internalQuiesceableContext);
            } catch (Exception e) {
                LOG.error("Failed to create new internal coordinator due to ", e);
                cleanAndFailJob(e);
            }
        }

        synchronized CompletableFuture<Void> closeAsync(long timeoutMs) {
            closed = true;
            if (internalCoordinator != null) {
                internalQuiesceableContext.quiesce();
                pendingCalls.clear();
                return closeAsyncWithTimeout(
                        "SourceCoordinator for " + operatorId,
                        (ThrowingRunnable<Exception>) internalCoordinator::close,
                        Duration.ofMillis(timeoutMs))
                        .exceptionally(
                                e -> {
                                    cleanAndFailJob(e);
                                    return null;
                                });
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }

        void processPendingCalls() {
            if (failed || closed || internalCoordinator == null) {
                return;
            }
            String name = "Unknown Call Name";
            try {
                while (!hasCaughtUp) {
                    while (!pendingCalls.isEmpty()) {
                        NamedCall namedCall = pendingCalls.poll();
                        if (namedCall != null) {
                            name = namedCall.name;
                            namedCall.getConsumer().accept(internalCoordinator);
                        }
                    }
                    synchronized (this) {
                        if (pendingCalls.isEmpty()) {
                            hasCaughtUp = true;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("Failed to process pending calls {} on coordinator.", name, t);
                cleanAndFailJob(t);
            }
        }

        void start() throws Exception {
            internalCoordinator.start();
        }

        void resetAndStart(
                final long checkpointId,
                @Nullable final byte[] checkpointData,
                final boolean started) {

            if (failed || closed || internalCoordinator == null) {
                return;
            }
            try {
                internalCoordinator.resetToCheckpoint(checkpointId, checkpointData);
                if (started) {
                    internalCoordinator.start();
                }
            } catch (Exception e) {
                LOG.error("Failed to reset the coordinator to checkpoint and start.", e);
                cleanAndFailJob(e);
            }
        }

        private void cleanAndFailJob(Throwable t) {
            if (!failed) {
                failed = true;
                internalQuiesceableContext.getContext().failJob(t);
                pendingCalls.clear();
            }
        }
    }

    private static class NamedCall {
        private final String name;
        private final ThrowingConsumer<OperatorCoordinator, ?> consumer;

        private NamedCall(String name, ThrowingConsumer<OperatorCoordinator, ?> consumer) {
            this.name = name;
            this.consumer = consumer;
        }

        public String getName() {
            return name;
        }

        public ThrowingConsumer<OperatorCoordinator, ?> getConsumer() {
            return consumer;
        }
    }
}
