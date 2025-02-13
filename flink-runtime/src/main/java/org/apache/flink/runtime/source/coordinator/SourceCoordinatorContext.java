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
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SupportsIntermediateNoMoreSplits;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.IsProcessingBacklogEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.flink.runtime.operators.coordination.ComponentClosingUtils.shutdownExecutorForcefully;
import static org.apache.flink.util.Preconditions.checkState;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 为 {@link OperatorCoordinator} 提供上下文的类。与 {@link SplitEnumeratorContext} 相比，该类允许与状态交互，并向 SourceOperator 发送 {@link OperatorEvent}。
 * 而 {@link SplitEnumeratorContext} 仅允许发送 {@link SourceEvent}。
 *
 * <p>上下文主要服务于以下目的：
 *
 * <ul>
 *   <li>信息提供者 - 上下文为枚举器提供了必要的信息，使其了解 SourceOperator 的状态以及它们的分片分配情况。
 *       这些信息使分片枚举器能够进行协调工作。
 *   <li>行动执行者 - 上下文还为枚举器提供了一些可以执行的行动，以进行协调工作。到目前为止，主要有两个行动：
 *          1）向 SourceOperator 分配分片，2）向 SourceOperator 发送自定义的 {@link SourceEvent SourceEvents}。
 *   <li>线程模型强制执行 - 上下文确保所有对协调器状态的操作都由同一个线程处理。
 * </ul>
 *
 * @param <SplitT> 分片的类型。
 */
@Internal
public class SourceCoordinatorContext<SplitT extends SourceSplit>
        implements SplitEnumeratorContext<SplitT>, SupportsIntermediateNoMoreSplits, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinatorContext.class);

    private final ScheduledExecutorService workerExecutor; // 用于执行 worker 线程任务的线程池
    private final ScheduledExecutorService coordinatorExecutor; // 用于执行协调器线程任务的线程池
    private final ExecutorNotifier notifier; // 用于通知任务执行状态，并处理线程中的异常
    private final OperatorCoordinator.Context operatorCoordinatorContext; // 提供上下文信息，与 OperatorCoordinator 相关
    private final SimpleVersionedSerializer<SplitT> splitSerializer; // 用于序列化和反序列化 SplitT 类型的数据
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, ReaderInfo>> registeredReaders; // 存储已注册的 SourceOperator 信息，按子任务 ID 和尝试编号进行索引
    private final SplitAssignmentTracker<SplitT> assignmentTracker; // 跟踪数据分片的分配状态
    private final SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory; // 线程工厂，用于创建和管理协调器线程
    private SubtaskGateways subtaskGateways; // 维护不同子任务的网关信息
    private final String coordinatorThreadName; // 协调器线程的名称
    private final boolean supportsConcurrentExecutionAttempts; // 是否支持并发执行尝试
    private boolean[] subtaskHasNoMoreSplits; // 存储子任务是否已经处理完所有分片
    private volatile boolean closed; // 标记上下文是否已关闭
    private volatile TernaryBoolean backlog = TernaryBoolean.UNDEFINED; // 标记是否正在处理积压数据

    /**
     * 构造函数，初始化 SourceCoordinatorContext。
     *
     * @param coordinatorThreadFactory 线程工厂，用于创建协调器线程。
     * @param numWorkerThreads 工作线程的数量。
     * @param operatorCoordinatorContext 与 OperatorCoordinator 相关的上下文。
     * @param splitSerializer 用于序列化和反序列化 SplitT 类型的数据。
     * @param supportsConcurrentExecutionAttempts 是否支持并发执行尝试。
     */
    public SourceCoordinatorContext(
            SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            int numWorkerThreads,
            OperatorCoordinator.Context operatorCoordinatorContext,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            boolean supportsConcurrentExecutionAttempts) {
        this(
                Executors.newScheduledThreadPool(1, coordinatorThreadFactory),
                Executors.newScheduledThreadPool(
                        numWorkerThreads,
                        new ExecutorThreadFactory(
                                coordinatorThreadFactory.getCoordinatorThreadName() + "-worker")),
                coordinatorThreadFactory,
                operatorCoordinatorContext,
                splitSerializer,
                new SplitAssignmentTracker<>(),
                supportsConcurrentExecutionAttempts);
    }

    /**
     * 用于单元测试的包私有构造函数。
     *
     * @param coordinatorExecutor 协调器线程池。
     * @param workerExecutor 工作线程池。
     * @param coordinatorThreadFactory 线程工厂，用于创建协调器线程。
     * @param operatorCoordinatorContext 与 OperatorCoordinator 相关的上下文。
     * @param splitSerializer 用于序列化和反序列化 SplitT 类型的数据。
     * @param splitAssignmentTracker 跟踪数据分片分配状态的对象。
     * @param supportsConcurrentExecutionAttempts 是否支持并发执行尝试。
     */
    @VisibleForTesting
    SourceCoordinatorContext(
            ScheduledExecutorService coordinatorExecutor,
            ScheduledExecutorService workerExecutor,
            SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            SplitAssignmentTracker<SplitT> splitAssignmentTracker,
            boolean supportsConcurrentExecutionAttempts) {
        this.workerExecutor = workerExecutor;
        this.coordinatorExecutor = coordinatorExecutor;
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.operatorCoordinatorContext = operatorCoordinatorContext;
        this.splitSerializer = splitSerializer;
        this.registeredReaders = new ConcurrentHashMap<>();
        this.assignmentTracker = splitAssignmentTracker;
        this.coordinatorThreadName = coordinatorThreadFactory.getCoordinatorThreadName();
        this.supportsConcurrentExecutionAttempts = supportsConcurrentExecutionAttempts;

        // 创建一个错误处理的协调器线程
        final Executor errorHandlingCoordinatorExecutor =
                (runnable) ->
                        coordinatorExecutor.execute(
                                new ThrowableCatchingRunnable(
                                        this::handleUncaughtExceptionFromAsyncCall, runnable));

        this.notifier =
                new ExecutorNotifier(workerExecutor, errorHandlingCoordinatorExecutor); // 初始化通知器
    }

    /**
     * 检查是否支持并发执行尝试。
     *
     * @return 如果支持并发执行尝试，返回 true；否则返回 false。
     */
    boolean isConcurrentExecutionAttemptsSupported() {
        return supportsConcurrentExecutionAttempts;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return new InternalSplitEnumeratorMetricGroup(
                operatorCoordinatorContext.metricGroup()); // 返回内部的分片枚举器指标组
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        checkAndLazyInitialize();
        // 检查是否支持并发执行尝试，如果不支持，则抛出异常
        checkState(
                !supportsConcurrentExecutionAttempts,
                "The split enumerator must invoke SplitEnumeratorContext"
                        + "#sendEventToSourceReader(int, int, SourceEvent) instead of "
                        + "SplitEnumeratorContext#sendEventToSourceReader(int, SourceEvent) "
                        + "to send custom source events in concurrent execution attempts scenario "
                        + "(e.g. if speculative execution is enabled).");
        checkSubtaskIndex(subtaskId); // 检查子任务索引是否有效

        // 在协调器线程中调用 lambda 表达式
        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getOnlyGatewayAndCheckReady(subtaskId); // 获取子任务的网关
                    gateway.sendEvent(new SourceEventWrapper(event)); // 向子任务发送事件
                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId)); // 如果失败，记录错误信息
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
        checkAndLazyInitialize();
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getGatewayAndCheckReady(subtaskId, attemptNumber); // 获取特定尝试的子任务网关
                    gateway.sendEvent(new SourceEventWrapper(event)); // 向子任务发送事件
                    return null;
                },
                String.format(
                        "Failed to send event %s to subtask %d (#%d)",
                        event, subtaskId, attemptNumber)); // 如果失败，记录错误信息
    }

    void sendEventToSourceOperator(int subtaskId, OperatorEvent event) {
        checkAndLazyInitialize();
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getOnlyGatewayAndCheckReady(subtaskId); // 获取子任务的网关
                    gateway.sendEvent(event); // 向子任务发送操作事件
                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }

    void sendEventToSourceOperatorIfTaskReady(int subtaskId, OperatorEvent event) {
        checkAndLazyInitialize();
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getOnlyGatewayAndNotCheckReady(subtaskId); // 获取子任务的网关
                    if (gateway != null) {
                        gateway.sendEvent(event); // 如果网关存在，发送操作事件
                    }

                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }

    @Override
    public int currentParallelism() {
        return operatorCoordinatorContext.currentParallelism(); // 返回当前并行度
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        final Map<Integer, ReaderInfo> readers = new HashMap<>();
        // 遍历已注册的读者信息
        for (Map.Entry<Integer, ConcurrentMap<Integer, ReaderInfo>> entry : registeredReaders.entrySet()) {
            final int subtaskIndex = entry.getKey();
            final Map<Integer, ReaderInfo> attemptReaders = entry.getValue();
            int earliestAttempt = Integer.MAX_VALUE;
            // 寻找最早的尝试编号
            for (int attemptNumber : attemptReaders.keySet()) {
                if (attemptNumber < earliestAttempt) {
                    earliestAttempt = attemptNumber;
                }
            }
            readers.put(subtaskIndex, attemptReaders.get(earliestAttempt)); // 保存最早的尝试的读者信息
        }
        return Collections.unmodifiableMap(readers); // 返回不可修改的读者信息
    }

    @Override
    public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
        return Collections.unmodifiableMap(registeredReaders); // 返回不可修改的读者尝试信息
    }

    @Override
    public void assignSplits(SplitsAssignment<SplitT> assignment) {
        // 确保分片分配在协调器线程中执行
        callInCoordinatorThread(
                () -> {
                    // 确保分配中的所有子任务都已注册
                    assignment
                            .assignment()
                            .forEach(
                                    (id, splits) -> {
                                        if (!registeredReaders.containsKey(id)) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Cannot assign splits %s to subtask %d because the subtask is not registered.",
                                                            splits, id));
                                        }
                                    });

                    assignmentTracker.recordSplitAssignment(assignment); // 记录分片分配
                    assignSplitsToAttempts(assignment); // 向尝试分配分片
                    return null;
                },
                String.format("Failed to assign splits %s due to ", assignment)); // 如果失败，记录错误信息
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        checkSubtaskIndex(subtask);

        callInCoordinatorThread(
                () -> {
                    subtaskHasNoMoreSplits[subtask] = true; // 标记子任务已无更多分片
                    signalNoMoreSplitsToAttempts(subtask); // 向所有尝试发送无更多分片信号
                    return null;
                },
                "Failed to send 'NoMoreSplits' to reader " + subtask); // 如果失败，记录错误信息
    }

    @Override
    public void signalIntermediateNoMoreSplits(int subtask) {
        checkSubtaskIndex(subtask);

        callInCoordinatorThread(
                () -> {
                    signalNoMoreSplitsToAttempts(subtask); // 向所有尝试发送中间无更多分片信号
                    return null;
                },
                "Failed to send 'IntermediateNoMoreSplits' to reader " + subtask); // 如果失败，记录错误信息
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period) {
        notifier.notifyReadyAsync(callable, handler, initialDelay, period); // 异步调用并通知结果
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        notifier.notifyReadyAsync(callable, handler); // 异步调用并通知结果
    }

    /**
     * 在协调器线程中运行 Runnable。
     *
     * @param runnable 要运行的 Runnable。
     */
    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        // 当使用 ScheduledThreadPool 时，未捕获的异常处理器会捕获线程池中的异常，所以需要手动调用它
        coordinatorExecutor.execute(
                new ThrowableCatchingRunnable(
                        throwable ->
                                coordinatorThreadFactory.uncaughtException(Thread.currentThread(), throwable),
                        runnable));
    }

    @Override
    public void close() throws InterruptedException {
        closed = true;
        // 安全关闭线程池，确保关闭序列完全执行
        shutdownExecutorForcefully(workerExecutor, Duration.ofNanos(Long.MAX_VALUE));
        shutdownExecutorForcefully(coordinatorExecutor, Duration.ofNanos(Long.MAX_VALUE));
    }

    @VisibleForTesting
    boolean isClosed() {
        return closed; // 返回是否已关闭
    }

    @Override
    public void setIsProcessingBacklog(boolean isProcessingBacklog) {
        CheckpointCoordinator checkpointCoordinator =
                getCoordinatorContext().getCheckpointCoordinator(); // 获取 CheckpointCoordinator
        OperatorID operatorID = getCoordinatorContext().getOperatorId(); // 获取操作算子 ID
        if (checkpointCoordinator != null) {
            checkpointCoordinator.setIsProcessingBacklog(operatorID, isProcessingBacklog); // 设置是否正在处理积压数据
        }
        backlog = TernaryBoolean.fromBoolean(isProcessingBacklog); // 更新积压状态
        callInCoordinatorThread(
                () -> {
                    final IsProcessingBacklogEvent isProcessingBacklogEvent =
                            new IsProcessingBacklogEvent(isProcessingBacklog); // 创建积压事件
                    for (int i = 0; i < getCoordinatorContext().currentParallelism(); i++) {
                        sendEventToSourceOperatorIfTaskReady(i, isProcessingBacklogEvent); // 向源操作者发送事件
                    }
                    return null;
                },
                "Failed to send BacklogEvent to reader."); // 如果失败，记录错误信息
    }

    // 包私有方法，用于 SourceCoordinator

    void attemptReady(OperatorCoordinator.SubtaskGateway gateway) {
        checkAndLazyInitialize();
        checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread()); // 检查当前线程是否是协调器线程

        subtaskGateways.registerSubtaskGateway(gateway); // 注册子任务网关
    }

    void attemptFailed(int subtaskIndex, int attemptNumber) {
        checkAndLazyInitialize();
        checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread()); // 检查当前线程是否是协调器线程

        subtaskGateways.unregisterSubtaskGateway(subtaskIndex, attemptNumber); // 注销子任务网关
    }

    void subtaskReset(int subtaskIndex) {
        checkAndLazyInitialize();
        checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread()); // 检查当前线程是否是协调器线程

        subtaskGateways.reset(subtaskIndex); // 重置子任务网关
        registeredReaders.remove(subtaskIndex); // 移除已注册的读者信息
        subtaskHasNoMoreSplits[subtaskIndex] = false; // 重置子任务无更多分片标志
    }

    boolean hasNoMoreSplits(int subtaskIndex) {
        checkAndLazyInitialize();
        return subtaskHasNoMoreSplits[subtaskIndex]; // 返回子任务是否已无更多分片
    }

    /**
     * 用给定的原因失败作业。
     *
     * @param cause 失败的原因。
     */
    void failJob(Throwable cause) {
        operatorCoordinatorContext.failJob(cause); // 触发 Job 失败
    }

    void handleUncaughtExceptionFromAsyncCall(Throwable t) {
        if (closed) {
            return; // 如果已关闭，直接返回
        }

        ExceptionUtils.rethrowIfFatalErrorOrOOM(t); // 如果是致命错误或 OOM，重新抛出异常
        LOG.error(
                "Exception while handling result from async call in {}. Triggering job failover.",
                coordinatorThreadName,
                t); // 记录错误日志
        failJob(t); // 触发 Job 失败
    }

    /**
     * 处理 SourceCoordinatorContext 的检查点行为。
     *
     * @param checkpointId 检查点 ID。
     */
    void onCheckpoint(long checkpointId) throws Exception {
        assignmentTracker.onCheckpoint(checkpointId); // 在检查点期间更新分配跟踪状态
    }

    /**
     * 注册 SourceOperator 。
     *
     * @param subtaskId 子任务 ID。
     * @param attemptNumber 尝试编号。
     * @param location 子任务的位置。
     */
    void registerSourceReader(int subtaskId, int attemptNumber, String location) {
        final Map<Integer, ReaderInfo> attemptReaders =
                registeredReaders.computeIfAbsent(
                        subtaskId, k -> new ConcurrentHashMap<>()); // 获取或创建尝试读者信息
        checkState(
                !attemptReaders.containsKey(attemptNumber),
                "ReaderInfo of subtask %s (#%s) already exists.",
                subtaskId,
                attemptNumber); // 检查尝试读者是否已存在
        attemptReaders.put(attemptNumber, new ReaderInfo(subtaskId, location)); // 注册读者信息

        sendCachedSplitsToNewlyRegisteredReader(subtaskId, attemptNumber); // 向新注册的读者发送缓存的分片
    }

    /**
     * 注销 SourceOperator 。
     *
     * @param subtaskId 子任务 ID。
     * @param attemptNumber 尝试编号。
     */
    void unregisterSourceReader(int subtaskId, int attemptNumber) {
        final Map<Integer, ReaderInfo> attemptReaders = registeredReaders.get(subtaskId); // 获取尝试读者信息
        if (attemptReaders != null) {
            attemptReaders.remove(attemptNumber); // 移除读者信息
            if (attemptReaders.isEmpty()) {
                registeredReaders.remove(subtaskId); // 如果尝试为空，移除子任务信息
            }
        }
    }

    /**
     * 获取并移除未检查点分配的分片。
     *
     * @param subtaskId 子任务 ID。
     * @param restoredCheckpointId 恢复的检查点 ID。
     * @return 需要重新分配的分片列表。
     */
    List<SplitT> getAndRemoveUncheckpointedAssignment(int subtaskId, long restoredCheckpointId) {
        return assignmentTracker.getAndRemoveUncheckpointedAssignment(
                subtaskId, restoredCheckpointId); // 获取并移除未检查点分配的分片
    }

    /**
     * 处理成功检查点。
     *
     * @param checkpointId 成功的检查点 ID。
     */
    void onCheckpointComplete(long checkpointId) {
        assignmentTracker.onCheckpointComplete(checkpointId); // 更新检查点完成后的分配状态
    }

    OperatorCoordinator.Context getCoordinatorContext() {
        return operatorCoordinatorContext; // 返回与 OperatorCoordinator 相关的上下文
    }

    // 执行器相关方法

    Future<?> submitTask(Runnable task) {
        return coordinatorExecutor.submit(task); // 提交任务并返回 Future
    }

    ScheduledFuture<?> schedulePeriodTask(
            Runnable command, long initDelay, long period, TimeUnit unit) {
        return coordinatorExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        command.run(); // 执行任务
                    } catch (Throwable t) {
                        handleUncaughtExceptionFromAsyncCall(t); // 处理未捕获的异常
                    }
                },
                initDelay,
                period,
                unit); // 定期执行任务
    }

    CompletableFuture<?> supplyAsync(Supplier<?> task) {
        return CompletableFuture.supplyAsync(task, coordinatorExecutor); // 异步执行任务并返回 CompletableFuture
    }

    // 私有辅助方法

    private void checkSubtaskIndex(int subtaskIndex) {
        if (subtaskIndex < 0 || subtaskIndex >= getCoordinatorContext().currentParallelism()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Subtask index %d is out of bounds [0, %s)",
                            subtaskIndex, getCoordinatorContext().currentParallelism())); // 检查子任务索引是否有效
        }
    }

    private void checkAndLazyInitialize() {
        if (subtaskGateways == null) {
            final int parallelism = operatorCoordinatorContext.currentParallelism();
            checkState(
                    parallelism != ExecutionConfig.PARALLELISM_DEFAULT,
                    "Parallelism must be set"); // 检查并行度是否已设置
            this.subtaskGateways = new SubtaskGateways(parallelism); // 初始化子任务网关
            this.subtaskHasNoMoreSplits = new boolean[parallelism]; // 初始化无更多分片标志数组
            Arrays.fill(subtaskHasNoMoreSplits, false);
        }
    }

    /**
     * 调用方法时委托给协调器线程，如果当前线程不是协调器线程，则在协调器线程中执行；否则直接执行。
     *
     * @param callable 要调用的 Callable。
     * @param errorMessage 错误信息。
     * @return 调用结果。
     */
    private <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
        // 确保分片分配在协调器线程中执行
        if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
            try {
                final Callable<V> guardedCallable =
                        () -> {
                            try {
                                return callable.call(); // 调用 Callable
                            } catch (Throwable t) {
                                LOG.error("Uncaught Exception in Source Coordinator Executor", t); // 记录未捕获的异常
                                ExceptionUtils.rethrowException(t);
                                return null;
                            }
                        };

                return coordinatorExecutor.submit(guardedCallable).get(); // 提交任务并等待结果
            } catch (InterruptedException | ExecutionException e) {
                throw new FlinkRuntimeException(errorMessage, e); // 如果发生异常，抛出运行时异常
            }
        }

        try {
            return callable.call(); // 直接调用 Callable
        } catch (Throwable t) {
            LOG.error("Uncaught Exception in Source Coordinator Executor", t); // 记录未捕获的异常
            throw new FlinkRuntimeException(errorMessage, t); // 抛出运行时异常
        }
    }

    private void assignSplitsToAttempts(SplitsAssignment<SplitT> assignment) {
        assignment.assignment().forEach((index, splits) -> assignSplitsToAttempts(index, splits)); // 向所有尝试分配分片
    }

    private void assignSplitsToAttempts(int subtaskIndex, List<SplitT> splits) {
        getRegisteredAttempts(subtaskIndex).forEach(
                attempt -> assignSplitsToAttempt(subtaskIndex, attempt, splits)); // 向每个尝试分配分片
    }

    private void assignSplitsToAttempt(int subtaskIndex, int attemptNumber, List<SplitT> splits) {
        checkAndLazyInitialize();
        if (splits.isEmpty()) {
            return; // 如果没有分片，直接返回
        }

        checkAttemptReaderReady(subtaskIndex, attemptNumber); // 检查尝试读者是否准备好

        final AddSplitEvent<SplitT> addSplitEvent;
        try {
            addSplitEvent = new AddSplitEvent<>(splits, splitSerializer); // 创建 AddSplitEvent
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to serialize splits.", e); // 如果序列化失败，抛出异常
        }

        final OperatorCoordinator.SubtaskGateway gateway =
                subtaskGateways.getGatewayAndCheckReady(subtaskIndex, attemptNumber); // 获取子任务网关
        gateway.sendEvent(addSplitEvent); // 向子任务发送分片
    }

    private void signalNoMoreSplitsToAttempts(int subtaskIndex) {
        getRegisteredAttempts(subtaskIndex)
                .forEach(
                        attemptNumber -> signalNoMoreSplitsToAttempt(subtaskIndex, attemptNumber)); // 向所有尝试发送无更多分片信号
    }

    private void signalNoMoreSplitsToAttempt(int subtaskIndex, int attemptNumber) {
        checkAndLazyInitialize();
        checkAttemptReaderReady(subtaskIndex, attemptNumber); // 检查尝试读者是否准备好

        final OperatorCoordinator.SubtaskGateway gateway =
                subtaskGateways.getGatewayAndCheckReady(subtaskIndex, attemptNumber); // 获取子任务网关
        gateway.sendEvent(new NoMoreSplitsEvent()); // 向子任务发送无更多分片信号
    }

    private void checkAttemptReaderReady(int subtaskIndex, int attemptNumber) {
        checkState(registeredReaders.containsKey(subtaskIndex)); // 检查子任务是否已注册
        checkState(getRegisteredAttempts(subtaskIndex).contains(attemptNumber)); // 检查尝试编号是否有效
    }

    private Set<Integer> getRegisteredAttempts(int subtaskIndex) {
        return registeredReaders.get(subtaskIndex).keySet(); // 获取已注册的尝试编号集合
    }

    private void sendCachedSplitsToNewlyRegisteredReader(int subtaskIndex, int attemptNumber) {
        // 对于批处理作业，检查点永远不会发生，因此 assignmentTracker 中的 un-checkpointed 分配可以被视为缓存的分片。
        // 对于流处理作业，supportsConcurrentExecutionAttempts 应该为 false，并且当新读者注册时，新读者未检查点的分配应该为空（当最后一个读者失败时被清除）。
        final LinkedHashSet<SplitT> cachedSplits =
                assignmentTracker.uncheckpointedAssignments().get(subtaskIndex); // 获取缓存的分片

        if (cachedSplits != null) {
            if (supportsConcurrentExecutionAttempts) {
                assignSplitsToAttempt(
                        subtaskIndex, attemptNumber, new ArrayList<>(cachedSplits)); // 向尝试分配缓存的分片
                if (hasNoMoreSplits(subtaskIndex)) {
                    signalNoMoreSplitsToAttempt(subtaskIndex, attemptNumber); // 发送无更多分片信号
                }
            } else {
                throw new IllegalStateException("No cached split is expected."); // 如果不支持并发执行尝试，抛出异常
            }
        }
    }

    /**
     * 返回 Source 是否正在处理积压数据。如果未通过 {@link #setIsProcessingBacklog} 方法设置，则返回 UNDEFINED。
     */
    public TernaryBoolean isBacklog() {
        return backlog; // 返回积压状态
    }

    /**
     * 维护不同执行尝试的子任务的网关。
     */
    private static class SubtaskGateways {
        private final Map<Integer, OperatorCoordinator.SubtaskGateway>[] gateways; // 存储子任务网关

        private SubtaskGateways(int parallelism) {
            gateways = new Map[parallelism]; // 初始化网关数组
            for (int i = 0; i < parallelism; i++) {
                gateways[i] = new HashMap<>(); // 初始化每个子任务的网关
            }
        }

        private void registerSubtaskGateway(OperatorCoordinator.SubtaskGateway gateway) {
            final int subtaskIndex = gateway.getSubtask(); // 获取子任务索引
            final int attemptNumber = gateway.getExecution().getAttemptNumber(); // 获取尝试编号

            checkState(
                    !gateways[subtaskIndex].containsKey(attemptNumber),
                    "Already have a subtask gateway for %s (#%s).",
                    subtaskIndex,
                    attemptNumber); // 检查是否已存在网关
            gateways[subtaskIndex].put(attemptNumber, gateway); // 注册子任务网关
        }

        private void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
            gateways[subtaskIndex].remove(attemptNumber); // 注销子任务网关
        }

        private OperatorCoordinator.SubtaskGateway getOnlyGatewayAndCheckReady(int subtaskIndex) {
            checkState(
                    gateways[subtaskIndex].size() > 0,
                    "Subtask %s is not ready yet to receive events.",
                    subtaskIndex); // 检查子任务是否准备好

            return Iterables.getOnlyElement(gateways[subtaskIndex].values()); // 获取唯一的网关
        }

        private OperatorCoordinator.SubtaskGateway getOnlyGatewayAndNotCheckReady(int subtaskIndex) {
            if (gateways[subtaskIndex].size() > 0) {
                return Iterables.getOnlyElement(gateways[subtaskIndex].values()); // 获取唯一的网关
            } else {
                return null; // 如果没有网关，返回 null
            }
        }

        private OperatorCoordinator.SubtaskGateway getGatewayAndCheckReady(
                int subtaskIndex, int attemptNumber) {
            final OperatorCoordinator.SubtaskGateway gateway =
                    gateways[subtaskIndex].get(attemptNumber); // 获取指定尝试的网关
            if (gateway != null) {
                return gateway; // 如果存在，返回网关
            }

            throw new IllegalStateException(
                    String.format(
                            "Subtask %d (#%d) is not ready yet to receive events.",
                            subtaskIndex, attemptNumber)); // 如果不存在，抛出异常
        }

        private void reset(int subtaskIndex) {
            gateways[subtaskIndex].clear(); // 重置子任务网关
        }
    }
}
