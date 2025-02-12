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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * SplitEnumeratorContext 是一个接口，提供 Flink 运行时的上下文信息，
 * 用于管理 SplitEnumerator 的分片（Split）分配、任务调度及 SourceReader 交互。
 *
 * <p>主要作用：
 * <ul>
 *     <li>1. 维护 SplitEnumerator 运行时的任务信息（任务并行度、已注册 Readers）。</li>
 *     <li>2. 允许 SplitEnumerator 发送事件给 SourceReader。</li>
 *     <li>3. 管理 Split 分配，确保 Split 被合理分配给不同的 SourceReader。</li>
 *     <li>4. 提供异步任务执行（callAsync），用于分布式环境下的任务调度。</li>
 *     <li>5. 允许在 SourceCoordinator 线程中执行任务，避免并发问题。</li>
 * </ul>
 *
 * @param <SplitT> 代表数据分片（Split）的类型，必须继承自 {@link SourceSplit}。
 */
@Public
public interface SplitEnumeratorContext<SplitT extends SourceSplit> {

    /**
     * 获取 SplitEnumerator 相关的度量指标组（Metric Group）。
     *
     * <p>Flink 允许 SplitEnumerator 组件暴露一些内部状态和统计数据，例如：
     * <ul>
     *     <li>已分配的 Split 数量</li>
     *     <li>待分配的 Split 数量</li>
     *     <li>数据吞吐量</li>
     * </ul>
     *
     * @return SplitEnumerator 组件的指标信息，用于 Flink 任务监控。
     */
    SplitEnumeratorMetricGroup metricGroup();

    /**
     * 发送自定义事件到指定 SourceReader。
     *
     * <p>该方法允许 SplitEnumerator 主动与 SourceReader 进行通信，
     * 例如：
     * <ul>
     *     <li>通知 SourceReader 进行特定操作（如 Split 更新）。</li>
     *     <li>协调多个 SourceReader 之间的数据分配。</li>
     * </ul>
     *
     * @param subtaskId 目标 SourceReader 的 Subtask ID。
     * @param event 需要发送的事件。
     */
    void sendEventToSourceReader(int subtaskId, SourceEvent event);

    /**
     * 发送自定义事件到指定 SourceReader（包含执行尝试编号）。
     *
     * <p>如果启用了推测执行（Speculative Execution），Flink 允许多个执行尝试并行运行，
     * 此方法可以指定具体的执行尝试 ID，确保事件发送到正确的任务实例。
     *
     * <p>如果不支持推测执行，可以使用 {@link #sendEventToSourceReader(int, SourceEvent)} 方法。</p>
     *
     * @param subtaskId 目标 SourceReader 的 Subtask ID。
     * @param attemptNumber 执行尝试编号（Execution Attempt）。
     * @param event 需要发送的事件。
     */
    default void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取当前 Source 任务的并行度（Parallelism）。
     *
     * <p>注意：由于 Flink 可能会自动扩缩容，任务的并行度可能会随时间变化。
     * SplitEnumerator 不应缓存该值，而应始终调用该方法获取最新的并行度。</p>
     *
     * @return 当前 Source 任务的并行度。
     */
    int currentParallelism();

    /**
     * 获取当前已注册的 SourceReader。
     *
     * <p>返回的 Map 映射：
     * <ul>
     *     <li>Key: Subtask ID（Flink 任务实例 ID）。</li>
     *     <li>Value: ReaderInfo（任务运行的元信息，如主机名、线程 ID）。</li>
     * </ul>
     *
     * <p>注意：如果同一个任务有多个执行尝试（Speculative Execution），
     * 该方法默认返回最早启动的任务实例。</p>
     *
     * @return 当前已注册的 SourceReader 信息。
     */
    Map<Integer, ReaderInfo> registeredReaders();

    /**
     * 获取所有执行尝试的 SourceReader 信息。
     *
     * <p>如果一个 Subtask 存在多个执行尝试，该方法返回所有尝试的详细信息。</p>
     *
     * @return 已注册的 SourceReader，包含所有执行尝试的信息。
     */
    default Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
        throw new UnsupportedOperationException();
    }

    /**
     * 分配多个 Split 到指定的 SourceReader。
     *
     * <p>该方法支持批量分配，避免多个单独的 Split 分配请求导致的额外开销。</p>
     *
     * @param newSplitAssignments 需要分配的 Split 任务。
     */
    void assignSplits(SplitsAssignment<SplitT> newSplitAssignments);

    /**
     * 分配单个 Split 给 SourceReader。
     *
     * <p>对于多个 Split，建议使用 {@link #assignSplits(SplitsAssignment)} 批量分配，以提高性能。</p>
     *
     * @param split 需要分配的 Split。
     * @param subtask 目标 Subtask ID。
     */
    default void assignSplit(SplitT split, int subtask) {
        assignSplits(new SplitsAssignment<>(split, subtask));
    }

    /**
     * 发送信号给 SourceReader，表示不会再有新的 Split 需要分配。
     *
     * <p>当 Flink 任务进入终止状态，或者 SplitEnumerator 确认所有数据已分配完成时，
     * 可以调用此方法通知 SourceReader 不会再接收到新的 Split。</p>
     *
     * @param subtask 目标 Subtask ID。
     */
    void signalNoMoreSplits(int subtask);

    /**
     * 提交一个异步任务，并在任务完成后交给指定的回调处理。
     *
     * <p>该方法适用于长时间运行的任务，如：
     * <ul>
     *     <li>查询远程存储服务获取 Split 信息。</li>
     *     <li>访问外部系统检查任务状态。</li>
     * </ul>
     *
     * @param callable 需要执行的任务。
     * @param handler 任务完成后的回调处理器，包含返回结果或异常。
     * @param <T> 任务的返回类型。
     */
    <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler);

    /**
     * 提交一个定期执行的异步任务，并在任务完成后交给指定的回调处理。
     *
     * <p>该方法适用于需要定期运行的任务，例如：
     * <ul>
     *     <li>定期检查 Kafka 主题是否有新的分区。</li>
     *     <li>定期与外部存储服务同步数据。</li>
     * </ul>
     *
     * @param callable 需要执行的任务。
     * @param handler 任务完成后的回调处理器。
     * @param initialDelayMillis 任务的初始延迟时间（毫秒）。
     * @param periodMillis 任务执行的时间间隔（毫秒）。
     * @param <T> 任务的返回类型。
     */
    <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelayMillis,
            long periodMillis);

    /**
     * 在 SourceCoordinator 线程中执行一个任务。
     *
     * <p>用于协调多个 Source 任务时，确保所有任务都在单一线程中执行，避免并发问题。</p>
     *
     * @param runnable 需要执行的任务。
     */
    void runInCoordinatorThread(Runnable runnable);

    /**
     * 设置当前 Source 是否在处理 Backlog（积压数据）。
     *
     * <p>当 Source 处理的是历史数据（如 Kafka 旧日志、过期文件数据），
     * Flink 可以调整调度策略，提高吞吐量而非优先考虑低延迟。</p>
     *
     * @param isProcessingBacklog 是否正在处理积压数据。
     */
    @PublicEvolving
    default void setIsProcessingBacklog(boolean isProcessingBacklog) {
        throw new UnsupportedOperationException();
    }
}

