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
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * SourceReader 负责从 SourceSplit 中读取记录，并将其转换为 Flink 可处理的数据流。
 *
 * <p>在 Flink 中，每个 SourceReader 会由 TaskManager 的并行任务实例化，并从 SplitEnumerator
 * 处获取分片（Split）。然后，它会逐步读取数据并发送到下游算子。</p>
 *
 * <p>Flink 提供了 {@link org.apache.flink.connector.base.source.reader.SourceReaderBase}
 * 作为基础实现，它提供了一种高效的数据交接协议，避免了在任务线程中执行阻塞 I/O，并支持多种
 * 分片处理模式（如单线程读取、多线程并行读取等）。</p>
 *
 * <p>实现类可以提供如下指标（Metrics）：</p>
 * <ul>
 *     <li>{@link OperatorIOMetricGroup#getNumRecordsInCounter()}（强烈推荐，记录输入的记录数）</li>
 *     <li>{@link OperatorIOMetricGroup#getNumBytesInCounter()}（推荐，记录输入的字节数）</li>
 *     <li>{@link SourceReaderMetricGroup#getNumRecordsInErrorsCounter()}（推荐，记录错误记录数）</li>
 *     <li>{@link SourceReaderMetricGroup#setPendingRecordsGauge(Gauge)}（挂起的记录数指标）</li>
 *     <li>{@link SourceReaderMetricGroup#setPendingBytesGauge(Gauge)}（挂起的字节数指标）</li>
 * </ul>
 *
 * @param <T>       此 SourceReader 生成的记录类型。
 * @param <SplitT>  处理的 SourceSplit 类型，必须继承 {@link SourceSplit}。
 */
@Public
public interface SourceReader<T, SplitT extends SourceSplit>
        extends AutoCloseable, CheckpointListener {

    /** 启动 SourceReader，通常用于初始化资源或启动异步数据读取线程。 */
    void start();

    /**
     * 轮询（poll）获取下一个可用的记录，并将其输出到 {@link ReaderOutput}。
     *
     * <p>该方法必须是 **非阻塞** 的。如果没有可用数据，应返回 {@link InputStatus#NOTHING_AVAILABLE}，
     * 让 Flink 运行时决定何时再次调用该方法。</p>
     *
     * <p>虽然该方法可以一次性输出多个记录，但更推荐一次输出 **单条记录**，然后返回
     * {@link InputStatus#MORE_AVAILABLE}，以让调用线程知道仍有更多数据可读取。</p>
     *
     * @param output 记录输出接口，SourceReader 应向其中写入数据。
     * @return 当前 SourceReader 的输入状态，决定了后续行为：
     *     <ul>
     *         <li>{@link InputStatus#MORE_AVAILABLE} - 仍有更多数据可读，Flink 会立即再次调用该方法。</li>
     *         <li>{@link InputStatus#NOTHING_AVAILABLE} - 当前暂无数据可读，Flink 会等待 {@link #isAvailable()} 变为可用。</li>
     *         <li>{@link InputStatus#END_OF_INPUT} - 所有输入数据已读取完毕，Flink 将关闭此 SourceReader。</li>
     *     </ul>
     * @throws Exception 读取过程中可能出现的异常，异常会触发任务失败及重启机制。
     */
    InputStatus pollNext(ReaderOutput<T> output) throws Exception;

    /**
     * 生成当前 SourceReader 的 checkpoint 状态。
     * <p>Flink 在进行 checkpoint 时会调用该方法，将当前正在读取的分片状态进行持久化，
     * 以便任务恢复时能继续读取未完成的数据。</p>
     *
     * @param checkpointId  当前 checkpoint 的 ID。
     * @return  当前 SourceReader 处理的所有分片的快照信息。
     */
    List<SplitT> snapshotState(long checkpointId);

    /**
     * 返回一个 Future，当数据可用时该 Future 变为完成状态。
     *
     * <p>Flink 运行时会不断调用该方法，并在 Future 完成时调用 {@link #pollNext(ReaderOutput)}
     * 方法获取数据。如果 Future 长时间未完成，Flink 任务将进入空闲状态。</p>
     *
     * <p>该方法的正确性约定如下：
     * <ul>
     *     <li>如果 SourceReader 仍有可用数据，则之前返回的所有 Future **必须最终完成**，否则任务可能会无限挂起。</li>
     *     <li>如果 SourceReader 确实无数据可读，不应直接返回已完成的 Future，否则会导致高频轮询，造成 CPU 过载。</li>
     * </ul>
     * </p>
     *
     * @return 当有可用记录时变为完成状态的 Future。
     */
    CompletableFuture<Void> isAvailable();

    /**
     * 由 SplitEnumerator 负责调用，将新分配的 Split 添加到当前 Reader 进行读取。
     *
     * @param splits 新分配的 Split 列表。
     */
    void addSplits(List<SplitT> splits);

    /**
     * 通知当前 Reader 不会再收到新的 Split 任务。
     *
     * <p>当 SplitEnumerator 调用 {@link SplitEnumeratorContext#signalNoMoreSplits(int)}
     * 时，Flink 运行时会调用该方法，表示该 Reader 任务可以执行收尾操作并准备终止。</p>
     */
    void notifyNoMoreSplits();

    /**
     * 处理 SplitEnumerator 发送的自定义事件。
     *
     * <p>SplitEnumerator 可以通过 {@link SplitEnumeratorContext#sendEventToSourceReader(int, SourceEvent)}
     * 向 SourceReader 发送 SourceEvent 事件。本方法用于接收并处理这些事件。</p>
     *
     * <p>大多数 Source 并不需要特殊的自定义事件，因此此方法默认实现为空。</p>
     *
     * @param sourceEvent SplitEnumerator 发送的事件。
     */
    default void handleSourceEvents(SourceEvent sourceEvent) {}

    /**
     * 当一个 Checkpoint 完成时，通知 SourceReader。
     *
     * <p>默认实现为空，通常 SourceReader 不需要处理 Checkpoint 完成的事件。</p>
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /**
     * 用于暂停或恢复指定的分片读取。
     *
     * <p>此方法的主要用途是 **对齐水位线（watermark alignment）**，当 Flink 需要对不同的
     * SourceSplit 进行时间对齐时，可以通过此方法暂停某些进度过快的 Split，并恢复进度较慢的 Split。</p>
     *
     * <p>默认实现抛出 {@link UnsupportedOperationException}，未来的 Flink 版本可能会移除该默认实现。
     * 因此建议所有 SourceReader 实现类都应覆盖该方法。</p>
     *
     * @param splitsToPause  需要暂停的 Split ID 集合。
     * @param splitsToResume 需要恢复的 Split ID 集合。
     */
    @PublicEvolving
    default void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        throw new UnsupportedOperationException(
                "此 SourceReader 不支持暂停或恢复分片，这可能会导致水位线未对齐。\n"
                        + "未对齐的分片可能导致水位线偏移超出允许范围，进而影响事件时间窗口计算。\n"
                        + "强烈建议实现此方法，以支持流式数据的水位线对齐。\n"
                        + "如需继续使用未对齐的分片，可以在 Flink 配置中设置\n"
                        + "pipeline.watermark-alignment.allow-unaligned-source-splits = true。\n"
                        + "但请注意，未来 Flink 版本可能会移除该配置选项。");
    }
}

