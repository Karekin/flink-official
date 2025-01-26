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

package org.apache.flink.connector.base.source.reader.splitreader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import java.io.IOException;
import java.util.Collection;

/**
 * 一个接口，用于从分片中读取数据。实现类可以从单个分片或多个分片中读取数据。
 *
 * @param <E> 数据元素的类型。
 * @param <SplitT> 分片的类型，必须继承自 {@link SourceSplit}。
 */
@PublicEvolving
public interface SplitReader<E, SplitT extends SourceSplit> extends AutoCloseable {

    /**
     * 从指定的分片中读取数据元素到阻塞队列中。该方法可能是阻塞的，但当调用 {@link #wakeUp()} 时应能解除阻塞。
     *
     * <p>在被唤醒的情况下，具体实现可以选择返回而不抛出异常，或者直接抛出中断异常。
     * 无论如何，该方法应具有可重入性，这意味着下一次调用 `fetch` 时应能从上次被唤醒或中断的位置继续读取。
     *
     * <p>实现类可以选择一次性读取完整的分片记录，也可以在达到某个阈值时停止读取。
     * 如果后者发生，那么再次调用 `fetch` 时应能从停止的记录位置继续读取，基于 `SplitState` 恢复状态。
     *
     * @return 已完成分片的 ID 列表。
     * @throws IOException 如果遇到 I/O 错误，例如反序列化失败。
     */
    RecordsWithSplitIds<E> fetch() throws IOException;

    /**
     * 处理分片的更改请求。此方法应为非阻塞操作。
     *
     * <p>为了保证 {@link SourceReaderBase} 内部状态的一致性，如果分片无效（例如，分片中没有任何记录），
     * 应将其作为完成的分片放回 {@link RecordsWithSplitIds}，以便 {@link SourceReaderBase} 能够清理为该分片创建的资源。
     *
     * <p>类似地，如果分片被移除，也应将其作为完成的分片放回 {@link RecordsWithSplitIds}，以便清理相关资源。
     *
     * @param splitsChanges 分片的更改信息，SplitReader 需要处理这些更改。
     */
    void handleSplitsChanges(SplitsChange<SplitT> splitsChanges);

    /**
     * 唤醒分片读取器，以防抓取线程在 {@link #fetch()} 中被阻塞。
     */
    void wakeUp();

    /**
     * 暂停或恢复单个分片的读取。
     *
     * <p>注意，此方法在没有并行调用其他方法的情况下执行，因此可以非原子地更新订阅状态。
     * 此方法为连接器提供了更表达式化的 API，允许一次性更新所有订阅状态。
     *
     * <p>目前，此方法主要用于对齐分片的水位线（watermarks），当使用水位线对齐且从多个分片读取时非常有用。
     *
     * <p>默认实现会抛出 {@link UnsupportedOperationException}，但未来的版本中将移除默认实现。
     * 为了兼容未来的版本，建议覆盖默认实现并实现此方法。
     *
     * @param splitsToPause 要暂停的分片列表。
     * @param splitsToResume 要恢复的分片列表。
     */
    default void pauseOrResumeSplits(
            Collection<SplitT> splitsToPause, Collection<SplitT> splitsToResume) {
        throw new UnsupportedOperationException(
                "该分片读取器不支持暂停或恢复分片操作，这可能导致分片之间的水位线未对齐。\n"
                        + "未对齐的分片是指分片的输出水位线超出允许限制。\n"
                        + "强烈建议不要使用未对齐的源分片，因为这会导致当每个读取器包含多个分片时水位线对齐不可预测。\n"
                        + "建议实现分片的暂停功能，以支持此功能。\n"
                        + "如果需要，可以通过设置配置参数 `pipeline.watermark-alignment.allow-unaligned-source-splits` 为 true\n"
                        + "来允许未对齐的源分片。但请注意，该配置参数将在未来的 Flink 版本中被移除。");
    }
}

