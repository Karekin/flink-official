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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 修改 {@link SplitReader} 中分片的暂停状态的任务。
 *
 * <p>该任务默认在 {@link SplitFetcherManager} 中使用，并假设一个 {@link SplitFetcher}
 * 可以管理多个分片。如果 {@code SplitFetcher} 只管理单个分片，建议通过继承 {@link SplitFetcherManager}
 * 的方式暂停整个 {@code SplitFetcher}。
 *
 * @param <SplitT> 分片的类型，必须继承自 {@link SourceSplit}。
 */
@Internal
class PauseOrResumeSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {

    private static final Logger LOG = LoggerFactory.getLogger(PauseOrResumeSplitsTask.class);

    // 分片读取器，用于执行分片的暂停或恢复操作
    private final SplitReader<?, SplitT> splitReader;

    // 需要暂停的分片集合
    private final Collection<SplitT> splitsToPause;

    // 需要恢复的分片集合
    private final Collection<SplitT> splitsToResume;

    // 是否允许未对齐的源分片
    private final boolean allowUnalignedSourceSplits;

    /**
     * 构造函数。
     *
     * @param splitReader 分片读取器，用于管理分片的读取。
     * @param splitsToPause 要暂停的分片集合。
     * @param splitsToResume 要恢复的分片集合。
     * @param allowUnalignedSourceSplits 是否允许未对齐的源分片。如果为 true，则忽略不支持的操作。
     */
    PauseOrResumeSplitsTask(
            SplitReader<?, SplitT> splitReader,
            Collection<SplitT> splitsToPause,
            Collection<SplitT> splitsToResume,
            boolean allowUnalignedSourceSplits) {
        this.splitReader = checkNotNull(splitReader, "splitReader 不能为 null");
        this.splitsToPause = checkNotNull(splitsToPause, "splitsToPause 不能为 null");
        this.splitsToResume = checkNotNull(splitsToResume, "splitsToResume 不能为 null");
        this.allowUnalignedSourceSplits = allowUnalignedSourceSplits;
    }

    /**
     * 执行分片的暂停或恢复逻辑。
     *
     * <p>调用 {@link SplitReader#pauseOrResumeSplits} 方法暂停或恢复指定的分片。
     * 如果分片读取器不支持该操作，且未允许未对齐的源分片，则抛出 {@link UnsupportedOperationException}。
     *
     * @return 始终返回 true，表示任务成功完成。
     * @throws IOException 如果发生 I/O 错误。
     * @throws UnsupportedOperationException 如果不支持暂停或恢复分片，且未允许未对齐的分片。
     */
    @Override
    public boolean run() throws IOException {
        try {
            splitReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
        } catch (UnsupportedOperationException e) {
            if (!allowUnalignedSourceSplits) {
                throw e; // 抛出异常，因为未允许未对齐的源分片
            }
            // 如果允许未对齐分片，则忽略异常
        }
        return true;
    }

    /**
     * 唤醒任务。此任务不需要唤醒逻辑，因此为空实现。
     */
    @Override
    public void wakeUp() {
        // 不执行任何操作
    }

    /**
     * 返回任务的字符串表示形式，包含暂停和恢复的分片信息。
     *
     * @return 任务的字符串表示形式。
     */
    @Override
    public String toString() {
        return "PauseOrResumeSplitsTask{"
                + "splitsToResume=" + splitsToResume
                + ", splitsToPause=" + splitsToPause
                + '}';
    }
}

