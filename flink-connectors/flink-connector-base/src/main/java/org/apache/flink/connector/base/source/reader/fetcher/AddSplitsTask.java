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
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import java.util.List;
import java.util.Map;

/**
 * 一个任务类，用于向分片读取器中添加新的分片。
 *
 * @param <SplitT> 分片的类型，必须继承自 {@link SourceSplit}。
 */
@Internal
class AddSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {

    // 分片读取器，用于处理分片的添加操作
    private final SplitReader<?, SplitT> splitReader;

    // 待添加的分片列表
    private final List<SplitT> splitsToAdd;

    // 当前已分配的分片映射（分片ID -> 分片）
    private final Map<String, SplitT> assignedSplits;

    /**
     * 构造函数。
     *
     * @param splitReader 分片读取器，用于管理分片的读取。
     * @param splitsToAdd 要添加的分片列表。
     * @param assignedSplits 当前已分配的分片映射，用于记录分片的分配状态。
     */
    AddSplitsTask(
            SplitReader<?, SplitT> splitReader,
            List<SplitT> splitsToAdd,
            Map<String, SplitT> assignedSplits) {
        this.splitReader = splitReader;
        this.splitsToAdd = splitsToAdd;
        this.assignedSplits = assignedSplits;
    }

    /**
     * 执行添加分片的任务逻辑。
     *
     * <p>将 {@code splitsToAdd} 中的每个分片添加到 {@code assignedSplits} 映射中，
     * 并调用分片读取器的 {@link SplitReader#handleSplitsChanges} 方法通知分片的更改。
     *
     * @return 总是返回 true，表示任务成功完成。
     */
    @Override
    public boolean run() {
        // 将待添加的分片逐一放入已分配的分片映射中
        for (SplitT s : splitsToAdd) {
            assignedSplits.put(s.splitId(), s);
        }

        // 通知分片读取器处理分片的添加
        splitReader.handleSplitsChanges(new SplitsAddition<>(splitsToAdd));
        return true;
    }

    /**
     * 唤醒任务（此任务不需要唤醒逻辑，故为空实现）。
     */
    @Override
    public void wakeUp() {
        // 此任务不需要唤醒逻辑，因此不做任何处理
    }

    /**
     * 返回任务的字符串表示形式。
     *
     * @return 包含待添加分片信息的字符串。
     */
    @Override
    public String toString() {
        return String.format("AddSplitsTask: [%s]", splitsToAdd);
    }
}
