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
import org.apache.flink.api.common.state.CheckpointListener;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * SplitEnumerator 是一个接口，负责发现数据源中的分片（Split），
 * 并将其分配给 {@link SourceReader} 进行读取。
 *
 * <p>它的主要职责包括：
 * <ul>
 *     <li>1. 发现数据源中的 Split。</li>
 *     <li>2. 监听 SourceReader 的请求并分配 Split。</li>
 *     <li>3. 处理 SourceReader 失败后需要重新分配的 Split。</li>
 *     <li>4. 维护 Split 的状态，并在 Flink Checkpoint 时进行快照存储。</li>
 * </ul>
 *
 * @param <SplitT> 代表数据分片（Split）的类型，必须继承自 {@link SourceSplit}。
 * @param <CheckpointT> 代表 SplitEnumerator 在 Checkpoint 时存储的状态类型。
 */
@Public
public interface SplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        extends AutoCloseable, CheckpointListener {

    /**
     * 启动 SplitEnumerator。
     *
     * <p>该方法会在 SourceEnumerator 初始化后调用，通常用于执行启动逻辑。
     * 默认实现不执行任何操作，具体数据源可以根据需要重写此方法。</p>
     */
    void start();

    /**
     * 处理 SourceReader 发送的 Split 请求。
     *
     * <p>当 SourceReader 调用 {@link SourceReaderContext#sendSplitRequest()} 时，
     * 该方法会被调用，通常用于给 SourceReader 分配 Split。</p>
     *
     * @param subtaskId 发送请求的 SourceReader 的子任务 ID。
     * @param requesterHostname 可选参数，表示请求任务运行的主机名，可以用于进行本地性优化分配 Split。
     */
    void handleSplitRequest(int subtaskId, @Nullable String requesterHostname);

    /**
     * 当某个 SourceReader 失败后，需要将其已分配但尚未消费完的 Split 重新分配。
     *
     * <p>当 SourceReader 发生失败时，它上次成功 Checkpoint 之后分配的 Split 需要重新分配。
     * 该方法会将这些 Split 加回到 SplitEnumerator 进行重新分配。</p>
     *
     * @param splits 需要重新分配的 Split 列表。
     * @param subtaskId 发生故障的 SourceReader 的子任务 ID。
     */
    void addSplitsBack(List<SplitT> splits, int subtaskId);

    /**
     * 当新的 SourceReader 启动时，该方法会被调用。
     *
     * <p>当 Flink 任务扩容或恢复时，会新增 SourceReader，该方法可以用于给新任务分配 Split。</p>
     *
     * @param subtaskId 新启动的 SourceReader 的子任务 ID。
     */
    void addReader(int subtaskId);

    /**
     * 创建 SplitEnumerator 的快照状态，并存储到 Flink Checkpoint。
     *
     * <p>在 Flink Checkpoint 过程中，该方法会被调用，负责存储当前分片分配状态。
     * 在恢复任务时，可以使用该状态重新构建 SplitEnumerator。</p>
     *
     * <p>存储的状态应该只包含尚未分配的 Split，已分配的 Split 在恢复后由各个 SourceReader 继续处理。</p>
     *
     * @param checkpointId Flink Checkpoint ID。
     * @return 返回存储的 Checkpoint 状态对象。
     * @throws Exception 如果无法创建快照，则抛出异常。
     */
    CheckpointT snapshotState(long checkpointId) throws Exception;

    /**
     * 关闭 SplitEnumerator，释放相关资源（例如线程、网络连接等）。
     *
     * <p>该方法通常在 Job 取消、Failover 或终止时调用，以确保资源被正确释放。</p>
     *
     * @throws IOException 关闭过程中可能抛出的 IO 异常。
     */
    @Override
    void close() throws IOException;

    /**
     * 当 Flink Checkpoint 成功完成时，该方法会被调用。
     *
     * <p>默认实现为空，因为大多数 SourceEnumerator 不需要额外处理 Checkpoint 完成事件。
     * 如果数据源需要与外部系统进行 Checkpoint 相关的交互（如 Kafka、Pulsar），
     * 可以重写此方法以执行相应逻辑。</p>
     *
     * @param checkpointId 成功完成的 Checkpoint ID。
     * @throws Exception 处理 Checkpoint 完成事件时可能抛出的异常。
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /**
     * 处理 SourceReader 发送的自定义事件。
     *
     * <p>该方法提供了一种机制，允许 SourceReader 发送自定义事件给 SplitEnumerator。
     * 例如，一些特殊数据源可能需要在 Reader 和 Enumerator 之间进行自定义事件通信。
     *
     * <p>默认实现为空，只有需要额外交互的 Source 才需要实现该方法。</p>
     *
     * @param subtaskId 发送事件的 SourceReader 子任务 ID。
     * @param sourceEvent 具体的自定义事件。
     */
    default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}
}

