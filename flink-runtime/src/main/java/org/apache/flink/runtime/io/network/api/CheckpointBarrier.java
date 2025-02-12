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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * CheckpointBarrier（检查点屏障）用于在流处理拓扑中对齐检查点（Checkpoint）。
 * 源算子（Source）在 JobManager 指示执行 Checkpoint 时，会发出 CheckpointBarrier。
 * 当算子（Operator）在其输入通道上接收到 CheckpointBarrier 时，它会将该点视为
 * Checkpoint 之前的数据（pre-checkpoint）和 Checkpoint 之后的数据（post-checkpoint）的分界点。
 *
 * <p>当算子从其所有输入通道接收到 CheckpointBarrier 后，它就知道当前检查点已完成，
 * 这时它可以触发算子的特定 Checkpoint 处理逻辑，并将 Barrier 继续广播到下游算子。
 *
 * <p>根据语义保证（例如 Exactly Once 语义），可能会在 Checkpoint 完成之前阻止后续数据的处理。
 *
 * <p>CheckpointBarrier 的 ID（检查点编号）是严格递增的，以保证每个 Checkpoint 都是有序的。
 */
public class CheckpointBarrier extends RuntimeEvent {

    /** Checkpoint 的唯一 ID，每个 Checkpoint 递增 */
    private final long id;

    /** 触发 Checkpoint 的时间戳 */
    private final long timestamp;

    /** Checkpoint 相关的配置信息，如对齐方式（aligned/unaligned）、是否是 Savepoint 等 */
    private final CheckpointOptions checkpointOptions;

    /**
     * 构造函数，初始化 CheckpointBarrier
     *
     * @param id Checkpoint ID
     * @param timestamp Checkpoint 触发的时间戳
     * @param checkpointOptions Checkpoint 选项配置
     */
    public CheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) {
        this.id = id;
        this.timestamp = timestamp;
        this.checkpointOptions = checkNotNull(checkpointOptions);
    }

    /**
     * 获取 Checkpoint ID
     *
     * @return Checkpoint ID
     */
    public long getId() {
        return id;
    }

    /**
     * 获取 Checkpoint 触发时间戳
     *
     * @return Checkpoint 触发的时间戳
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * 获取 Checkpoint 选项配置
     *
     * @return CheckpointOptions 对象
     */
    public CheckpointOptions getCheckpointOptions() {
        return checkpointOptions;
    }

    /**
     * 创建一个新的 CheckpointBarrier，并修改 CheckpointOptions
     * 该方法用于在已有 Barrier 的基础上修改 Checkpoint 选项，而不影响原始对象
     *
     * @param checkpointOptions 新的 Checkpoint 选项
     * @return 修改后的 CheckpointBarrier，如果选项未变，则返回当前对象
     */
    public CheckpointBarrier withOptions(CheckpointOptions checkpointOptions) {
        return this.checkpointOptions == checkpointOptions
                ? this
                : new CheckpointBarrier(id, timestamp, checkpointOptions);
    }

    // ------------------------------------------------------------------------
    // 序列化相关方法
    // ------------------------------------------------------------------------

    /**
     * 由于 CheckpointBarrier 事件的序列化是在 EventSerializer 类中特殊处理的，
     * 因此这里不需要实现具体的序列化方法。如果该方法被调用，会抛出异常。
     *
     * @param out 数据输出流
     * @throws IOException 抛出异常，表示该方法不应被调用
     */
    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    /**
     * 由于 CheckpointBarrier 事件的反序列化是在 EventSerializer 类中特殊处理的，
     * 因此这里不需要实现具体的反序列化方法。如果该方法被调用，会抛出异常。
     *
     * @param in 数据输入流
     * @throws IOException 抛出异常，表示该方法不应被调用
     */
    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    /**
     * 计算对象的哈希值
     *
     * @return 计算得到的哈希值
     */
    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32) ^ timestamp ^ (timestamp >>> 32));
    }

    /**
     * 比较两个 CheckpointBarrier 是否相等
     *
     * @param other 需要比较的对象
     * @return 如果对象相同返回 true，否则返回 false
     */
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != CheckpointBarrier.class) {
            return false;
        } else {
            CheckpointBarrier that = (CheckpointBarrier) other;
            return that.id == this.id
                    && that.timestamp == this.timestamp
                    && this.checkpointOptions.equals(that.checkpointOptions);
        }
    }

    /**
     * 生成字符串表示形式
     *
     * @return 该对象的字符串表示
     */
    @Override
    public String toString() {
        return String.format(
                "CheckpointBarrier %d @ %d Options: %s", id, timestamp, checkpointOptions);
    }

    /**
     * 判断当前 Barrier 是否用于 Checkpoint（而非 Savepoint）
     *
     * @return 如果是 Checkpoint，返回 true，否则返回 false
     */
    public boolean isCheckpoint() {
        return !checkpointOptions.getCheckpointType().isSavepoint();
    }

    /**
     * 生成一个新的 Unaligned CheckpointBarrier
     * 如果当前 Barrier 已经是 Unaligned，则返回自身，否则创建一个新的 Barrier
     *
     * @return 具有 Unaligned Checkpoint 选项的新 Barrier
     */
    public CheckpointBarrier asUnaligned() {
        return checkpointOptions.isUnalignedCheckpoint()
                ? this
                : new CheckpointBarrier(
                getId(), getTimestamp(), getCheckpointOptions().toUnaligned());
    }
}

