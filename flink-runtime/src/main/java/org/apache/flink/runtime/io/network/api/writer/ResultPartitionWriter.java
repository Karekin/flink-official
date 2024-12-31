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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * 面向记录的运行时结果写入接口，用于生成结果。
 *
 * <p>如果在调用 {@link ResultPartitionWriter#fail(Throwable)} 或 {@link ResultPartitionWriter#finish()} 之前调用了 {@link ResultPartitionWriter#close()}，
 * 则会突然触发生产的失败和取消。在这种情况下，仍需要调用 {@link ResultPartitionWriter#fail(Throwable)}，以便完全释放与分区相关的所有资源，
 * 并在可能的情况下将失败原因传播给消费者。
 */
public interface ResultPartitionWriter extends AutoCloseable, AvailabilityProvider {

    /**
     * 设置分区，这通常是一个与创建相比较重的阻塞操作。
     */
    void setup() throws IOException;

    /**
     * 获取分区的 ID。
     */
    ResultPartitionID getPartitionId();

    /**
     * 获取子分区的数量。
     */
    int getNumberOfSubpartitions();

    /**
     * 获取目标键组的数量。
     */
    int getNumTargetKeyGroups();

    /**
     * 设置每个网关的最大透支缓冲区大小。
     */
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /**
     * 将给定的序列化记录写入目标子分区。
     * @param record 序列化记录
     * @param targetSubpartition 目标子分区索引
     * @throws IOException IO异常
     */
    void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException;

    /**
     * 将给定的序列化记录写入所有子分区。
     * <p>这种方法相比逐一写入每个子分区可以有更好的性能，因为底层实现可以进行优化，
     * 比如只需要将序列化记录复制一次到一个共享通道，所有子分区都可以消费。
     * @param record 序列化记录
     * @throws IOException IO异常
     */
    void broadcastRecord(ByteBuffer record) throws IOException;

    /**
     * 将给定的 {@link AbstractEvent} 广播到所有子分区。
     * @param event 要广播的事件
     * @param isPriorityEvent 是否为优先事件
     * @throws IOException IO异常
     */
    void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException;

    /**
     * 将对齐的屏障超时转为非对齐的屏障。
     * @param checkpointId 检查点ID
     * @throws IOException IO异常
     */
    void alignedBarrierTimeout(long checkpointId) throws IOException;

    /**
     * 中止检查点。
     * @param checkpointId 检查点ID
     * @param cause 中止原因
     */
    void abortCheckpoint(long checkpointId, CheckpointException cause);

    /**
     * 通知下游任务，此 {@code ResultPartitionWriter} 已经发出了所有的用户记录。
     * @param mode 指定是否需要刷新所有记录（例如在停止并保存点的情况下为 false）。
     * @throws IOException IO异常
     */
    void notifyEndOfData(StopMode mode) throws IOException;

    /**
     * 获取一个 future，用于指示所有记录是否已被下游任务处理。
     */
    CompletableFuture<Void> getAllDataProcessedFuture();

    /**
     * 为 {@link ResultPartitionWriter} 设置度量组。
     * @param metrics 度量组
     */
    void setMetricGroup(TaskIOMetricGroup metrics);

    /**
     * 为具有给定索引范围的子分区创建一个读取器。
     * @param indexSet 子分区索引集合
     * @param availabilityListener 可用性监听器
     * @throws IOException IO异常
     */
    ResultSubpartitionView createSubpartitionView(
            ResultSubpartitionIndexSet indexSet, BufferAvailabilityListener availabilityListener)
            throws IOException;

    /**
     * 手动触发所有子分区的数据消费。
     */
    void flushAll();

    /**
     * 手动触发指定子分区的数据消费。
     * @param subpartitionIndex 子分区索引
     */
    void flush(int subpartitionIndex);

    /**
     * 使分区的生产失败。
     * <p>此方法会尽最大努力将非 {@code null} 的失败原因传播给消费者。
     * 同时，此调用也会释放与分区相关的所有资源。如果尚未关闭分区，仍需要之后进行关闭操作。
     * @param throwable 失败原因
     */
    void fail(@Nullable Throwable throwable);

    /**
     * 成功完成分区的生产。
     * <p>之后仍需关闭分区。
     * @throws IOException IO异常
     */
    void finish() throws IOException;

    /**
     * 检查分区是否已完成。
     * @return 如果已完成则返回 true
     */
    boolean isFinished();

    /**
     * 释放分区写入器，释放生产的数据，不允许任何读取器再消费分区。
     * @param cause 释放的原因
     */
    void release(Throwable cause);

    /**
     * 检查分区是否已释放。
     * @return 如果已释放则返回 true
     */
    boolean isReleased();

    /**
     * 关闭分区写入器，释放分配的资源，例如缓冲池。
     * @throws Exception 异常
     */
    void close() throws Exception;
}
