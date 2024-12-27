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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 一个 {@link ResultPartition} 的实现类，直接将缓冲区数据写入到对应的 {@link ResultSubpartition}。
 * 这与基于联合结构的实现（例如基于排序的分区）不同，后者是在写入阶段完成后，
 * 子分区从联合结构中提取数据。
 *
 * <p>注意：在读取阶段，所有子分区都会返回缓冲区及其积压数据（backlog），
 * 用于通过网络传输。
 */
public abstract class BufferWritingResultPartition extends ResultPartition {

    /** 当前分区中的子分区数组。至少包含一个子分区。 */
    protected final ResultSubpartition[] subpartitions;

    /**
     * 在非广播模式下，每个子分区会维护一个独立的 BufferBuilder。
     * 当没有分配缓冲区时，该字段可能为 null。
     */
    private final BufferBuilder[] unicastBufferBuilders;

    /** 在广播模式下，所有子分区共享一个 BufferBuilder。 */
    private BufferBuilder broadcastBufferBuilder;

    /** 用于记录硬性背压时间（每秒的毫秒数）。 */
    private TimerGauge hardBackPressuredTimeMsPerSecond = new TimerGauge();

    /** 总写入字节数。 */
    private long totalWrittenBytes;


    /**
     * 构造函数，初始化 BufferWritingResultPartition。
     *
     * @param owningTaskName     拥有此分区的任务名称。
     * @param partitionIndex     分区的索引号。
     * @param partitionId        分区的唯一标识符。
     * @param partitionType      分区类型（例如 BLOCKING、PIPELINED 等）。
     * @param subpartitions      分区的子分区数组。
     * @param numTargetKeyGroups 目标 key 组数量。
     * @param partitionManager   分区管理器，用于管理此分区。
     * @param bufferCompressor   可选的缓冲区压缩器，用于压缩数据。
     * @param bufferPoolFactory  提供缓冲池的工厂方法，支持抛出 IO 异常。
     */
    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        // 调用父类的构造函数进行初始化。
        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length, // 子分区的数量
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        // 检查子分区数组是否为空，并将其赋值给当前实例的字段。
        this.subpartitions = checkNotNull(subpartitions);

        // 初始化单播模式下的 BufferBuilder 数组，数组大小等于子分区的数量。
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }


    /**
     * 内部设置方法，用于检查和初始化缓冲池。
     * @throws IOException 如果缓冲池的配置不正确（例如缓冲区不足）。
     */
    @Override
    protected void setupInternal() throws IOException {
        // 检查缓冲池中是否有足够的保证缓冲区数量，至少要与子分区数量相等。
        checkState(
                bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "ResultPartition 设置逻辑中存在错误：缓冲池的保证缓冲区数量不足，"
                        + "无法支持当前分区。");
    }


    /**
     * 获取所有子分区中队列中缓冲区的总数。
     * @return 总的缓冲区数量。
     */
    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        // 遍历每个子分区，累计其未同步队列中的缓冲区数量。
        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }


    /**
     * 获取所有子分区队列中缓冲区的总大小（以字节为单位），
     * 仅计算未使用的部分。
     *
     * @return 未使用的缓冲区字节大小。
     */
    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        long totalNumberOfBytes = 0;

        // 遍历所有子分区，计算其缓冲区的字节数。
        for (ResultSubpartition subpartition : subpartitions) {
            totalNumberOfBytes += Math.max(0, subpartition.getTotalNumberOfBytesUnsafe());
        }

        // 用已写入的总字节数减去所有子分区的已使用字节数，得到未使用字节数。
        return totalWrittenBytes - totalNumberOfBytes;
    }


    /**
     * 获取指定子分区队列中的缓冲区数量。
     * @param targetSubpartition 目标子分区的索引。
     * @return 子分区中的缓冲区数量。
     */
    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        // 确保子分区索引在合法范围内。
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        // 返回指定子分区未同步队列中的缓冲区数量。
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }


    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 发射记录到指定的目标子分区。
     *
     * @param record            要发射的ByteBuffer记录
     * @param targetSubpartition 目标子分区的编号
     * @throws IOException      如果在发射记录过程中发生I/O错误
    */
    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        // 更新已写入的总字节数
        totalWrittenBytes += record.remaining();
        // 为新记录追加单播数据到BufferBuilder
        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);
        // 循环直到ByteBuffer中没有剩余数据
        while (record.hasRemaining()) {
            // full buffer, partial record
            // 如果当前BufferBuilder已满，且记录尚未发射完毕
            finishUnicastBufferBuilder(targetSubpartition);
            // 为记录的剩余部分追加到新的BufferBuilder
            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
        }
        // 如果BufferBuilder已满，且记录已完全发射
        if (buffer.isFull()) {
            // full buffer, full record
            finishUnicastBufferBuilder(targetSubpartition);
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        totalWrittenBytes += ((long) record.remaining() * numSubpartitions);

        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }

        // partial buffer, full record
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定的事件广播到所有子分区。
     *
     * 在广播之前，会检查当前是否处于生产状态，并完成广播和单播缓冲区的构建。
     *
     * @param event 要广播的事件对象，必须是AbstractEvent或其子类的实例
     * @param isPriorityEvent 指示该事件是否为优先事件的布尔值
     * @throws IOException 如果在序列化事件或将其添加到子分区时发生I/O异常，则抛出此异常
    */
    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        // 检查当前是否处于生产状态
        checkInProduceState();
        // 完成广播缓冲区构建器的构建 关闭broadcastBufferBuilder 设置为null
        finishBroadcastBufferBuilder();
        //完成单个子类缓冲区的设置unicastBufferBuilders
        finishUnicastBufferBuilders();
        // 使用事件序列化为BufferConsumer，以便将事件写入缓冲区
        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            // 将事件序列化到缓冲区后，更新总写入字节数（乘以子分区数量）
            totalWrittenBytes += ((long) eventBufferConsumer.getWrittenBytes() * numSubpartitions);
            // 遍历所有子分区
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each subpartition of
                // targetPartition
                // 复制事件缓冲区消费者中的缓冲区（可能是为了避免并发修改）
                // 并将其添加到子分区中，同时保留缓冲区以便子分区可以回收它
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.alignedBarrierTimeout(checkpointId);
        }
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        hardBackPressuredTimeMsPerSecond = metrics.getHardBackPressuredTimePerSecond();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的子分区索引和缓冲区可用性监听器，创建一个ResultSubpartitionView对象。
     * 这个视图对象表示一个结果子分区的读取视图。
     *
     * @param subpartitionIndex 子分区的索引
     * @param availabilityListener 缓冲区可用性监听器
     * @return ResultSubpartitionView对象，表示结果子分区的读取视图
     * @throws IOException 如果在创建子分区视图时发生I/O错误
     * @throws IndexOutOfBoundsException 如果提供的子分区索引超出范围
     * @throws IllegalStateException 如果该分区已经被释放
    */
    @Override
    protected ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        // 检查子分区索引是否在合法范围内
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        // 检查该分区是否已经被释放
        checkState(!isReleased(), "Partition released.");
        // 从数组中根据索引获取ResultSubpartition对象
        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        // 调用ResultSubpartition对象的createReadView方法来创建读取视图
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

        LOG.debug("Created {}", readView);
        // 返回创建的读取视图
        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            totalWrittenBytes += subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        super.close();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为新记录追加到指定的目标子分区对应的BufferBuilder。
     * 如果目标子分区对应的BufferBuilder不存在，则创建一个新的BufferBuilder。
     * @param record           要追加的ByteBuffer记录
     * @param targetSubpartition 目标子分区的编号
     * @return                 追加数据后的BufferBuilder
     * @throws IOException      如果发生I/O错误或目标子分区编号超出范围
    */
    private BufferBuilder appendUnicastDataForNewRecord(
            final ByteBuffer record, final int targetSubpartition) throws IOException {
        // 检查目标子分区编号是否有效
        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }
        // 获取目标子分区对应的BufferBuilder
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];
        // 如果BufferBuilder为空，则创建一个新的BufferBuilder
        if (buffer == null) {
            // 请求一个新的BufferBuilder
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            // 将新的BufferBuilder添加到目标子分区中，并设置其初始的序列号和记录长度
            // 同时构建BufferConsumer放入队列
            addToSubpartition(buffer, targetSubpartition, 0, record.remaining());
        }
        // 将ByteBuffer中的数据追加到BufferBuilder中
        append(record, buffer);
        // 返回追加数据后的BufferBuilder
        return buffer;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 数据追加到BufferBuilder
    */
    private int append(ByteBuffer record, BufferBuilder buffer) {
        // Try to avoid hard back-pressure in the subsequent calls to request buffers
        // by ignoring Buffer Debloater hints and extending the buffer if possible (trim).
        // This decreases the probability of hard back-pressure in cases when
        // the output size varies significantly and BD suggests too small values.
        // The hint will be re-applied on the next iteration.
        // 检查ByteBuffer中剩余的数据是否大于或等于BufferBuilder的可写字节数
        if (record.remaining() >= buffer.getWritableBytes()) {
            // This 2nd check is expensive, so it shouldn't be re-ordered.
            // However, it has the same cost as the subsequent call to request buffer, so it doesn't
            // affect the performance much.
            // 如果缓冲区池没有可用的缓冲区
            if (!bufferPool.isAvailable()) {
                // add 1 byte to prevent immediately flushing the buffer and potentially fit the
                // next record
                // 计算新的缓冲区大小，确保至少为buffer的最大容量，或者足够存储当前剩余数据和额外的一个字节
                int newSize =
                        buffer.getMaxCapacity()
                                + (record.remaining() - buffer.getWritableBytes())
                                + 1;
                // 尝试扩展缓冲区的大小，但不超过其最大容量
                buffer.trim(Math.max(buffer.getMaxCapacity(), newSize));
            }
        }
        // 将ByteBuffer中的数据追加到BufferBuilder，并提交更改
        // 返回实际追加的字节数
        return buffer.appendAndCommit(record);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将BufferBuilder中的数据添加到指定的目标子分区，并根据需要调整BufferBuilder的大小。
     *
     * @param buffer               要添加的BufferBuilder
     * @param targetSubpartition    目标子分区的编号
     * @param partialRecordLength    已写入BufferBuilder的部分记录长度
     * @param minDesirableBufferSize 期望的最小缓冲区大小
     * @throws IOException           如果在添加数据或调整缓冲区大小时发生I/O错误
    */
    private void addToSubpartition(
            BufferBuilder buffer,
            int targetSubpartition,
            int partialRecordLength,
            int minDesirableBufferSize)
            throws IOException {
        // 调用目标子分区的add方法，传入BufferBuilder的BufferConsumer和一个部分记录长度
        // add方法将数据添加到子分区中，并返回期望的缓冲区大小
        int desirableBufferSize =
                subpartitions[targetSubpartition].add(
                        buffer.createBufferConsumerFromBeginning(), partialRecordLength);
        // 根据期望的缓冲区大小和最小期望缓冲区大小，调整BufferBuilder的大小
        resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
    }

    protected int addToSubpartition(
            int targetSubpartition, BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException {
        totalWrittenBytes += bufferConsumer.getWrittenBytes();
        return subpartitions[targetSubpartition].add(bufferConsumer, partialRecordLength);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 调整BufferBuilder的缓冲区大小。
     *
     * @param buffer                 要调整大小的BufferBuilder
     * @param desirableBufferSize    期望的缓冲区大小
     * @param minDesirableBufferSize 最小期望的缓冲区大小
     *
    */
    private void resizeBuffer(
            BufferBuilder buffer, int desirableBufferSize, int minDesirableBufferSize) {
        // 如果期望的缓冲区大小大于0，则进行调整
        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            // 如果BufferBuilder中已经写入了部分数据，则结果大小不能小于已写入的值
            // 因此，我们使用Math.max确保结果大小不小于minDesirableBufferSize和desirableBufferSize中的较大值
            // 调用BufferBuilder的trim方法，将缓冲区大小调整为newSize
            buffer.trim(Math.max(minDesirableBufferSize, desirableBufferSize));
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0, record.remaining());
        }

        append(record, buffer);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        createBroadcastBufferConsumers(buffer, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(
            BufferBuilder buffer, int partialRecordBytes, int minDesirableBufferSize)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            int desirableBufferSize = Integer.MAX_VALUE;
            for (ResultSubpartition subpartition : subpartitions) {
                int subPartitionBufferSize = subpartition.add(consumer.copy(), partialRecordBytes);
                if (subPartitionBufferSize != ResultSubpartition.ADD_BUFFER_ERROR_CODE) {
                    desirableBufferSize = Math.min(desirableBufferSize, subPartitionBufferSize);
                }
            }
            resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求一个新的单播（Unicast）BufferBuilder。
     *
     * @param targetSubpartition 目标子分区索引
     * @return 新的BufferBuilder实例
     * @throws IOException 如果在请求过程中发生I/O错误
    */
    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        // 检查当前是否处于生产状态，校验ResoutPartition是否已经完成
        checkInProduceState();
        ensureUnicastMode();
        // 从池中请求一个新的BufferBuilder实例，像bufferPool申请MemorySegement
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        // 将新请求的BufferBuilder实例保存到unicastBufferBuilders数组中对应的目标子分区位置
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;
        // 返回新请求的BufferBuilder实例
        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从池中请求一个新的BufferBuilder实例。
     *
     * @param targetSubpartition 目标子分区索引
     * @return 新的BufferBuilder实例，如果池中有可用的则返回；否则等待直到有可用实例
     * @throws IOException 如果在等待过程中被中断或发生其他I/O错误
    */
    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition)
            throws IOException {
        // 尝试从bufferPool中请求一个BufferBuilder实例
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
        // 如果成功从池中获取到BufferBuilder，则直接返回
        if (bufferBuilder != null) {
            return bufferBuilder;
        }
        // 如果没有立即获取到BufferBuilder，则标记开始计算硬背压（hard backpressure）的时间
        hardBackPressuredTimeMsPerSecond.markStart();
        try {
            // 调用阻塞方法等待直到从bufferPool中获取到一个BufferBuilder
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            // 获取到BufferBuilder后，标记结束计算硬背压的时间
            hardBackPressuredTimeMsPerSecond.markEnd();
            // 返回从池中获取的BufferBuilder实例
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 完成目标子分区对应的单播BufferBuilder的处理，释放资源并更新统计信息。
     *
     * @param targetSubpartition 目标子分区的编号
    */
    private void finishUnicastBufferBuilder(int targetSubpartition) {
        // 获取目标子分区对应的BufferBuilder
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        // 如果BufferBuilder不为空，即存在有效的BufferBuilder
        if (bufferBuilder != null) {
            // 调用BufferBuilder的finish方法以完成数据准备，并返回实际写入的字节数
            int bytes = bufferBuilder.finish();
            // 更新目标子分区的已写入字节数统计
            resultPartitionBytes.inc(targetSubpartition, bytes);
            // 更新总输出字节数的统计
            numBytesOut.inc(bytes);
            // 更新输出的BufferBuilder数量统计
            numBuffersOut.inc();
            // 将目标子分区对应的BufferBuilder设置为null，以便下次创建新的BufferBuilder
            unicastBufferBuilders[targetSubpartition] = null;
            // 关闭BufferBuilder以释放资源
            bufferBuilder.close();
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int subpartition = 0; subpartition < numSubpartitions; subpartition++) {
            finishUnicastBufferBuilder(subpartition);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            int bytes = broadcastBufferBuilder.finish();
            resultPartitionBytes.incAll(bytes);
            numBytesOut.inc(bytes * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }

    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public TimerGauge getHardBackPressuredTimeMsPerSecond() {
        return hardBackPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }
}
