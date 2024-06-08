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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.RecoveryMetadata;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.RECOVERY_METADATA;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel, which requests a remote partition queue. */
public class RemoteInputChannel extends InputChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteInputChannel.class);

    private static final int NONE = -1;

    /** ID to distinguish this channel from other channels sharing the same TCP connection. */
    private final InputChannelID id = new InputChannelID();

    /** The connection to use to request the remote partition. */
    private final ConnectionID connectionId;

    /** The connection manager to use connect to the remote partition provider. */
    private final ConnectionManager connectionManager;

    /**
     * The received buffers. Received buffers are enqueued by the network I/O thread and the queue
     * is consumed by the receiving task thread.
     */
    private final PrioritizedDeque<SequenceBuffer> receivedBuffers = new PrioritizedDeque<>();

    /**
     * Flag indicating whether this channel has been released. Either called by the receiving task
     * thread or the task manager actor.
     */
    private final AtomicBoolean isReleased = new AtomicBoolean();

    /** Client to establish a (possibly shared) TCP connection and request the partition. */
    private volatile PartitionRequestClient partitionRequestClient;

    /** The next expected sequence number for the next buffer. */
    private int expectedSequenceNumber = 0;

    /** The initial number of exclusive buffers assigned to this channel. */
    /** 分配给该通道的独占缓冲区的初始数量 */
    private final int initialCredit;

    /** The milliseconds timeout for partition request listener in result partition manager. */
    private final int partitionRequestListenerTimeout;

    /** The number of available buffers that have not been announced to the producer yet. */
    private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

    private final BufferManager bufferManager;

    @GuardedBy("receivedBuffers")
    private int lastBarrierSequenceNumber = NONE;

    @GuardedBy("receivedBuffers")
    private long lastBarrierId = NONE;

    private final ChannelStatePersister channelStatePersister;

    private long totalQueueSizeInBytes;

    public RemoteInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            ConnectionID connectionId,
            ConnectionManager connectionManager,
            int initialBackOff,
            int maxBackoff,
            int partitionRequestListenerTimeout,
            int networkBuffersPerChannel,
            Counter numBytesIn,
            Counter numBuffersIn,
            ChannelStateWriter stateWriter) {

        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndexSet,
                initialBackOff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);
        checkArgument(networkBuffersPerChannel >= 0, "Must be non-negative.");

        this.partitionRequestListenerTimeout = partitionRequestListenerTimeout;
        this.initialCredit = networkBuffersPerChannel;
        this.connectionId = checkNotNull(connectionId);
        this.connectionManager = checkNotNull(connectionManager);
        this.bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);
        this.channelStatePersister = new ChannelStatePersister(stateWriter, getChannelInfo());
    }

    @VisibleForTesting
    void setExpectedSequenceNumber(int expectedSequenceNumber) {
        this.expectedSequenceNumber = expectedSequenceNumber;
    }

    /**
     * Setup includes assigning exclusive buffers to this input channel, and this method should be
     * called only once after this input channel is created.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为此输入通道分配独占缓冲区，并且此方法应在创建此输入通道后仅调用一次。
    */
    @Override
    void setup() throws IOException {
        checkState(
                bufferManager.unsynchronizedGetAvailableExclusiveBuffers() == 0,
                "Bug in input channel setup logic: exclusive buffers have already been set for this input channel.");
        //调用BufferManager.requestExclusiveBuffers方法从提供者请求独占缓冲区。
        bufferManager.requestExclusiveBuffers(initialCredit);
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    /** Requests a remote subpartition. */
    @VisibleForTesting
    @Override
    public void requestSubpartitions() throws IOException, InterruptedException {
        if (partitionRequestClient == null) {
            LOG.debug(
                    "{}: Requesting REMOTE subpartitions {} of partition {}. {}",
                    this,
                    consumedSubpartitionIndexSet,
                    partitionId,
                    channelStatePersister);
            // Create a client and request the partition
            try {
                partitionRequestClient =
                        connectionManager.createPartitionRequestClient(connectionId);
            } catch (IOException e) {
                // IOExceptions indicate that we could not open a connection to the remote
                // TaskExecutor
                throw new PartitionConnectionException(partitionId, e);
            }

            partitionRequestClient.requestSubpartition(
                    partitionId, consumedSubpartitionIndexSet, this, 0);
        }
    }

    /** Retriggers a remote subpartition request. */
    void retriggerSubpartitionRequest() throws IOException {
        checkPartitionRequestQueueInitialized();

        if (increaseBackoff()) {
            partitionRequestClient.requestSubpartition(
                    partitionId, consumedSubpartitionIndexSet, this, 0);
        } else {
            failPartitionRequest();
        }
    }

    /**
     * The remote task manager creates partition request listener and returns {@link
     * PartitionNotFoundException} until the listener is timeout, so the backoff should add the
     * timeout milliseconds if it exists.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     */
    @Override
    protected boolean increaseBackoff() {
        if (partitionRequestListenerTimeout > 0) {
            currentBackoff += partitionRequestListenerTimeout;
            return currentBackoff < 2 * maxBackoff;
        }

        // Backoff is disabled
        return false;
    }

    @Override
    protected int peekNextBufferSubpartitionIdInternal() throws IOException {
        checkPartitionRequestQueueInitialized();

        synchronized (receivedBuffers) {
            final SequenceBuffer next = receivedBuffers.peek();

            if (next != null) {
                return next.subpartitionId;
            } else {
                return -1;
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *从已消耗的子分区返回下一个缓冲区，如果没有数据可返回，则返回{@code Optional.empty（）}。
    */
    @Override
    public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
        // 检查分区请求队列是否已初始化
        checkPartitionRequestQueueInitialized();
        // 定义变量来保存即将返回的序列缓冲区和它的数据类型
        final SequenceBuffer next;
        final DataType nextDataType;
        // 使用同步块来确保线程安全地访问receivedBuffers队列
        synchronized (receivedBuffers) {
            // 从receivedBuffers队列中取出下一个序列缓冲区
            next = receivedBuffers.poll();
            // 如果取出了序列缓冲区，则从总队列大小中减去该缓冲区的大小
            if (next != null) {
                totalQueueSizeInBytes -= next.buffer.getSize();
            }
            // 尝试查看队列中的下一个元素（不取出），并获取其数据类型
            // 如果队列为空，则数据类型设为NONE
            nextDataType =
                    receivedBuffers.peek() != null
                            ? receivedBuffers.peek().buffer.getDataType()
                            : DataType.NONE;
        }
        // 如果取出的序列缓冲区为空（即队列中没有更多数据）
        if (next == null) {
            if (isReleased.get()) {
                throw new CancelTaskException(
                        "Queried for a buffer after channel has been released.");
            }
            // 如果没有更多数据，返回空的Optional对象
            return Optional.empty();
        }
        // 记录输入通道获取缓冲区的跟踪日志
        NetworkActionsLogger.traceInput(
                "RemoteInputChannel#getNextBuffer",
                next.buffer,
                inputGate.getOwningTaskName(),
                channelInfo,
                channelStatePersister,
                next.sequenceNumber);
        // 更新接收的字节数和缓冲区数量统计信息
        numBytesIn.inc(next.buffer.getSize());
        numBuffersIn.inc();
        // 创建一个新的BufferAndAvailability对象，包含序列缓冲区的缓冲区、数据类型、可用性（此处设为0）和序列号
        // 并将其包装在Optional对象中返回
        return Optional.of(
                new BufferAndAvailability(next.buffer, nextDataType, 0, next.sequenceNumber));
    }

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    @Override
    void sendTaskEvent(TaskEvent event) throws IOException {
        checkState(
                !isReleased.get(),
                "Tried to send task event to producer after channel has been released.");
        checkPartitionRequestQueueInitialized();

        partitionRequestClient.sendTaskEvent(partitionId, event, this);
    }

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    @Override
    public boolean isReleased() {
        return isReleased.get();
    }

    /** Releases all exclusive and floating buffers, closes the partition request client. */
    @Override
    void releaseAllResources() throws IOException {
        if (isReleased.compareAndSet(false, true)) {

            final ArrayDeque<Buffer> releasedBuffers;
            synchronized (receivedBuffers) {
                releasedBuffers =
                        receivedBuffers.stream()
                                .map(sb -> sb.buffer)
                                .collect(Collectors.toCollection(ArrayDeque::new));
                receivedBuffers.clear();
            }
            bufferManager.releaseAllBuffers(releasedBuffers);

            // The released flag has to be set before closing the connection to ensure that
            // buffers received concurrently with closing are properly recycled.
            if (partitionRequestClient != null) {
                partitionRequestClient.close(this);
            } else {
                connectionManager.closeOpenChannelConnections(connectionId);
            }
        }
    }

    @Override
    int getBuffersInUseCount() {
        return getNumberOfQueuedBuffers()
                + Math.max(0, bufferManager.getNumberOfRequiredBuffers() - initialCredit);
    }

    @Override
    void announceBufferSize(int newBufferSize) {
        try {
            notifyNewBufferSize(newBufferSize);
        } catch (Throwable t) {
            ExceptionUtils.rethrow(t);
        }
    }

    private void failPartitionRequest() {
        setError(new PartitionNotFoundException(partitionId));
    }

    @Override
    public String toString() {
        return "RemoteInputChannel [" + partitionId + " at " + connectionId + "]";
    }

    // ------------------------------------------------------------------------
    // Credit-based
    // ------------------------------------------------------------------------

    /**
     * Enqueue this input channel in the pipeline for notifying the producer of unannounced credit.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将此输入通道加入管道中，以便通知生产者有未声明的信用额度可用。
     *
     * <p>当生产者需要知道是否有额外的信用额度可用时，它会调用此方法。
     * 这通常发生在生产者需要发送更多数据到消费者，但之前可能由于某些原因（如流量控制）
     * 而被限制发送时。</p>
     *
     * <p>注意：在调用此方法之前，需要确保相关的分区请求队列已经被初始化。
     * 如果没有初始化，则可能会抛出异常。</p>
     *
     * @throws IOException 如果在尝试通知信用额度可用时发生I/O错误
    */
    private void notifyCreditAvailable() throws IOException {
        // 检查分区请求队列是否已初始化
        checkPartitionRequestQueueInitialized();
        // 通知生产者此通道有信用额度可用
        partitionRequestClient.notifyCreditAvailable(this);
    }

    private void notifyNewBufferSize(int newBufferSize) throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();

        partitionRequestClient.notifyNewBufferSize(this, newBufferSize);
    }

    @VisibleForTesting
    public int getNumberOfAvailableBuffers() {
        return bufferManager.getNumberOfAvailableBuffers();
    }

    @VisibleForTesting
    public int getNumberOfRequiredBuffers() {
        return bufferManager.unsynchronizedGetNumberOfRequiredBuffers();
    }

    @VisibleForTesting
    public int getSenderBacklog() {
        return getNumberOfRequiredBuffers() - initialCredit;
    }

    @VisibleForTesting
    boolean isWaitingForFloatingBuffers() {
        return bufferManager.unsynchronizedIsWaitingForFloatingBuffers();
    }

    @VisibleForTesting
    public Buffer getNextReceivedBuffer() {
        final SequenceBuffer sequenceBuffer = receivedBuffers.poll();
        return sequenceBuffer != null ? sequenceBuffer.buffer : null;
    }

    @VisibleForTesting
    BufferManager getBufferManager() {
        return bufferManager;
    }

    @VisibleForTesting
    PartitionRequestClient getPartitionRequestClient() {
        return partitionRequestClient;
    }

    /**
     * The unannounced credit is increased by the given amount and might notify increased credit to
     * the producer.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 增加未声明的信用额度并可能通知生产者信用额度已增加。
     *
     * <p>此方法会将给定的信用额度数量增加到未声明的信用额度中，
     * 如果在增加之前未声明的信用额度为0，则可能会通知生产者有新的信用额度可用。</p>
     *
     * @param numAvailableBuffers 可用缓冲区的数量
     * @throws IOException 如果在通知过程中发生I/O错误
    */
    @Override
    public void notifyBufferAvailable(int numAvailableBuffers) throws IOException {
        // 如果在调用getAndAdd之前unannouncedCredit的值为0（即没有未声明的信用额度），
        if (numAvailableBuffers > 0 && unannouncedCredit.getAndAdd(numAvailableBuffers) == 0) {
            // 并且现在增加了numAvailableBuffers的信用额度，则执行notifyCreditAvailable()方法
            notifyCreditAvailable();
        }
    }

    @Override
    public void resumeConsumption() throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();

        if (initialCredit == 0) {
            // this unannounced credit can be a positive value because credit assignment and the
            // increase of this value is not an atomic operation and as a result, this unannounced
            // credit value can be get increased even after this channel has been blocked and all
            // floating credits are released, it is important to clear this unannounced credit and
            // at the same time reset the sender's available credits to keep consistency
            unannouncedCredit.set(0);
        }

        // notifies the producer that this channel is ready to
        // unblock from checkpoint and resume data consumption
        partitionRequestClient.resumeConsumption(this);
    }

    @Override
    public void acknowledgeAllRecordsProcessed() throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();

        partitionRequestClient.acknowledgeAllRecordsProcessed(this);
    }

    private void onBlockingUpstream() {
        if (initialCredit == 0) {
            // release the allocated floating buffers so that they can be used by other channels if
            // no exclusive buffer is configured, it is important because a blocked channel can not
            // transmit any data so the allocated floating buffers can not be recycled, as a result,
            // other channels may can't allocate new buffers for data transmission (an extreme case
            // is that we only have 1 floating buffer and 0 exclusive buffer)
            bufferManager.releaseFloatingBuffers();
        }
    }

    // ------------------------------------------------------------------------
    // Network I/O notifications (called by network I/O thread)
    // ------------------------------------------------------------------------

    /**
     * Gets the currently unannounced credit.
     *
     * @return Credit which was not announced to the sender yet.
     */
    public int getUnannouncedCredit() {
        return unannouncedCredit.get();
    }

    /**
     * Gets the unannounced credit and resets it to <tt>0</tt> atomically.
     *
     * @return Credit which was not announced to the sender yet.
     */
    public int getAndResetUnannouncedCredit() {
        return unannouncedCredit.getAndSet(0);
    }

    /**
     * Gets the current number of received buffers which have not been processed yet.
     *
     * @return Buffers queued for processing.
     */
    public int getNumberOfQueuedBuffers() {
        synchronized (receivedBuffers) {
            return receivedBuffers.size();
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return Math.max(0, receivedBuffers.size());
    }

    @Override
    public long unsynchronizedGetSizeOfQueuedBuffers() {
        return Math.max(0, totalQueueSizeInBytes);
    }

    public int unsynchronizedGetExclusiveBuffersUsed() {
        return Math.max(
                0, initialCredit - bufferManager.unsynchronizedGetAvailableExclusiveBuffers());
    }

    public int unsynchronizedGetFloatingBuffersAvailable() {
        return Math.max(0, bufferManager.unsynchronizedGetFloatingBuffersAvailable());
    }

    public InputChannelID getInputChannelId() {
        return id;
    }

    public int getInitialCredit() {
        return initialCredit;
    }

    public BufferProvider getBufferProvider() throws IOException {
        if (isReleased.get()) {
            return null;
        }

        return inputGate.getBufferProvider();
    }

    /**
     * Requests buffer from input channel directly for receiving network data. It should always
     * return an available buffer in credit-based mode unless the channel has been released.
     *
     * @return The available buffer.
     */
    @Nullable
    public Buffer requestBuffer() {
        return bufferManager.requestBuffer();
    }

    /**
     * Receives the backlog from the producer's buffer response. If the number of available buffers
     * is less than backlog + initialCredit, it will request floating buffers from the buffer
     * manager, and then notify unannounced credits to the producer.
     *
     * @param backlog The number of unsent buffers in the producer's sub partition.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 接收来自生产者缓冲响应的积压（backlog）。
     * 如果可用缓冲区的数量小于积压（backlog）与初始信用（initialCredit）之和，
     * 它将向缓冲区管理器请求浮动缓冲区（floating buffers），
     * 然后向生产者通知未声明的信用（credits）。
     *
     * @param backlog 生产者子分区中未发送的缓冲区数量
     * @throws IOException 如果在请求缓冲区或通知信用时发生I/O错误
    */
    public void onSenderBacklog(int backlog) throws IOException {
        /**
         * 1.计算需要请求的缓冲区总数，即积压（backlog）与初始信用（initialCredit）之和
         * 2.调用缓冲区管理器的requestFloatingBuffers方法，请求所需的浮动缓冲区
         * 3.调用notifyBufferAvailable方法，通知可用的缓冲区数量
         */
        notifyBufferAvailable(bufferManager.requestFloatingBuffers(backlog + initialCredit));
    }

    /**
     * Handles the input buffer. This method is taking over the ownership of the buffer and is fully
     * responsible for cleaning it up both on the happy path and in case of an error.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 当从远程输入通道接收到一个缓冲区时调用此方法。
     *
     * @param sequenceNumber 缓冲区的序列号
     * @param backlog        积压量，表示发送者等待处理的消息数量
     * @throws IOException   如果在处理过程中发生I/O错误
    */
    public void onBuffer(Buffer buffer, int sequenceNumber, int backlog, int subpartitionId)
            throws IOException {
        // 定义一个标志，用于表示是否成功处理空缓冲区
        boolean recycleBuffer = true;

        try {
            // 检查传入的序列号是否与期望的序列号相匹配
            if (expectedSequenceNumber != sequenceNumber) {
                // 如果不匹配，抛出异常并记录错误
                onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
                return;
            }
            // 如果缓冲区的数据类型会阻塞上游操作
            if (buffer.getDataType().isBlockingUpstream()) {
                // 调用阻塞上游的处理逻辑
                onBlockingUpstream();
                checkArgument(backlog == 0, "Illegal number of backlog: %s, should be 0.", backlog);
            }
            // 定义一个变量，用于记录接收到的缓冲区是否为空
            final boolean wasEmpty;
            // 定义一个变量，用于记录是否为第一个优先级事件
            boolean firstPriorityEvent = false;
            // 同步块，确保线程安全
            synchronized (receivedBuffers) {
                // 记录输入日志，包括缓冲区信息、任务名、通道信息等
                NetworkActionsLogger.traceInput(
                        "RemoteInputChannel#onBuffer",
                        buffer,
                        inputGate.getOwningTaskName(),
                        channelInfo,
                        channelStatePersister,
                        sequenceNumber);
                // Similar to notifyBufferAvailable(), make sure that we never add a buffer
                // after releaseAllResources() released all buffers from receivedBuffers
                // (see above for details).
                // 确保在调用releaseAllResources()释放所有资源后，不再添加缓冲区
                if (isReleased.get()) {
                    return;
                }
                // 检查receivedBuffers是否为空
                wasEmpty = receivedBuffers.isEmpty();
                // 创建一个SequenceBuffer对象，包含缓冲区、序列号和子分区ID
                SequenceBuffer sequenceBuffer =
                        new SequenceBuffer(buffer, sequenceNumber, subpartitionId);
                // 获取缓冲区的数据类型
                DataType dataType = buffer.getDataType();
                // 如果数据类型具有优先级
                if (dataType.hasPriority()) {
                    // 添加到优先级缓冲区队列
                    firstPriorityEvent = addPriorityBuffer(sequenceBuffer);
                    // 因为是优先级事件，所以不需要回收缓冲区
                    recycleBuffer = false;
                } else {
                    // 将缓冲区添加到接收到的缓冲区队列
                    receivedBuffers.add(sequenceBuffer);
                    // 因为是普通事件，所以不需要回收缓冲区
                    recycleBuffer = false;
                    // 如果数据类型需要requiresAnnouncement
                    if (dataType.requiresAnnouncement()) {
                        // 其标记为优先级事件
                        firstPriorityEvent = addPriorityBuffer(announce(sequenceBuffer));
                    }
                }
                // 更新缓冲区队列的总大小（以字节为单位）
                totalQueueSizeInBytes += buffer.getSize();
                // 检查这个缓冲区中是否包含了一个屏障
                final OptionalLong barrierId =
                        channelStatePersister.checkForBarrier(sequenceBuffer.buffer);
                // 如果找到了屏障，并且它的ID大于上一个屏障的ID
                if (barrierId.isPresent() && barrierId.getAsLong() > lastBarrierId) {
                    // checkpoint was not yet started by task thread,
                    // so remember the numbers of buffers to spill for the time when
                    // it will be started
                    // 如果任务线程还没有开始处理这个检查点（checkpoint），
                    // 那么记住需要溢出的缓冲区数量，以便在检查点开始时处理
                    lastBarrierId = barrierId.getAsLong();
                    lastBarrierSequenceNumber = sequenceBuffer.sequenceNumber;
                }
                // 持久化这个缓冲区（保存到磁盘或其他存储介质）
                channelStatePersister.maybePersist(buffer);
                // 更新预期的序列号（可能是用于跟踪或同步）
                ++expectedSequenceNumber;
            }
            // 如果这是一个优先级事件
            if (firstPriorityEvent) {
                // 通知优先级事件（可能是调用某个监听器或回调方法）
                notifyPriorityEvent(sequenceNumber);
            }
            // 如果之前队列是空的（可能是之前没有任何数据或消息）
            if (wasEmpty) {
                // 通知通道现在非空（可能是触发某些消费者开始处理数据）
                notifyChannelNonEmpty();
            }
             // 如果backlog是非负数
            if (backlog >= 0) {
                // 处理发送方的backlog（可能是调整发送速率、通知其他组件或进行其他相关操作）
                onSenderBacklog(backlog);
            }
            // 无论之前的代码是否抛出异常，都会执行finally块中的代码
        } finally {
            // 如果需要回收缓冲区
            if (recycleBuffer) {
                // 回收缓冲区（释放内存或其他资源）
                buffer.recycleBuffer();
            }
        }
    }

    /** @return {@code true} if this was first priority buffer added. */
    private boolean addPriorityBuffer(SequenceBuffer sequenceBuffer) {
        receivedBuffers.addPriorityElement(sequenceBuffer);
        return receivedBuffers.getNumPriorityElements() == 1;
    }

    private SequenceBuffer announce(SequenceBuffer sequenceBuffer) throws IOException {
        checkState(
                !sequenceBuffer.buffer.isBuffer(),
                "Only a CheckpointBarrier can be announced but found %s",
                sequenceBuffer.buffer);
        checkAnnouncedOnlyOnce(sequenceBuffer);
        AbstractEvent event =
                EventSerializer.fromBuffer(sequenceBuffer.buffer, getClass().getClassLoader());
        checkState(
                event instanceof CheckpointBarrier,
                "Only a CheckpointBarrier can be announced but found %s",
                sequenceBuffer.buffer);
        CheckpointBarrier barrier = (CheckpointBarrier) event;
        return new SequenceBuffer(
                EventSerializer.toBuffer(
                        new EventAnnouncement(barrier, sequenceBuffer.sequenceNumber), true),
                sequenceBuffer.sequenceNumber,
                sequenceBuffer.subpartitionId);
    }

    private void checkAnnouncedOnlyOnce(SequenceBuffer sequenceBuffer) {
        Iterator<SequenceBuffer> iterator = receivedBuffers.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            if (iterator.next().sequenceNumber == sequenceBuffer.sequenceNumber) {
                count++;
            }
        }
        checkState(
                count == 1,
                "Before enqueuing the announcement there should be exactly single occurrence of the buffer, but found [%d]",
                count);
    }

    /**
     * Spills all queued buffers on checkpoint start. If barrier has already been received (and
     * reordered), spill only the overtaken buffers.
     */
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        synchronized (receivedBuffers) {
            if (barrier.getId() < lastBarrierId) {
                throw new CheckpointException(
                        String.format(
                                "Sequence number for checkpoint %d is not known (it was likely been overwritten by a newer checkpoint %d)",
                                barrier.getId(), lastBarrierId),
                        CheckpointFailureReason
                                .CHECKPOINT_SUBSUMED); // currently, at most one active unaligned
                // checkpoint is possible
            } else if (barrier.getId() > lastBarrierId) {
                // This channel has received some obsolete barrier, older compared to the
                // checkpointId
                // which we are processing right now, and we should ignore that obsoleted checkpoint
                // barrier sequence number.
                resetLastBarrier();
            }

            channelStatePersister.startPersisting(
                    barrier.getId(), getInflightBuffersUnsafe(barrier.getId()));
        }
    }

    public void checkpointStopped(long checkpointId) {
        synchronized (receivedBuffers) {
            channelStatePersister.stopPersisting(checkpointId);
            if (lastBarrierId == checkpointId) {
                resetLastBarrier();
            }
        }
    }

    @VisibleForTesting
    List<Buffer> getInflightBuffers(long checkpointId) {
        synchronized (receivedBuffers) {
            return getInflightBuffersUnsafe(checkpointId);
        }
    }

    @Override
    public void convertToPriorityEvent(int sequenceNumber) throws IOException {
        boolean firstPriorityEvent;
        synchronized (receivedBuffers) {
            checkState(channelStatePersister.hasBarrierReceived());
            int numPriorityElementsBeforeRemoval = receivedBuffers.getNumPriorityElements();
            SequenceBuffer toPrioritize =
                    receivedBuffers.getAndRemove(
                            sequenceBuffer -> sequenceBuffer.sequenceNumber == sequenceNumber);
            checkState(lastBarrierSequenceNumber == sequenceNumber);
            checkState(!toPrioritize.buffer.isBuffer());
            checkState(
                    numPriorityElementsBeforeRemoval == receivedBuffers.getNumPriorityElements(),
                    "Attempted to convertToPriorityEvent an event [%s] that has already been prioritized [%s]",
                    toPrioritize,
                    numPriorityElementsBeforeRemoval);
            // set the priority flag (checked on poll)
            // don't convert the barrier itself (barrier controller might not have been switched
            // yet)
            AbstractEvent e =
                    EventSerializer.fromBuffer(
                            toPrioritize.buffer, this.getClass().getClassLoader());
            toPrioritize.buffer.setReaderIndex(0);
            toPrioritize =
                    new SequenceBuffer(
                            EventSerializer.toBuffer(e, true),
                            toPrioritize.sequenceNumber,
                            toPrioritize.subpartitionId);
            firstPriorityEvent =
                    addPriorityBuffer(
                            toPrioritize); // note that only position of the element is changed
            // converting the event itself would require switching the controller sooner
        }
        if (firstPriorityEvent) {
            notifyPriorityEventForce(); // forcibly notify about the priority event
            // instead of passing barrier SQN to be checked
            // because this SQN might have be seen by the input gate during the announcement
        }
    }

    private void notifyPriorityEventForce() {
        inputGate.notifyPriorityEventForce(this);
    }

    /**
     * Returns a list of buffers, checking the first n non-priority buffers, and skipping all
     * events.
     */
    private List<Buffer> getInflightBuffersUnsafe(long checkpointId) {
        assert Thread.holdsLock(receivedBuffers);

        checkState(checkpointId == lastBarrierId || lastBarrierId == NONE);

        final List<Buffer> inflightBuffers = new ArrayList<>();
        Iterator<SequenceBuffer> iterator = receivedBuffers.iterator();
        // skip all priority events (only buffers are stored anyways)
        Iterators.advance(iterator, receivedBuffers.getNumPriorityElements());

        int finalBufferSubpartitionId = -1;
        while (iterator.hasNext()) {
            SequenceBuffer sequenceBuffer = iterator.next();
            if (sequenceBuffer.buffer.isBuffer()) {
                if (shouldBeSpilled(sequenceBuffer.sequenceNumber)) {
                    inflightBuffers.add(sequenceBuffer.buffer.retainBuffer());
                    finalBufferSubpartitionId = sequenceBuffer.subpartitionId;
                } else {
                    break;
                }
            }
        }

        if (finalBufferSubpartitionId >= 0 && consumedSubpartitionIndexSet.size() > 1) {
            MemorySegment memorySegment;
            try {
                memorySegment =
                        MemorySegmentFactory.wrap(
                                EventSerializer.toSerializedEvent(
                                                new RecoveryMetadata(finalBufferSubpartitionId))
                                        .array());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            inflightBuffers.add(
                    new NetworkBuffer(
                            memorySegment,
                            FreeingBufferRecycler.INSTANCE,
                            RECOVERY_METADATA,
                            memorySegment.size()));
        }

        return inflightBuffers;
    }

    private void resetLastBarrier() {
        lastBarrierId = NONE;
        lastBarrierSequenceNumber = NONE;
    }

    /**
     * @return if given {@param sequenceNumber} should be spilled given {@link
     *     #lastBarrierSequenceNumber}. We might not have yet received {@link CheckpointBarrier} and
     *     we might need to spill everything. If we have already received it, there is a bit nasty
     *     corner case of {@link SequenceBuffer#sequenceNumber} overflowing that needs to be handled
     *     as well.
     */
    private boolean shouldBeSpilled(int sequenceNumber) {
        if (lastBarrierSequenceNumber == NONE) {
            return true;
        }
        checkState(
                receivedBuffers.size() < Integer.MAX_VALUE / 2,
                "Too many buffers for sequenceNumber overflow detection code to work correctly");

        boolean possibleOverflowAfterOvertaking = Integer.MAX_VALUE / 2 < lastBarrierSequenceNumber;
        boolean possibleOverflowBeforeOvertaking =
                lastBarrierSequenceNumber < -Integer.MAX_VALUE / 2;

        if (possibleOverflowAfterOvertaking) {
            return sequenceNumber < lastBarrierSequenceNumber && sequenceNumber > 0;
        } else if (possibleOverflowBeforeOvertaking) {
            return sequenceNumber < lastBarrierSequenceNumber || sequenceNumber > 0;
        } else {
            return sequenceNumber < lastBarrierSequenceNumber;
        }
    }

    public void onEmptyBuffer(int sequenceNumber, int backlog) throws IOException {
        boolean success = false;

        synchronized (receivedBuffers) {
            if (!isReleased.get()) {
                if (expectedSequenceNumber == sequenceNumber) {
                    expectedSequenceNumber++;
                    success = true;
                } else {
                    onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
                }
            }
        }

        if (success && backlog >= 0) {
            onSenderBacklog(backlog);
        }
    }

    public void onFailedPartitionRequest() {
        inputGate.triggerPartitionStateCheck(partitionId, channelInfo);
    }

    public void onError(Throwable cause) {
        setError(cause);
    }

    private void checkPartitionRequestQueueInitialized() throws IOException {
        checkError();
        checkState(
                partitionRequestClient != null,
                "Bug: partitionRequestClient is not initialized before processing data and no error is detected.");
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();
        partitionRequestClient.notifyRequiredSegmentId(this, subpartitionId, segmentId);
    }

    private static class BufferReorderingException extends IOException {

        private static final long serialVersionUID = -888282210356266816L;

        private final int expectedSequenceNumber;

        private final int actualSequenceNumber;

        BufferReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
            this.expectedSequenceNumber = expectedSequenceNumber;
            this.actualSequenceNumber = actualSequenceNumber;
        }

        @Override
        public String getMessage() {
            return String.format(
                    "Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
                    expectedSequenceNumber, actualSequenceNumber);
        }
    }

    private static final class SequenceBuffer {
        final Buffer buffer;
        final int sequenceNumber;

        final int subpartitionId;

        private SequenceBuffer(Buffer buffer, int sequenceNumber, int subpartitionId) {
            this.buffer = buffer;
            this.sequenceNumber = sequenceNumber;
            this.subpartitionId = subpartitionId;
        }

        @Override
        public String toString() {
            return String.format(
                    "SequenceBuffer(isEvent = %s, dataType = %s, sequenceNumber = %s)",
                    !buffer.isBuffer(), buffer.getDataType(), sequenceNumber);
        }
    }
}
