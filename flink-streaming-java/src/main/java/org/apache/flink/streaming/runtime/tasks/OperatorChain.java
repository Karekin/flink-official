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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.CountingOutput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@code OperatorChain} 包含所有在单个 {@link StreamTask} 中作为一个链执行的操作符。
 *
 * <p>链的主要入口点是 {@code mainOperator}。{@code mainOperator} 负责驱动 {@link StreamTask} 的执行，
 * 通过从网络输入和/或源输入中拉取记录，并将生成的记录推送到剩余的链式操作符。
 *
 * @param <OUT> 链接受的元素类型，即链中主操作符的输入类型。
 */

public abstract class OperatorChain<OUT, OP extends StreamOperator<OUT>>
        implements BoundedMultiInput, Closeable {

    /**
     * 日志记录器，用于记录 OperatorChain 的相关日志。
     */
    private static final Logger LOG = LoggerFactory.getLogger(OperatorChain.class);

    /**
     * 用于将处理后的数据发送到网络中的 RecordWriterOutput 数组。
     */
    protected final RecordWriterOutput<?>[] streamOutputs;

    /**
     * 主操作符的输出，用于管理水印的输出。
     */
    protected final WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;

    /**
     * 主操作符的包装器。在迭代的场景下，{@link StreamIterationHead} 和 {@link StreamIterationTail}
     * 负责执行反馈边时不包含任何操作符，此时 {@code mainOperatorWrapper} 和 {@code tailOperatorWrapper} 为 null。
     *
     * <p>通常链中的第一个操作符与 {@link #mainOperatorWrapper} 相同，但如果存在链式的源输入则情况不同。
     * 在这种情况下，其中一个源输入将成为第一个操作符。例如，可以存在以下操作符链：
     *
     * <pre>
     * first
     *      \
     *      main (multi-input) -> ... -> tail
     *      /
     * second
     * </pre>
     *
     * <p>其中 "first" 和 "second"（可以有更多）是链式的源操作符。当涉及到关闭、状态初始化或状态快照时，
     * 操作符链的遍历顺序为：first, second, main, ..., tail，或反序：tail, ..., main, second, first。
     */
    @Nullable protected final StreamOperatorWrapper<OUT, OP> mainOperatorWrapper;

    /**
     * 操作符链中的第一个操作符包装器。
     */
    @Nullable protected final StreamOperatorWrapper<?, ?> firstOperatorWrapper;

    /**
     * 操作符链中的最后一个操作符包装器。
     */
    @Nullable protected final StreamOperatorWrapper<?, ?> tailOperatorWrapper;

    /**
     * 链式源操作符的配置与实例映射。
     */
    protected final Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSources;

    /**
     * 操作符的总数。
     */
    protected final int numOperators;

    /**
     * 操作符事件分发器的实现，用于处理操作符间的事件。
     */
    protected final OperatorEventDispatcherImpl operatorEventDispatcher;

    /**
     * 用于管理资源关闭的工具。
     */
    protected final Closer closer = Closer.create();

    /**
     * 可选的恢复完成输入。
     */
    protected final @Nullable FinishedOnRestoreInput finishedOnRestoreInput;

    /**
     * 标志操作符链是否已关闭。
     */
    protected boolean isClosed;

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建OperatorChain
    */
    public OperatorChain(
            StreamTask<OUT, OP> containingTask,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate) {
        // 初始化OperatorEventDispatcherImpl实例，用于处理Operator事件
        this.operatorEventDispatcher =
                new OperatorEventDispatcherImpl(
                        // 获取环境中的用户代码类加载器
                        containingTask.getEnvironment().getUserCodeClassLoader().asClassLoader(),
                        // 获取环境中的OperatorCoordinator事件网关
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway());
        // 获取用户代码类加载器
        final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
        // 获取StreamTask的配置
        final StreamConfig configuration = containingTask.getConfiguration();
        // 根据用户代码类加载器从配置中获取StreamOperatorFactory
        StreamOperatorFactory<OUT> operatorFactory =
                configuration.getStreamOperatorFactory(userCodeClassloader);

        // we read the chained configs, and the order of record writer registrations by output name
        // 读取配置文件获取配置
        Map<Integer, StreamConfig> chainedConfigs =
                configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);

        // create the final output stream writers
        // we iterate through all the out edges from this job vertex and create a stream output
        // 创建最终的输出流写入器
        //遍历此JobVertex的所有输出边并创建流输出
        // 获取非链式输出列表，这里的"非链式"通常意味着输出不会直接流向下一个操作符或任务，而是可能需要写入外部存储或进行其他处理
        List<NonChainedOutput> outputsInOrder =
                configuration.getVertexNonChainedOutputs(userCodeClassloader);
        // 创建一个HashMap，用于存储IntermediateDataSetID和RecordWriterOutput<?>的映射关系
        // 预期大小设置为outputsInOrder的大小，以提高性能（减少重新哈希和扩容的次数）
        Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs =
                CollectionUtil.newHashMapWithExpectedSize(outputsInOrder.size());
        // 创建一个RecordWriterOutput<?>类型的数组，用于存储流输出
        // 数组的大小与outputsInOrder的大小相同
        this.streamOutputs = new RecordWriterOutput<?>[outputsInOrder.size()];
        // 如果任务作为已完成状态部署，则创建FinishedOnRestoreInput实例
        this.finishedOnRestoreInput =
                this.isTaskDeployedAsFinished()
                        ? new FinishedOnRestoreInput(
                                streamOutputs, configuration.getInputs(userCodeClassloader).length)
                        : null;

        // from here on, we need to make sure that the output writers are shut down again on failure
        boolean success = false;
        try {
            // 根据输出顺序、RecordWriter代理、链式配置等创建链式输出
            createChainOutputs(
                    outputsInOrder,
                    recordWriterDelegate,
                    chainedConfigs,
                    containingTask,
                    recordWriterOutputs);

            // we create the chain of operators and grab the collector that leads into the chain
            // 创建一个ArrayList来存储所有的StreamOperatorWrapper对象，其初始大小与chainedConfigs的大小相同
            List<StreamOperatorWrapper<?, ?>> allOpWrappers =
                    new ArrayList<>(chainedConfigs.size());
            // 创建一个OutputCollector对象，该对象将用于处理主要操作符的输出
            this.mainOperatorOutput =
                    createOutputCollector(
                            containingTask, // 包含此操作符的任务
                            configuration, // 配置信息
                            chainedConfigs, // 链式配置信息
                            userCodeClassloader,  // 用户代码类加载器
                            recordWriterOutputs, // 记录写入器输出
                            allOpWrappers, // 存储所有的Operator包装器
                            containingTask.getMailboxExecutorFactory(),// 获取任务的邮箱执行工厂
                            operatorFactory != null);// 判断是否提供了操作符工厂
            // 如果操作符工厂不为空
            if (operatorFactory != null) {
                // 使用提供的操作符工厂创建主要操作符和处理时间服务
                Tuple2<OP, Optional<ProcessingTimeService>> mainOperatorAndTimeService =
                        StreamOperatorFactoryUtil.createOperator(
                                operatorFactory,
                                containingTask,
                                configuration,
                                mainOperatorOutput,
                                operatorEventDispatcher);
                // 获取主要操作符
                OP mainOperator = mainOperatorAndTimeService.f0;
                // 将主要操作符的当前输出水印指标添加到度量组中
                mainOperator
                        .getMetricGroup()
                        .gauge(
                                MetricNames.IO_CURRENT_OUTPUT_WATERMARK,
                                mainOperatorOutput.getWatermarkGauge());
                /**
                 * 创建主要操作符的包装器
                 *
                 */
                this.mainOperatorWrapper =
                        createOperatorWrapper(
                                mainOperator,
                                containingTask,
                                configuration,
                                mainOperatorAndTimeService.f1,
                                true);

                // add main operator to end of chain
                // 将主要操作符包装器添加到链的末尾
                allOpWrappers.add(mainOperatorWrapper);

                this.tailOperatorWrapper = allOpWrappers.get(0);
            } else {
                // 检查是否确实没有添加任何操作符包装器
                checkState(allOpWrappers.size() == 0);
                // 设置主要操作符包装器和尾操作符包装器为空
                this.mainOperatorWrapper = null;
                this.tailOperatorWrapper = null;
            }
            // 创建链式数据源
            this.chainedSources =
                    createChainedSources(
                            containingTask,
                            configuration.getInputs(userCodeClassloader),
                            chainedConfigs,
                            userCodeClassloader,
                            allOpWrappers);
             // 设置操作符的数量
            this.numOperators = allOpWrappers.size();
            // 将操作符包装器链接在一起
            firstOperatorWrapper = linkOperatorWrappers(allOpWrappers);
            // 标记操作为成功
            success = true;
        } finally {
            // make sure we clean up after ourselves in case of a failure after acquiring
            // the first resources
            // 确保在获取第一个资源后，如果出现失败，进行清理工作
            if (!success) {
                for (int i = 0; i < streamOutputs.length; i++) {
                    if (streamOutputs[i] != null) {
                        streamOutputs[i].close();
                    }
                    streamOutputs[i] = null;
                }
            }
        }
    }

    @VisibleForTesting
    OperatorChain(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            RecordWriterOutput<?>[] streamOutputs,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput,
            StreamOperatorWrapper<OUT, OP> mainOperatorWrapper) {
        this.streamOutputs = streamOutputs;
        this.finishedOnRestoreInput = null;
        this.mainOperatorOutput = checkNotNull(mainOperatorOutput);
        this.operatorEventDispatcher = null;

        checkState(allOperatorWrappers != null && allOperatorWrappers.size() > 0);
        this.mainOperatorWrapper = checkNotNull(mainOperatorWrapper);
        this.tailOperatorWrapper = allOperatorWrappers.get(0);
        this.numOperators = allOperatorWrappers.size();
        this.chainedSources = Collections.emptyMap();

        firstOperatorWrapper = linkOperatorWrappers(allOperatorWrappers);
    }

    public abstract boolean isTaskDeployedAsFinished();

    public abstract void dispatchOperatorEvent(
            OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException;

    public abstract void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Ends the main operator input specified by {@code inputId}).
     *
     * @param inputId the input ID starts from 1 which indicates the first input.
     */
    public abstract void endInput(int inputId) throws Exception;

    /**
     * Initialize state and open all operators in the chain from <b>tail to heads</b>, contrary to
     * {@link StreamOperator#close()} which happens <b>heads to tail</b> (see {@link
     * #finishOperators(StreamTaskActionExecutor, StopMode)}).
     */
    public abstract void initializeStateAndOpenOperators(
            StreamTaskStateInitializer streamTaskStateInitializer) throws Exception;

    /**
     * Closes all operators in a chain effect way. Closing happens from <b>heads to tail</b>
     * operator in the chain, contrary to {@link StreamOperator#open()} which happens <b>tail to
     * heads</b> (see {@link #initializeStateAndOpenOperators(StreamTaskStateInitializer)}).
     */
    public abstract void finishOperators(StreamTaskActionExecutor actionExecutor, StopMode stopMode)
            throws Exception;

    public abstract void notifyCheckpointComplete(long checkpointId) throws Exception;

    public abstract void notifyCheckpointAborted(long checkpointId) throws Exception;

    public abstract void notifyCheckpointSubsumed(long checkpointId) throws Exception;

    public abstract void snapshotState(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception;

    public OperatorEventDispatcher getOperatorEventDispatcher() {
        return operatorEventDispatcher;
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定的事件广播到所有流输出。
     *
     * @param event 要广播的事件对象，必须是AbstractEvent或其子类的实例
     * @param isPriorityEvent 指示该事件是否为优先事件的布尔值
     * @throws IOException 如果在广播事件过程中发生I/O异常，则抛出此异常
    */
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        // 遍历所有的流输出
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            // 对每个流输出调用broadcastEvent方法，将事件和优先级标志传递给它
            streamOutput.broadcastEvent(event, isPriorityEvent);
        }
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.alignedBarrierTimeout(checkpointId);
        }
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.abortCheckpoint(checkpointId, cause);
        }
    }

    /**
     * Execute {@link StreamOperator#close()} of each operator in the chain of this {@link
     * StreamTask}. Closing happens from <b>tail to head</b> operator in the chain.
     */
    public void closeAllOperators() throws Exception {
        isClosed = true;
    }

    public RecordWriterOutput<?>[] getStreamOutputs() {
        return streamOutputs;
    }

    /** Returns an {@link Iterable} which traverses all operators in forward topological order. */
    @VisibleForTesting
    public Iterable<StreamOperatorWrapper<?, ?>> getAllOperators() {
        return getAllOperators(false);
    }

    /**
     * Returns an {@link Iterable} which traverses all operators in forward or reverse topological
     * order.
     */
    protected Iterable<StreamOperatorWrapper<?, ?>> getAllOperators(boolean reverse) {
        return reverse
                ? new StreamOperatorWrapper.ReadIterator(tailOperatorWrapper, true)
                : new StreamOperatorWrapper.ReadIterator(mainOperatorWrapper, false);
    }

    public Input getFinishedOnRestoreInputOrDefault(Input defaultInput) {
        return finishedOnRestoreInput == null ? defaultInput : finishedOnRestoreInput;
    }

    public int getNumberOfOperators() {
        return numOperators;
    }

    public WatermarkGaugeExposingOutput<StreamRecord<OUT>> getMainOperatorOutput() {
        return mainOperatorOutput;
    }

    public ChainedSource getChainedSource(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput);
    }

    public List<Output<StreamRecord<?>>> getChainedSourceOutputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceOutput)
                .collect(Collectors.toList());
    }

    public StreamTaskSourceInput<?> getSourceTaskInput(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput).getSourceTaskInput();
    }

    public List<StreamTaskSourceInput<?>> getSourceTaskInputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceTaskInput)
                .collect(Collectors.toList());
    }

    /**
     * This method should be called before finishing the record emission, to make sure any data that
     * is still buffered will be sent. It also ensures that all data sending related exceptions are
     * recognized.
     *
     * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
     */
    public void flushOutputs() throws IOException {
        for (RecordWriterOutput<?> streamOutput : getStreamOutputs()) {
            streamOutput.flush();
        }
    }

    /**
     * This method releases all resources of the record writer output. It stops the output flushing
     * thread (if there is one) and releases all buffers currently held by the output serializers.
     *
     * <p>This method should never fail.
     */
    public void close() throws IOException {
        closer.close();
    }

    @Nullable
    public OP getMainOperator() {
        return (mainOperatorWrapper == null) ? null : mainOperatorWrapper.getStreamOperator();
    }

    @Nullable
    protected StreamOperator<?> getTailOperator() {
        return (tailOperatorWrapper == null) ? null : tailOperatorWrapper.getStreamOperator();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为指定的 StreamOperator 捕获通道状态，并将其与 OperatorSnapshotFutures 关联起来。
     *
     * @param op                      要进行通道状态快照的 StreamOperator。
     * @param channelStateWriteResult 通道状态写入的结果，包含了输入通道和结果子分区的状态句柄。
     * @param snapshotInProgress      当前的快照进度对象。
    */
    protected void snapshotChannelStates(
            StreamOperator<?> op,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            OperatorSnapshotFutures snapshotInProgress) {
        // 如果传入的 operator 是主 operator，则设置其输入通道状态的快照Future
        if (op == getMainOperator()) {
            snapshotInProgress.setInputChannelStateFuture(
                    channelStateWriteResult
                            .getInputChannelStateHandles()// 获取输入通道状态句柄
                            .thenApply(StateObjectCollection::new)// 将句柄转换为 StateObjectCollection
                            .thenApply(SnapshotResult::of));// 将 StateObjectCollection 封装为 SnapshotResult
        }
        // 如果传入的 operator 是尾 operator，则设置其结果子分区状态的快照Future
        if (op == getTailOperator()) {
            snapshotInProgress.setResultSubpartitionStateFuture(
                    channelStateWriteResult
                            .getResultSubpartitionStateHandles()// 获取结果子分区状态句柄
                            .thenApply(StateObjectCollection::new) // 将句柄转换为 StateObjectCollection
                            .thenApply(SnapshotResult::of)); // 将 StateObjectCollection 封装为 SnapshotResult
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    /** Wrapper class to access the chained sources and their's outputs. */
    public static class ChainedSource {
        private final WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput;
        private final StreamTaskSourceInput<?> sourceTaskInput;

        public ChainedSource(
                WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput,
                StreamTaskSourceInput<?> sourceTaskInput) {
            this.chainedSourceOutput = chainedSourceOutput;
            this.sourceTaskInput = sourceTaskInput;
        }

        public WatermarkGaugeExposingOutput<StreamRecord<?>> getSourceOutput() {
            return chainedSourceOutput;
        }

        public StreamTaskSourceInput<?> getSourceTaskInput() {
            return sourceTaskInput;
        }
    }

    // ------------------------------------------------------------------------
    //  initialization utilities
    // ------------------------------------------------------------------------

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建链式输出的方法，为一系列非链式输出设置相关的流输出和记录写入器
     *
     * @param outputsInOrder          按顺序排列的非链式输出列表
     * @param recordWriterDelegate    记录写入器的委托，用于创建流记录的序列化写入器
     * @param chainedConfigs           链式配置的映射，其中键为源节点ID
     * @param containingTask           包含此输出的流任务
     * @param recordWriterOutputs      用于存储创建的记录写入器输出的映射，其中键为中间数据集ID
    */
    private void createChainOutputs(
            List<NonChainedOutput> outputsInOrder,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate,
            Map<Integer, StreamConfig> chainedConfigs,
            StreamTask<OUT, OP> containingTask,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs) {
        // 遍历非链式输出列表
        for (int i = 0; i < outputsInOrder.size(); ++i) {
            // 获取当前非链式输出
            NonChainedOutput output = outputsInOrder.get(i);
            // 使用记录写入器委托和当前输出、链式配置、环境信息等创建流输出
            RecordWriterOutput<?> recordWriterOutput =
                    createStreamOutput(
                            recordWriterDelegate.getRecordWriter(i),// 获取对应索引的记录写入器
                            output,// 当前非链式输出
                            chainedConfigs.get(output.getSourceNodeId()),// 获取与当前输出相关的链式配置
                            containingTask.getEnvironment());// 获取包含任务的环境信息
            // 将创建的流输出存储在本地数组中
            this.streamOutputs[i] = recordWriterOutput;
            // 将创建的流输出存储在映射中，键为中间数据集ID
            recordWriterOutputs.put(output.getDataSetId(), recordWriterOutput);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个 RecordWriterOutput 对象，该对象用于将数据写入到指定的 RecordWriter 中
     * @param recordWriter 用于写入 StreamRecord
     * @param streamOutput  表示非链式输出
     * @param upStreamConfig 表示上游的配置信息
     * @param taskEnvironment 任务环境对象
    */
    private RecordWriterOutput<OUT> createStreamOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            NonChainedOutput streamOutput,
            StreamConfig upStreamConfig,
            Environment taskEnvironment) {
        // 获取 NonChainedOutput 对象的 OutputTag，如果是侧输出则不为 null
        OutputTag sideOutputTag =
                streamOutput.getOutputTag(); // OutputTag, return null if not sideOutput

        // 声明一个 TypeSerializer 对象，用于序列化输出的数据
        TypeSerializer outSerializer;

        // 判断是否为侧输出
        if (streamOutput.getOutputTag() != null) {
            // side output
            // 如果是侧输出，则从上游配置中获取对应 OutputTag 的 TypeSerializer
            outSerializer =
                    upStreamConfig.getTypeSerializerSideOut(
                            streamOutput.getOutputTag(),
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        } else {
            // main output
            // 如果不是侧输出，则从上游配置中获取主输出的 TypeSerializer
            outSerializer =
                    upStreamConfig.getTypeSerializerOut(
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        }
        // 创建一个 RecordWriterOutput 对象，并注册到 closer 中，以便在必要时进行关闭
        return closer.register(
                new RecordWriterOutput<OUT>(
                        recordWriter,
                        outSerializer,
                        sideOutputTag,
                        streamOutput.supportsUnalignedCheckpoints()));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个包含链式数据源的Map
     * @param containingTask 包含当前任务的StreamTask
     * @param configuredInputs 配置的输入数组
     * @param chainedConfigs 链式配置的Map
     * @param userCodeClassloader 用户代码的类加载器
     * @param allOpWrappers 所有的流操作符包装列表
    */
    @SuppressWarnings("rawtypes")
    private Map<StreamConfig.SourceInputConfig, ChainedSource> createChainedSources(
            StreamTask<OUT, OP> containingTask,
            StreamConfig.InputConfig[] configuredInputs,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            List<StreamOperatorWrapper<?, ?>> allOpWrappers) {
        // 如果没有源输入配置，则返回一个空的Map
        if (Arrays.stream(configuredInputs)
                .noneMatch(input -> input instanceof StreamConfig.SourceInputConfig)) {
            return Collections.emptyMap();
        }
        // 检查主操作符是否为MultipleInputStreamOperator类型
        // 因为只有MultipleInputStreamOperator才支持创建链式输入
        checkState(
                mainOperatorWrapper.getStreamOperator() instanceof MultipleInputStreamOperator,
                "Creating chained input is only supported with MultipleInputStreamOperator and MultipleInputStreamTask");

        // 初始化链式源输入的Map
        Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSourceInputs = new HashMap<>();
        // 获取MultipleInputStreamOperator
        MultipleInputStreamOperator<?> multipleInputOperator =
                (MultipleInputStreamOperator<?>) mainOperatorWrapper.getStreamOperator();
        // 获取操作符的所有输入
        List<Input> operatorInputs = multipleInputOperator.getInputs();
        // 获取下一个可用的源InputGate索引
        int sourceInputGateIndex =
                Arrays.stream(containingTask.getEnvironment().getAllInputGates())
                                .mapToInt(IndexedInputGate::getInputGateIndex)
                                .max()
                                .orElse(-1)
                        + 1;
        // configuredInputs
        for (int inputId = 0; inputId < configuredInputs.length; inputId++) {
            // 如果当前输入不是源输入配置，则跳过
            if (!(configuredInputs[inputId] instanceof StreamConfig.SourceInputConfig)) {
                continue;
            }
            // 转换为源输入配置
            StreamConfig.SourceInputConfig sourceInput =
                    (StreamConfig.SourceInputConfig) configuredInputs[inputId];
            // 获取源输入边的源ID
            int sourceEdgeId = sourceInput.getInputEdge().getSourceId();
            // 从链式配置中获取对应的StreamConfig
            StreamConfig sourceInputConfig = chainedConfigs.get(sourceEdgeId);
            // 获取输出标签
            OutputTag outputTag = sourceInput.getInputEdge().getOutputTag();
            // 创建链式源输出
            WatermarkGaugeExposingOutput chainedSourceOutput =
                    createChainedSourceOutput(
                            containingTask,
                            sourceInputConfig,
                            userCodeClassloader,
                            getFinishedOnRestoreInputOrDefault(operatorInputs.get(inputId)),
                            multipleInputOperator.getMetricGroup(),
                            outputTag);
            // 创建StreamOperator
            SourceOperator<?, ?> sourceOperator =
                    (SourceOperator<?, ?>)
                            createOperator(
                                    containingTask,
                                    sourceInputConfig,
                                    userCodeClassloader,
                                    (WatermarkGaugeExposingOutput<StreamRecord<OUT>>)
                                            chainedSourceOutput,
                                    allOpWrappers,
                                    true);

            //将链式源和对应的源输入配置添加到Map中
            chainedSourceInputs.put(
                    sourceInput,
                    new ChainedSource(
                            chainedSourceOutput,
                            this.isTaskDeployedAsFinished()
                                    ? new StreamTaskFinishedOnRestoreSourceInput<>(
                                            sourceOperator, sourceInputGateIndex++, inputId)
                                    : new StreamTaskSourceInput<>(
                                            sourceOperator, sourceInputGateIndex++, inputId)));
        }
        // 返回链式源输入的Map
        return chainedSourceInputs;
    }

    /**
     * Get the numRecordsOut counter for the operator represented by the given config. And re-use
     * the operator-level counter for the task-level numRecordsOut counter if this operator is at
     * the end of the operator chain.
     *
     * <p>Return null if we should not use the numRecordsOut counter to track the records emitted by
     * this operator.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取由给定配置表示的操作符的numRecordsOut计数器。
     * 如果此操作符位于操作符链的末尾，则重用操作符级别的计数器作为任务级别的numRecordsOut计数器。
     *
     * <p>如果不应使用numRecordsOut计数器来跟踪此操作符发出的记录，则返回null。</p>
     *
     * @param containingTask 包含此操作符的StreamTask实例
     * @param operatorConfig 操作符的配置信息
     * @return 操作符的numRecordsOut计数器，如果不需要则返回null
     * @throws StreamTaskException 如果无法从用户代码类加载器加载SinkWriterOperatorFactory类
    */
    @Nullable
    private Counter getOperatorRecordsOutCounter(
            StreamTask<?, ?> containingTask, StreamConfig operatorConfig) {
        // 获取用户代码类加载器
        ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
        // 根据配置和操作符ID获取StreamOperatorFactory的Class对象
        Class<StreamOperatorFactory<?>> streamOperatorFactoryClass =
                operatorConfig.getStreamOperatorFactoryClass(userCodeClassloader);

        // Do not use the numRecordsOut counter on output if this operator is SinkWriterOperator.
        //
        // Metric "numRecordsOut" is defined as the total number of records written to the
        // external system in FLIP-33, but this metric is occupied in AbstractStreamOperator as the
        // number of records sent to downstream operators, which is number of Committable batches
        // sent to SinkCommitter. So we skip registering this metric on output and leave this metric
        // to sink writer implementations to report.
        try {
            // 如果此操作符是SinkWriterOperator，则不使用numRecordsOut计数器进行输出
            // 加载SinkWriterOperatorFactory类
            Class<?> sinkWriterFactoryClass =
                    userCodeClassloader.loadClass(SinkWriterOperatorFactory.class.getName());
            // 如果streamOperatorFactoryClass是SinkWriterOperatorFactory或其子类，则返回null
            if (sinkWriterFactoryClass.isAssignableFrom(streamOperatorFactoryClass)) {
                return null;
            }
        } catch (ClassNotFoundException e) {
            //异常处理
            throw new StreamTaskException(
                    "Could not load SinkWriterOperatorFactory class from userCodeClassloader.", e);
        }
        // 获取Operator的度量组
        InternalOperatorMetricGroup operatorMetricGroup =
                containingTask
                        .getEnvironment()
                        .getMetricGroup()
                        .getOrAddOperator(
                                operatorConfig.getOperatorID(), operatorConfig.getOperatorName());
        // 从度量组中获取I/O度量组，并返回numRecordsOut计数器
        return operatorMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private WatermarkGaugeExposingOutput<StreamRecord> createChainedSourceOutput(
            StreamTask<?, OP> containingTask,
            StreamConfig sourceInputConfig,
            ClassLoader userCodeClassloader,
            Input input,
            OperatorMetricGroup metricGroup,
            OutputTag outputTag) {

        Counter recordsOutCounter = getOperatorRecordsOutCounter(containingTask, sourceInputConfig);

        WatermarkGaugeExposingOutput<StreamRecord> chainedSourceOutput;
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
            chainedSourceOutput =
                    new ChainingOutput(input, recordsOutCounter, metricGroup, outputTag);
        } else {
            TypeSerializer<?> inSerializer =
                    sourceInputConfig.getTypeSerializerOut(userCodeClassloader);
            chainedSourceOutput =
                    new CopyingChainingOutput(
                            input, inSerializer, recordsOutCounter, metricGroup, outputTag);
        }
        /**
         * Chained sources are closed when {@link
         * org.apache.flink.streaming.runtime.io.StreamTaskSourceInput} are being closed.
         */
        return closer.register(chainedSourceOutput);
    }
   /**
    * @授课老师(微信): yi_locus
    * email: 156184212@qq.com
    * 用于创建WatermarkGaugeExposingOutput类型的输出收集器
    * @param containingTask StreamTask实例，包含当前任务的信息
    * @param operatorConfig 当前操作符的StreamConfig配置
    * @param chainedConfigs 链式操作符的配置映射
    * @param userCodeClassloader 用户的代码类加载器
    * @param recordWriterOutputs 记录写入器输出的映射
    * @param allOperatorWrappers 所有操作符包装器的列表
    * @param mailboxExecutorFactory 邮箱执行器工厂
   */
    private <T> WatermarkGaugeExposingOutput<StreamRecord<T>> createOutputCollector(
            StreamTask<?, ?> containingTask,
            StreamConfig operatorConfig,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            MailboxExecutorFactory mailboxExecutorFactory,
            boolean shouldAddMetric) {
        // 创建一个ArrayList来存储所有的输出（包括非链式和链式输出）
        List<OutputWithChainingCheck<StreamRecord<T>>> allOutputs = new ArrayList<>(4);

        // create collectors for the network outputs
        //循环 为非链式网络输出创建收集器
        for (NonChainedOutput streamOutput :
                operatorConfig.getOperatorNonChainedOutputs(userCodeClassloader)) {
            // 从recordWriterOutputs映射中获取RecordWriterOutput对象，并进行类型转换（这里使用了unchecked警告的抑制）
            @SuppressWarnings("unchecked")
            RecordWriterOutput<T> recordWriterOutput =
                    (RecordWriterOutput<T>) recordWriterOutputs.get(streamOutput.getDataSetId());

            // 将RecordWriterOutput添加到allOutputs列表中
            allOutputs.add(recordWriterOutput);
        }

        // Create collectors for the chained outputs
        // 为链式输出创建收集器
        for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
            int outputId = outputEdge.getTargetId();
            StreamConfig chainedOpConfig = chainedConfigs.get(outputId);
            // 调用createOperatorChain方法来创建链式输出的WatermarkGaugeExposingOutput
            WatermarkGaugeExposingOutput<StreamRecord<T>> output =
                    createOperatorChain(
                            containingTask,
                            operatorConfig,
                            chainedOpConfig,
                            chainedConfigs,
                            userCodeClassloader,
                            recordWriterOutputs,
                            allOperatorWrappers,
                            outputEdge.getOutputTag(),
                            mailboxExecutorFactory,
                            shouldAddMetric);
            // 检查返回的output是否是OutputWithChainingCheck的实例
            checkState(output instanceof OutputWithChainingCheck);
            // 将output添加到allOutputs列表中
            allOutputs.add((OutputWithChainingCheck) output);
            // If the operator has multiple downstream chained operators, only one of them should
            // increment the recordsOutCounter for this operator. Set shouldAddMetric to false
            // so that we would skip adding the counter to other downstream operators.
            // 如果操作符有多个下游链式操作符，则只有其中一个应该增加recordsOutCounter

            // 因此，我们将shouldAddMetric设置为false，以便跳过向其他下游操作符添加计数器
            shouldAddMetric = false;
            shouldAddMetric = false;
        }

        // 定义一个WatermarkGaugeExposingOutput<StreamRecord<T>>类型的变量result，用于存储输出结果
        WatermarkGaugeExposingOutput<StreamRecord<T>> result;
        // 如果allOutputs集合的大小为1
        if (allOutputs.size() == 1) {
            // 则直接获取该输出作为result
            result = allOutputs.get(0);
            // only if this is a single RecordWriterOutput, reuse its numRecordOut for task.
            // 仅当这是一个RecordWriterOutput类型时，才将其numRecordOut设置为任务的计数器
            if (result instanceof RecordWriterOutput) {
                Counter numRecordsOutCounter = createNumRecordsOutCounter(containingTask);
                ((RecordWriterOutput<T>) result).setNumRecordsOut(numRecordsOutCounter);
            }
        } else {
            // send to N outputs. Note that this includes the special case
            // of sending to zero outputs
            // 如果allOutputs集合的大小不为1，则需要将结果发送到N个输出

            // 创建一个OutputWithChainingCheck<StreamRecord<T>>类型的数组，用于存储所有输出
            @SuppressWarnings({"unchecked"})
            OutputWithChainingCheck<StreamRecord<T>>[] allOutputsArray =
                    new OutputWithChainingCheck[allOutputs.size()];
            for (int i = 0; i < allOutputs.size(); i++) {
                allOutputsArray[i] = allOutputs.get(i);
            }

            // This is the inverse of creating the normal ChainingOutput.
            // If the chaining output does not copy we need to copy in the broadcast output,
            // otherwise multi-chaining would not work correctly.
            /**
             * 这是正常创建ChainingOutput的逆过程
             * 如果chaining output不复制数据，我们需要在broadcast output中进行复制，
             *  否则多链式操作将无法正常工作
             */
            Counter numRecordsOutForTask = createNumRecordsOutCounter(containingTask);
            // 根据是否启用对象重用，决定使用哪种输出收集器
            if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
                result =
                        // 如果启用对象重用，使用CopyingBroadcastingOutputCollector来复制数据到所有输出
                        closer.register(
                                new CopyingBroadcastingOutputCollector<>(
                                        allOutputsArray, numRecordsOutForTask));
            } else {
                result =
                        // 否则，使用BroadcastingOutputCollector直接广播数据到所有输出
                        closer.register(
                                new BroadcastingOutputCollector<>(
                                        allOutputsArray, numRecordsOutForTask));
            }
        }
         // 如果需要添加度量指标
        if (shouldAddMetric) {
            // Create a CountingOutput to increment the recordsOutCounter for this operator
            // if we have not added the counter to any downstream chained operator.

            // 创建一个CountingOutput来递增此操作员的recordsOutCounter，
            // 如果我们还没有将此计数器添加到任何下游链式操作员
            Counter recordsOutCounter =
                    getOperatorRecordsOutCounter(containingTask, operatorConfig);
            if (recordsOutCounter != null) {
                // 使用已有的计数器包装result，以便每次输出时递增计数
                result = new CountingOutput<>(result, recordsOutCounter);
            }
        }
        //返回结果
        return result;
    }

    private static Counter createNumRecordsOutCounter(StreamTask<?, ?> containingTask) {
        TaskIOMetricGroup taskIOMetricGroup =
                containingTask.getEnvironment().getMetricGroup().getIOMetricGroup();
        Counter counter = new SimpleCounter();
        taskIOMetricGroup.reuseRecordsOutputCounter(counter);
        return counter;
    }

    /**
     * Recursively create chain of operators that starts from the given {@param operatorConfig}.
     * Operators are created tail to head and wrapped into an {@link WatermarkGaugeExposingOutput}.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 递归地创建从给定{@param operatorConfig}开始的操作符链。
     * 操作符是按照从尾部到头部的顺序创建的，并且被封装在{@link WatermarkGaugeExposingOutput}中。
     *
     * @param <IN>  输入数据流的类型
     * @param <OUT> 输出数据流的类型
     * @param containingTask 包含当前操作符链的StreamTask
     * @param prevOperatorConfig 前一个操作符的配置（如果有的话）
     * @param operatorConfig 当前操作符的配置
     * @param chainedConfigs 链中所有操作符的配置映射
     * @param userCodeClassloader 用户代码类加载器
     * @param recordWriterOutputs 记录写入器的输出映射
     * @param allOperatorWrappers 所有操作符包装器的列表
     * @param outputTag 输出的标签
     * @param mailboxExecutorFactory 邮箱执行器工厂
     * @param shouldAddMetricForPrevOperator 是否应该为前一个操作符添加度量指标
     * @return 封装了操作符的WatermarkGaugeExposingOutput对象
     *
     */
    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createOperatorChain(
            StreamTask<OUT, ?> containingTask,
            StreamConfig prevOperatorConfig,
            StreamConfig operatorConfig,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            OutputTag<IN> outputTag,
            MailboxExecutorFactory mailboxExecutorFactory,
            boolean shouldAddMetricForPrevOperator) {
        // create the output that the operator writes to first. this may recursively create more
        // operators

        // 首先创建操作符写入的输出，这可能会递归地创建更多的操作符
        WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput =
                createOutputCollector(
                        containingTask,
                        operatorConfig,
                        chainedConfigs,
                        userCodeClassloader,
                        recordWriterOutputs,
                        allOperatorWrappers,
                        mailboxExecutorFactory,
                        true);

        // 根据给定的配置创建操作符
        OneInputStreamOperator<IN, OUT> chainedOperator =
                createOperator(
                        containingTask,
                        operatorConfig,
                        userCodeClassloader,
                        chainedOperatorOutput,
                        allOperatorWrappers,
                        false);

        // 将操作符封装在WatermarkGaugeExposingOutput中并返回
        return wrapOperatorIntoOutput(
                chainedOperator,
                containingTask,
                prevOperatorConfig,
                operatorConfig,
                userCodeClassloader,
                outputTag,
                shouldAddMetricForPrevOperator);
    }

    /**
     * Create and return a single operator from the given {@param operatorConfig} that will be
     * producing records to the {@param output}.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的{@code operatorConfig}创建一个单一的操作符，并将产生的记录输出到{@code output}。
     *
     * @param <OUT> 操作符输出的类型
     * @param <OP> 继承自StreamOperator<OUT>的操作符类型
     * @param containingTask 包含该操作符的StreamTask
     * @param operatorConfig 操作符的配置信息
     * @param userCodeClassloader 用于加载用户代码的类加载器
     * @param output 用于收集操作符输出的WatermarkGaugeExposingOutput
     * @param allOperatorWrappers 存储所有操作符包装器的列表
     * @param isHead 是否为头部操作符（影响操作符包装器的创建）
     * @return 返回一个操作符实例OP，该实例被配置为将输出写入到output中
    */
    private <OUT, OP extends StreamOperator<OUT>> OP createOperator(
            StreamTask<OUT, ?> containingTask,
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> output,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            boolean isHead) {

        // now create the operator and give it the output collector to write its output to
        // 现在创建操作符，并将输出收集器（output collector）给它以写入其输出
        Tuple2<OP, Optional<ProcessingTimeService>> chainedOperatorAndTimeService =
                StreamOperatorFactoryUtil.createOperator(
                        operatorConfig.getStreamOperatorFactory(userCodeClassloader),
                        containingTask,
                        operatorConfig,
                        output,
                        operatorEventDispatcher);

        OP chainedOperator = chainedOperatorAndTimeService.f0;
        // 创建一个操作符包装器，并将其添加到allOperatorWrappers列表中
        allOperatorWrappers.add(
                createOperatorWrapper(
                        chainedOperator,
                        containingTask,
                        operatorConfig,
                        chainedOperatorAndTimeService.f1,
                        isHead));
        // 配置并注册一个指标，用于监控当前输出的水印值
        chainedOperator
                .getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_OUTPUT_WATERMARK,
                        output.getWatermarkGauge()::getValue);
        return chainedOperator;
    }

    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> wrapOperatorIntoOutput(
            OneInputStreamOperator<IN, OUT> operator,
            StreamTask<OUT, ?> containingTask,
            StreamConfig prevOperatorConfig,
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            OutputTag<IN> outputTag,
            boolean shouldAddMetricForPrevOperator) {

        Counter recordsOutCounter = null;

        if (shouldAddMetricForPrevOperator) {
            recordsOutCounter = getOperatorRecordsOutCounter(containingTask, prevOperatorConfig);
        }

        WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
            currentOperatorOutput =
                    new ChainingOutput<>(
                            operator, recordsOutCounter, operator.getMetricGroup(), outputTag);
        } else {
            TypeSerializer<IN> inSerializer =
                    operatorConfig.getTypeSerializerIn1(userCodeClassloader);
            currentOperatorOutput =
                    new CopyingChainingOutput<>(
                            operator,
                            inSerializer,
                            recordsOutCounter,
                            operator.getMetricGroup(),
                            outputTag);
        }

        // wrap watermark gauges since registered metrics must be unique
        operator.getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_INPUT_WATERMARK,
                        currentOperatorOutput.getWatermarkGauge()::getValue);

        return closer.register(currentOperatorOutput);
    }

    /**
     * Links operator wrappers in forward topological order.
     *
     * @param allOperatorWrappers is an operator wrapper list of reverse topological order
     */
    private StreamOperatorWrapper<?, ?> linkOperatorWrappers(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers) {
        StreamOperatorWrapper<?, ?> previous = null;
        for (StreamOperatorWrapper<?, ?> current : allOperatorWrappers) {
            if (previous != null) {
                previous.setPrevious(current);
            }
            current.setNext(previous);
            previous = current;
        }
        return previous;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构造StreamOperatorWrapper 继承了StreamOperator
    */
    private <T, P extends StreamOperator<T>> StreamOperatorWrapper<T, P> createOperatorWrapper(
            P operator,
            StreamTask<?, ?> containingTask,
            StreamConfig operatorConfig,
            Optional<ProcessingTimeService> processingTimeService,
            boolean isHead) {
        return new StreamOperatorWrapper<>(
                operator,
                processingTimeService,
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(operatorConfig.getChainIndex()),
                isHead);
    }

    protected void sendAcknowledgeCheckpointEvent(long checkpointId) {
        if (operatorEventDispatcher == null) {
            return;
        }

        operatorEventDispatcher
                .getRegisteredOperators()
                .forEach(
                        x ->
                                operatorEventDispatcher
                                        .getOperatorEventGateway(x)
                                        .sendEventToCoordinator(
                                                new AcknowledgeCheckpointEvent(checkpointId)));
    }
}
