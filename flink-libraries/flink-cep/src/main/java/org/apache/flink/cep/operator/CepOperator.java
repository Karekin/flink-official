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

package org.apache.flink.cep.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.configuration.SharedBufferCacheConfig;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Stream;

/**
 * CEP pattern operator for a keyed input stream. For each key, the operator creates a {@link NFA}
 * and a priority queue to buffer out of order elements. Both data structures are stored using the
 * managed keyed state.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 *
 *
 */

/**
 * <p>这段代码是 Flink 的 <code>CepOperator</code>，用于处理基于 <em>Complex Event Processing (CEP)</em> 的模式匹配。
 * 它使用 <code>NFA</code>（非确定有限状态自动机）来解析流数据，并提供 <code>PatternProcessFunction</code> 来处理匹配到的模式。</p>
 *
 * <h2>代码分析</h2>
 *
 * <h3>1. 主要作用</h3>
 * <ul>
 *     <li><code>CepOperator</code> 继承自 <code>AbstractUdfStreamOperator</code>，用于基于 <em>keyed stream</em> 运行 CEP 规则。</li>
 *     <li>主要用于检测流数据是否匹配某种模式，并输出匹配结果。</li>
 *     <li>通过 <code>NFA</code> 进行状态管理和匹配模式，并使用 <code>SharedBuffer</code> 存储部分匹配的事件。</li>
 * </ul>
 *
 * <h3>2. 主要组件</h3>
 * <table border="1">
 *     <tr>
 *         <th>组件</th>
 *         <th>作用</th>
 *     </tr>
 *     <tr>
 *         <td><strong><code>NFA</code> (Non-deterministic Finite Automaton)</strong></td>
 *         <td>进行事件模式匹配</td>
 *     </tr>
 *     <tr>
 *         <td><strong><code>SharedBuffer</code></strong></td>
 *         <td>存储部分匹配的事件</td>
 *     </tr>
 *     <tr>
 *         <td><strong><code>InternalTimerService</code></strong></td>
 *         <td>处理时间事件 (event time / processing time)</td>
 *     </tr>
 *     <tr>
 *         <td><strong><code>PatternProcessFunction</code></strong></td>
 *         <td>处理匹配成功的模式</td>
 *     </tr>
 *     <tr>
 *         <td><strong><code>AfterMatchSkipStrategy</code></strong></td>
 *         <td>控制匹配后如何跳过某些元素</td>
 *     </tr>
 *     <tr>
 *         <td><strong><code>CepRuntimeContext</code></strong></td>
 *         <td>用于存储运行时上下文数据</td>
 *     </tr>
 * </table>
 *
 * <h3>3. 核心方法</h3>
 * <ul>
 *     <li><code>processElement(StreamRecord&lt;IN&gt; element)</code>：处理每个输入事件，更新 <code>NFA</code> 状态，并检查是否匹配模式。</li>
 *     <li><code>onEventTime(InternalTimer&lt;KEY, VoidNamespace&gt; timer)</code>：在 <code>event time</code> 触发时，对缓冲的事件进行处理。</li>
 *     <li><code>onProcessingTime(InternalTimer&lt;KEY, VoidNamespace&gt; timer)</code>：在 <code>processing time</code> 触发时，对缓冲的事件进行处理。</li>
 *     <li><code>advanceTime(NFAState nfaState, long timestamp)</code>：更新 <code>NFA</code> 的时间，丢弃过期的模式。</li>
 *     <li><code>processMatchedSequences(Iterable&lt;Map&lt;String, List&lt;IN&gt;&gt;&gt; matchingSequences, long timestamp)</code>：处理成功匹配的模式。</li>
 *     <li><code>processTimedOutSequences(Collection&lt;Tuple2&lt;Map&lt;String, List&lt;IN&gt;&gt;, Long&gt;&gt; timedOutSequences)</code>：处理超时未匹配的模式。</li>
 *     <li><code>bufferEvent(IN event, long currentTime)</code>：将事件存入 <code>elementQueueState</code>，等待匹配。</li>
 *     <li><code>registerTimer(long timestamp)</code>：注册 <code>event time</code> 或 <code>processing time</code> 计时器。</li>
 * </ul>
 *
 * <h3>4. 处理流程</h3>
 * <ol>
 *     <li><strong>新事件进入</strong>，调用 <code>processElement(StreamRecord&lt;IN&gt; element)</code>。</li>
 *     <li><strong>检查事件时间</strong>：
 *         <ul>
 *             <li>若是 <strong>Event Time</strong>，事件会缓存在 <code>elementQueueState</code>，直到 <code>Watermark</code> 触发。</li>
 *             <li>若是 <strong>Processing Time</strong>，直接处理。</li>
 *         </ul>
 *     </li>
 *     <li><strong>匹配模式</strong>：
 *         <ul>
 *             <li><code>NFA</code> 检查事件是否匹配当前模式。</li>
 *             <li>若匹配成功，则调用 <code>processMatchedSequences</code> 处理匹配数据。</li>
 *             <li>若模式未完成，事件存入 <code>SharedBuffer</code> 继续等待。</li>
 *         </ul>
 *     </li>
 *     <li><strong>处理 Watermark</strong>：
 *         <ul>
 *             <li><code>onEventTime</code> 触发后，取出 <code>elementQueueState</code> 中的事件，依次匹配。</li>
 *         </ul>
 *     </li>
 * </ol>
 *
 * <h3>5. 关键点</h3>
 * <ul>
 *     <li><strong>状态存储</strong>：
 *         <ul>
 *             <li><code>NFAState</code> 用于记录模式匹配进度，存入 <code>ValueState</code>。</li>
 *             <li><code>SharedBuffer</code> 用于存储事件序列，减少内存占用。</li>
 *             <li><code>elementQueueState</code> 存放延迟到来的事件，确保有序匹配。</li>
 *         </ul>
 *     </li>
 *     <li><strong>事件时间 vs 处理时间</strong>：
 *         <ul>
 *             <li><code>isProcessingTime = true</code> 时，<code>event time</code> 逻辑无效，所有事件按到达顺序处理。</li>
 *             <li><code>event time</code> 模式下，依赖 <code>watermark</code> 触发缓冲区处理。</li>
 *         </ul>
 *     </li>
 *     <li><strong>超时处理</strong>：
 *         <ul>
 *             <li><code>nfa.advanceTime(...)</code> 负责将超时的匹配丢弃。</li>
 *             <li><code>processTimedOutSequences(...)</code> 处理超时模式，调用 <code>TimedOutPartialMatchHandler</code>。</li>
 *         </ul>
 *     </li>
 * </ul>
 *
 * <h3>6. 总结</h3>
 * <ul>
 *     <li><strong><code>CepOperator</code> 作用</strong>：用于 Flink CEP 模式匹配，基于 <code>NFA</code> 处理流数据。</li>
 *     <li><strong>主要逻辑</strong>：
 *         <ul>
 *             <li>维护 <code>NFA</code> 状态，实现事件匹配。</li>
 *             <li>处理乱序数据，确保 <code>event time</code> 一致性。</li>
 *             <li>提供 <code>PatternProcessFunction</code>，允许用户定义匹配后处理逻辑。</li>
 *         </ul>
 *     </li>
 * </ul>
 */

@Internal
public class CepOperator<IN, KEY, OUT>
        extends AbstractUdfStreamOperator<OUT, PatternProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace> {

    private static final long serialVersionUID = -4166778210774160757L;

    // 用于记录被丢弃的延迟事件的度量指标名称
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    // 是否使用处理时间（Processing Time），如果为 false，则使用事件时间（Event Time）
    private final boolean isProcessingTime;

    // 输入数据类型的序列化器，用于状态存储和数据处理
    private final TypeSerializer<IN> inputSerializer;

    /////////////// 状态管理 ////////////////

    // NFA（非确定有限状态机）状态的名称
    private static final String NFA_STATE_NAME = "nfaStateName";

    // 事件队列状态名称，用于存储乱序到达的事件
    private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

    // NFA 工厂，用于创建 NFA 实例
    private final NFACompiler.NFAFactory<IN> nfaFactory;

    // 计算状态，存储 NFA 运行时的状态信息
    private transient ValueState<NFAState> computationStates;

    // 事件队列状态，按时间戳存储乱序到达的事件
    private transient MapState<Long, List<IN>> elementQueueState;

    // 共享缓冲区（SharedBuffer），用于存储部分匹配的事件，提高匹配效率
    private transient SharedBuffer<IN> partialMatches;

    // 定时器服务（Timer Service），用于处理事件时间或处理时间的触发逻辑
    private transient InternalTimerService<VoidNamespace> timerService;

    // NFA 实例，用于处理复杂事件匹配（CEP）
    private transient NFA<IN> nfa;

    /**
     * 事件比较器，用于二级排序，主要依据时间戳进行排序
     * 如果提供了自定义比较器，会用于事件处理的排序
     */
    private final EventComparator<IN> comparator;

    /**
     * 用于处理延迟到达的事件的 {@link OutputTag}。
     * 具有比当前 Watermark 更小时间戳的事件将被发送到此标签的侧输出流中。
     */
    private final OutputTag<IN> lateDataOutputTag;

    /**
     * 匹配成功后，决定跳过哪些元素的策略
     * 例如，跳过所有匹配中的第一个事件，以避免重复匹配
     */
    private final AfterMatchSkipStrategy afterMatchSkipStrategy;

    /**
     * 传递给用户定义函数（PatternProcessFunction）的上下文对象
     * 该对象用于提供匹配事件的相关信息
     */
    private transient ContextFunctionImpl context;

    /**
     * 主要的输出收集器（collector），用于设置流记录的正确时间戳并输出数据
     */
    private transient TimestampedCollector<OUT> collector;

    /**
     * 包装后的 RuntimeContext，提供受限的运行时上下文功能
     */
    private transient CepRuntimeContext cepRuntimeContext;

    /**
     * 轻量级的时间服务接口，提供给 NFA 访问时间相关的特性
     */
    private transient TimerService cepTimerService;

    // ------------------------------------------------------------------------
    // 度量指标（Metrics）
    // ------------------------------------------------------------------------

    // 统计丢弃的延迟事件的数量
    private transient Counter numLateRecordsDropped;


    /**
     * `CepOperator` 构造函数，初始化 `CEP` 计算相关组件。
     *
     * @param inputSerializer 输入数据的序列化器，用于处理流数据
     * @param isProcessingTime 是否使用处理时间（true 表示处理时间，false 表示事件时间）
     * @param nfaFactory `NFA`（非确定有限状态机）工厂，用于创建 `NFA` 实例
     * @param comparator 事件比较器（可选），用于二级排序，主要按照时间戳进行排序
     * @param afterMatchSkipStrategy 匹配后跳过策略（可选），控制模式匹配后的数据处理方式
     * @param function 用户定义的 `PatternProcessFunction`，用于处理匹配成功的模式
     * @param lateDataOutputTag 延迟事件的输出标签（可选），用于收集 `Watermark` 之后到达的迟到数据
     */
    public CepOperator(
            final TypeSerializer<IN> inputSerializer,
            final boolean isProcessingTime,
            final NFACompiler.NFAFactory<IN> nfaFactory,
            @Nullable final EventComparator<IN> comparator,
            @Nullable final AfterMatchSkipStrategy afterMatchSkipStrategy,
            final PatternProcessFunction<IN, OUT> function,
            @Nullable final OutputTag<IN> lateDataOutputTag) {

        // 调用父类 `AbstractUdfStreamOperator` 构造方法，传递用户定义的 `PatternProcessFunction`
        super(function);

        // 校验参数不能为空，确保输入序列化器和 `NFA` 工厂有效
        this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
        this.nfaFactory = Preconditions.checkNotNull(nfaFactory);

        // 赋值 `CEP` 计算相关属性
        this.isProcessingTime = isProcessingTime;
        this.comparator = comparator;
        this.lateDataOutputTag = lateDataOutputTag;

        // 处理匹配后的跳过策略，默认为 `noSkip()`（不跳过任何事件）
        if (afterMatchSkipStrategy == null) {
            this.afterMatchSkipStrategy = AfterMatchSkipStrategy.noSkip();
        } else {
            this.afterMatchSkipStrategy = afterMatchSkipStrategy;
        }
    }

    /**
     * `setup` 方法：初始化运行时环境，在任务启动前执行。
     *
     * @param containingTask 运行的 `Flink StreamTask`
     * @param config 操作符配置
     * @param output 输出流
     */
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {

        // 调用父类 `setup` 方法
        super.setup(containingTask, config, output);

        // 初始化 `CepRuntimeContext`，用于提供运行时环境信息
        this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());

        // 设置用户函数的运行时上下文
        FunctionUtils.setFunctionRuntimeContext(getUserFunction(), this.cepRuntimeContext);
    }

    /**
     * `initializeState` 方法：初始化 `Flink` 运行时状态。
     * 该方法在 `Flink` 作业恢复时调用，以便从之前的检查点（checkpoint）恢复状态。
     *
     * @param context 状态初始化上下文
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // 1. 初始化 `NFA` 状态，用于存储模式匹配的状态信息
        computationStates =
                context.getKeyedStateStore()
                        .getState(new ValueStateDescriptor<>(
                                NFA_STATE_NAME, new NFAStateSerializer()));

        // 2. 初始化 `SharedBuffer`，用于存储部分匹配事件，减少内存占用
        partialMatches =
                new SharedBuffer<>(
                        context.getKeyedStateStore(),
                        inputSerializer,
                        SharedBufferCacheConfig.of(getOperatorConfig().getConfiguration()));

        // 3. 初始化 `elementQueueState`，用于存储乱序到达的事件
        elementQueueState =
                context.getKeyedStateStore()
                        .getMapState(new MapStateDescriptor<>(
                                EVENT_QUEUE_STATE_NAME,
                                LongSerializer.INSTANCE,
                                new ListSerializer<>(inputSerializer)));

        // 4. 如果任务是从检查点恢复的，则进行状态迁移
        if (context.isRestored()) {
            partialMatches.migrateOldState(getKeyedStateBackend(), computationStates);
        }
    }

    /**
     * `open` 方法：操作符启动时执行，主要用于初始化 `NFA` 和 `CEP` 相关组件。
     */
    @Override
    public void open() throws Exception {
        super.open();

        // 1. 初始化定时器服务，用于管理 `Watermark` 或 `Processing Time`
        timerService = getInternalTimerService(
                "watermark-callbacks", VoidNamespaceSerializer.INSTANCE, this);

        // 2. 创建 `NFA` 实例，并进行初始化
        nfa = nfaFactory.createNFA();
        nfa.open(cepRuntimeContext, new Configuration());

        // 3. 初始化 `PatternProcessFunction` 上下文
        context = new ContextFunctionImpl();

        // 4. 初始化数据输出收集器，确保输出数据带有正确的时间戳
        collector = new TimestampedCollector<>(output);

        // 5. 初始化 `TimerService`，提供时间控制
        cepTimerService = new TimerServiceImpl();

        // 6. 注册 `Flink` 度量指标，统计丢弃的延迟事件
        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    /**
     * `close` 方法：操作符关闭时执行，主要用于释放资源。
     */
    @Override
    public void close() throws Exception {
        super.close();

        // 1. 关闭 `NFA` 释放资源
        if (nfa != null) {
            nfa.close();
        }

        // 2. 释放 `SharedBuffer` 的缓存统计计时器
        if (partialMatches != null) {
            partialMatches.releaseCacheStatisticsTimer();
        }
    }


    /**
     * 处理流中的每个输入元素，根据时间模式（处理时间或事件时间）进行相应的处理。
     *
     * @param element 输入的流事件
     * @throws Exception 可能抛出的异常
     */
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (isProcessingTime) { // 处理时间模式
            if (comparator == null) {
                // 处理时间模式下，事件不会乱序，因此无需额外排序
                NFAState nfaState = getNFAState();
                long timestamp = getProcessingTimeService().getCurrentProcessingTime();

                // 更新 `NFA` 的时间状态，确保时间推进
                advanceTime(nfaState, timestamp);

                // 处理当前事件，检查是否匹配 `CEP` 模式
                processEvent(nfaState, element.getValue(), timestamp);

                // 更新 `NFA` 状态，确保状态存入 `StateBackend`
                updateNFA(nfaState);
            } else {
                // 如果提供了 `EventComparator`，则缓冲事件
                long currentTime = timerService.currentProcessingTime();
                bufferEvent(element.getValue(), currentTime);
            }
        } else { // 事件时间模式（event time）
            long timestamp = element.getTimestamp();
            IN value = element.getValue();

            // 事件时间模式下，假设 `Watermark` 机制是正确的
            // 如果事件的时间戳小于等于当前 `Watermark`，则认为该事件是延迟数据
            if (timestamp > timerService.currentWatermark()) {
                // 有效时间的事件，缓冲到 `elementQueueState`，等待 `Watermark` 触发
                bufferEvent(value, timestamp);
            } else if (lateDataOutputTag != null) {
                // 如果有延迟数据输出标签，则将事件发送到侧输出流
                output.collect(lateDataOutputTag, element);
            } else {
                // 没有侧输出流的情况下，增加延迟数据丢弃计数器
                numLateRecordsDropped.inc();
            }
        }
    }

    /**
     * 注册 `Timer` 计时器。
     * 在 `Processing Time` 模式下，会在 `timestamp + 1` 触发。
     * 在 `Event Time` 模式下，会在 `timestamp` 触发。
     *
     * @param timestamp 触发时间
     */
    private void registerTimer(long timestamp) {
        if (isProcessingTime) {
            timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp + 1);
        } else {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
        }
    }

    /**
     * 缓存事件，等待 `Watermark` 触发后处理。
     *
     * @param event 需要缓存的事件
     * @param currentTime 当前事件的时间戳
     * @throws Exception 可能抛出的异常
     */
    private void bufferEvent(IN event, long currentTime) throws Exception {
        // 获取当前时间戳的事件列表
        List<IN> elementsForTimestamp = elementQueueState.get(currentTime);

        if (elementsForTimestamp == null) {
            // 初始化事件列表，并注册 `Timer`
            elementsForTimestamp = new ArrayList<>();
            registerTimer(currentTime);
        }

        // 添加事件到缓冲区
        elementsForTimestamp.add(event);
        elementQueueState.put(currentTime, elementsForTimestamp);
    }

    /**
     * 处理 `Event Time` 模式下的 `Watermark` 事件。
     * `Watermark` 触发后，会对所有已到达的事件进行处理，并更新 `NFA` 状态。
     *
     * @param timer 触发的 `Timer`
     * @throws Exception 可能抛出的异常
     */
    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // 1) 获取 `NFA` 需要处理的所有时间戳，并获取 `NFA` 状态
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        NFAState nfaState = getNFAState();

        // 2) 依次处理 `Watermark` 之前的所有事件
        while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
            long timestamp = sortedTimestamps.poll();

            // 2.1) 更新 `NFA` 时间状态，确保过期数据被丢弃
            advanceTime(nfaState, timestamp);

            // 2.2) 获取该时间戳的所有事件，并按照 `EventComparator` 进行排序
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                // 2.3) 依次处理事件，匹配 `CEP` 模式
                elements.forEachOrdered(
                        event -> {
                            try {
                                processEvent(nfaState, event, timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }

            // 2.4) 处理完成后，删除 `elementQueueState` 里的数据
            elementQueueState.remove(timestamp);
        }

        // 3) 再次更新 `NFA` 时间状态，确保 `Watermark` 之后的匹配正常进行
        advanceTime(nfaState, timerService.currentWatermark());

        // 4) 更新 `NFA` 状态，确保 `Flink` 状态一致性
        updateNFA(nfaState);
    }


    /**
     * 在 `Processing Time` 触发的定时器回调方法。
     * 主要作用：
     * 1. 获取所有待处理的事件时间戳，并恢复 `NFA` 状态。
     * 2. 依次处理 `Processing Time` 触发的事件，将其输入 `NFA` 进行模式匹配。
     * 3. 更新 `NFA` 状态，确保状态一致性。
     *
     * @param timer 触发的定时器
     * @throws Exception 可能抛出的异常
     */
    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // 1) 获取当前 Key 下待处理的时间戳，并恢复 `NFA` 状态
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        NFAState nfa = getNFAState();

        // 2) 依次处理所有待处理的时间戳
        while (!sortedTimestamps.isEmpty()) {
            long timestamp = sortedTimestamps.poll();

            // 2.1) 先推进 `NFA` 的时间状态，确保过期数据被丢弃
            advanceTime(nfa, timestamp);

            // 2.2) 获取当前时间戳的所有事件，并进行排序（如果有 `Comparator`）
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                // 2.3) 依次处理事件，检查是否匹配 `CEP` 模式
                elements.forEachOrdered(
                        event -> {
                            try {
                                processEvent(nfa, event, timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }

            // 2.4) 处理完成后，清理 `elementQueueState` 中的缓存数据
            elementQueueState.remove(timestamp);
        }

        // 3) 进一步推进 `NFA` 的时间状态，确保 `Processing Time` 逻辑正确
        advanceTime(nfa, timerService.currentProcessingTime());

        // 4) 更新 `NFA` 状态，确保 `Flink` 状态存储的一致性
        updateNFA(nfa);
    }

    /**
     * 对事件集合进行排序（如果提供了 `EventComparator`）。
     * 在 `Processing Time` 模式下，这通常用于确保事件按定义的顺序被处理。
     *
     * @param elements 待排序的事件集合
     * @return 排序后的事件流
     */
    private Stream<IN> sort(Collection<IN> elements) {
        Stream<IN> stream = elements.stream();
        return (comparator == null) ? stream : stream.sorted(comparator);
    }

    /**
     * 获取 `NFA` 当前状态，如果 `NFAState` 为空，则创建一个新的初始状态。
     *
     * @return `NFAState` 当前状态
     * @throws IOException 可能抛出的异常
     */
    private NFAState getNFAState() throws IOException {
        NFAState nfaState = computationStates.value();
        return nfaState != null ? nfaState : nfa.createInitialNFAState();
    }

    /**
     * 更新 `NFA` 状态，仅当 `NFA` 状态发生变化时才更新存储的状态。
     *
     * @param nfaState 需要更新的 `NFA` 状态
     * @throws IOException 可能抛出的异常
     */
    private void updateNFA(NFAState nfaState) throws IOException {
        if (nfaState.isStateChanged()) {
            nfaState.resetStateChanged();
            nfaState.resetNewStartPartialMatch();
            computationStates.update(nfaState);
        }
    }

    /**
     * 获取所有待处理的事件时间戳，并按照时间顺序排序。
     *
     * @return 排序后的时间戳队列
     * @throws Exception 可能抛出的异常
     */
    private PriorityQueue<Long> getSortedTimestamps() throws Exception {
        PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
        for (Long timestamp : elementQueueState.keys()) {
            sortedTimestamps.offer(timestamp);
        }
        return sortedTimestamps;
    }

    /**
     * 处理单个事件，将其输入 `NFA` 进行模式匹配，并输出匹配成功的事件序列。
     *
     * @param nfaState 当前 `NFA` 状态
     * @param event 需要处理的事件
     * @param timestamp 事件时间戳
     * @throws Exception 可能抛出的异常
     */
    private void processEvent(NFAState nfaState, IN event, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
            // 通过 `NFA` 处理事件，获取匹配到的事件序列
            Collection<Map<String, List<IN>>> patterns =
                    nfa.process(
                            sharedBufferAccessor,
                            nfaState,
                            event,
                            timestamp,
                            afterMatchSkipStrategy,
                            cepTimerService);

            // 如果 `NFA` 设定了窗口时间，则注册定时器以处理超时数据
            if (nfa.getWindowTime() > 0 && nfaState.isNewStartPartialMatch()) {
                registerTimer(timestamp + nfa.getWindowTime());
            }

            // 处理匹配到的事件序列
            processMatchedSequences(patterns, timestamp);
        }
    }

    /**
     * 推进 `NFA` 的时间状态，确保 `NFA` 不再接受比 `timestamp` 早的事件。
     * 这可能会导致过期模式被丢弃或超时。
     *
     * @param nfaState 需要推进时间的 `NFA` 状态
     * @param timestamp 新的时间戳
     * @throws Exception 可能抛出的异常
     */
    private void advanceTime(NFAState nfaState, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
            // `NFA` 处理时间推进，返回匹配的模式和超时的匹配
            Tuple2<
                    Collection<Map<String, List<IN>>>,
                    Collection<Tuple2<Map<String, List<IN>>, Long>>>
                    pendingMatchesAndTimeout =
                    nfa.advanceTime(
                            sharedBufferAccessor,
                            nfaState,
                            timestamp,
                            afterMatchSkipStrategy);

            // 获取所有匹配到的事件序列
            Collection<Map<String, List<IN>>> pendingMatches = pendingMatchesAndTimeout.f0;
            // 获取超时的事件序列
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut = pendingMatchesAndTimeout.f1;

            // 处理匹配成功的模式
            if (!pendingMatches.isEmpty()) {
                processMatchedSequences(pendingMatches, timestamp);
            }

            // 处理超时的模式
            if (!timedOut.isEmpty()) {
                processTimedOutSequences(timedOut);
            }
        }
    }


    /**
     * 处理匹配到的事件序列，并调用用户定义的 `PatternProcessFunction` 进行处理。
     *
     * @param matchingSequences 匹配到的事件序列，每个匹配模式都会包含多个事件
     * @param timestamp 事件时间戳
     * @throws Exception 可能抛出的异常
     */
    private void processMatchedSequences(
            Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
        // 获取用户定义的 `PatternProcessFunction`
        PatternProcessFunction<IN, OUT> function = getUserFunction();

        // 设置当前匹配的时间戳
        setTimestamp(timestamp);

        // 遍历所有匹配到的事件序列，并调用用户的 `processMatch` 方法处理
        for (Map<String, List<IN>> matchingSequence : matchingSequences) {
            function.processMatch(matchingSequence, context, collector);
        }
    }

    /**
     * 处理超时未匹配的事件序列。
     * 当某个匹配窗口超时，可能会触发超时事件，用户可以自定义处理逻辑。
     *
     * @param timedOutSequences 超时的匹配序列，每个 `Tuple2` 包含：
     *                          - `Map<String, List<IN>>`：超时的事件序列
     *                          - `Long`：超时时间戳
     * @throws Exception 可能抛出的异常
     */
    private void processTimedOutSequences(
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
        // 获取用户自定义的 `PatternProcessFunction`
        PatternProcessFunction<IN, OUT> function = getUserFunction();

        // 检查用户定义的函数是否实现了 `TimedOutPartialMatchHandler`
        if (function instanceof TimedOutPartialMatchHandler) {

            @SuppressWarnings("unchecked")
            TimedOutPartialMatchHandler<IN> timeoutHandler =
                    (TimedOutPartialMatchHandler<IN>) function;

            // 遍历所有超时的匹配序列，并调用 `processTimedOutMatch`
            for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
                setTimestamp(matchingSequence.f1);
                timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
            }
        }
    }

    /**
     * 设置当前时间戳，用于在 `Event Time` 模式下确保匹配事件的时间正确性。
     * 处理 `Processing Time` 模式时，时间戳不会影响 `StreamRecord` 处理逻辑。
     *
     * @param timestamp 需要设置的时间戳
     */
    private void setTimestamp(long timestamp) {
        if (!isProcessingTime) {
            collector.setAbsoluteTimestamp(timestamp);
        }
        context.setTimestamp(timestamp);
    }

    /**
     * 计时服务 `TimerServiceImpl` 的实现，用于 `NFA` 访问 `InternalTimerService` 以及
     * 获取当前的 `Processing Time`。
     */
    private class TimerServiceImpl implements TimerService {

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

    /**
     * `PatternProcessFunction.Context` 的实现，用于管理 `PatternProcessFunction`
     * 的运行时环境。
     *
     * 该类主要提供：
     * - 获取当前 `Processing Time`
     * - 获取当前记录的时间戳
     * - 允许用户向 `Side Output` 发送数据
     */
    private class ContextFunctionImpl implements PatternProcessFunction.Context {

        private Long timestamp;

        /**
         * 发送数据到 `Side Output` 侧输出流，并根据 `Event Time` 或 `Processing Time` 设置时间戳。
         *
         * @param outputTag `OutputTag` 标记，指定 Side Output 的类型
         * @param value 需要输出的数据
         * @param <X> 输出数据的类型
         */
        @Override
        public <X> void output(final OutputTag<X> outputTag, final X value) {
            final StreamRecord<X> record;
            if (isProcessingTime) {
                record = new StreamRecord<>(value);
            } else {
                record = new StreamRecord<>(value, timestamp());
            }
            output.collect(outputTag, record);
        }

        /**
         * 设置当前的时间戳
         *
         * @param timestamp 需要设置的时间戳
         */
        void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        /**
         * 获取当前记录的时间戳
         *
         * @return 当前记录的时间戳
         */
        @Override
        public long timestamp() {
            return timestamp;
        }

        /**
         * 获取当前 `Processing Time`
         *
         * @return 当前 `Processing Time`
         */
        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

////////////////////// 测试方法（仅用于单元测试） //////////////////////

    /**
     * 检查 `SharedBuffer` 是否非空，即是否存在部分匹配的事件。
     *
     * @param key 需要检查的 Key
     * @return `true` 表示 `SharedBuffer` 中有未完成匹配的事件
     * @throws Exception 可能抛出的异常
     */
    @VisibleForTesting
    boolean hasNonEmptySharedBuffer(KEY key) throws Exception {
        setCurrentKey(key);
        return !partialMatches.isEmpty();
    }

    /**
     * 检查 `elementQueueState` 是否非空，即是否有等待处理的事件。
     *
     * @param key 需要检查的 Key
     * @return `true` 表示有待处理的事件
     * @throws Exception 可能抛出的异常
     */
    @VisibleForTesting
    boolean hasNonEmptyPQ(KEY key) throws Exception {
        setCurrentKey(key);
        return !elementQueueState.isEmpty();
    }

    /**
     * 获取 `elementQueueState` 中的事件数量，即当前 Key 下待处理的事件个数。
     *
     * @param key 需要检查的 Key
     * @return 事件队列中的事件数量
     * @throws Exception 可能抛出的异常
     */
    @VisibleForTesting
    int getPQSize(KEY key) throws Exception {
        setCurrentKey(key);
        int counter = 0;
        for (List<IN> elements : elementQueueState.values()) {
            counter += elements.size();
        }
        return counter;
    }

    /**
     * 获取已丢弃的迟到数据数量。
     *
     * @return 迟到数据的计数
     */
    @VisibleForTesting
    long getLateRecordsNumber() {
        return numLateRecordsDropped.getCount();
    }

}
