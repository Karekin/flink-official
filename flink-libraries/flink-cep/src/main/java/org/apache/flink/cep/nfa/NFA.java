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

package org.apache.flink.cep.nfa;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

import static org.apache.flink.cep.nfa.MigrationUtils.deserializeComputationStates;

/**
 * 非确定有限状态自动机（NFA, Non-deterministic Finite Automaton）的实现，
 * 用于 Flink 复杂事件处理（CEP）中的模式匹配。
 *
 * <p>在 Flink 的 CEP 处理逻辑中：
 * <ul>
 *     <li>对于 Keyed Stream（基于 key 进行分区的流），每个 key 维护一个独立的 NFA 实例。</li>
 *     <li>对于非 Keyed Stream，全局维护一个 NFA 实例。</li>
 * </ul>
 *
 * <p>事件流进入时，NFA 会根据模式规则更新其内部状态机，并决定是否匹配成功。
 *
 * <p>部分匹配成功的事件会存储在 {@link SharedBuffer}（共享缓冲区）中，
 * 该缓冲区是专门为高效管理部分匹配事件而设计的内存优化数据结构。
 * 事件会在以下情况下从缓冲区中移除：
 * <ol>
 *     <li>匹配成功并输出（emitted）。</li>
 *     <li>匹配失败，例如包含 NOT 逻辑的模式（discarded）。</li>
 *     <li>超时，适用于设置了时间窗口的模式（timed-out）。</li>
 * </ol>
 *
 * <p>本实现基于论文 "Efficient Pattern Matching over Event Streams"。
 *
 * @param <T> 处理的事件类型
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 *      论文链接</a>
 */
public class NFA<T> {

    /**
     * 存储所有有效的 NFA 状态，由 {@link NFACompiler} 生成。
     * 这些状态是由用户定义的模式（Pattern）直接派生出来的。
     * Key 为状态名称，Value 为对应的状态对象。
     */
    private final Map<String, State<T>> states;

    /**
     * 存储 NFA 每个状态对应的窗口时间（适用于 `Pattern.within(Time, WithinType)` 方法）。
     * 仅当 `WithinType` 设为 `PREVIOUS_AND_CURRENT` 时生效。
     * Key 为状态名称，Value 为该状态的时间窗口长度（毫秒）。
     */
    private final Map<String, Long> windowTimes;

    /**
     * 整个模式的时间窗口长度（适用于 `Pattern.within(Time)` 方法）。
     * 该值表示整个匹配过程允许的最长时间（毫秒）。
     */
    private final long windowTime;

    /**
     * 指示是否需要输出超时的匹配模式：
     * <ul>
     *     <li>`true` - 超时匹配的模式仍会被输出。</li>
     *     <li>`false` - 超时的匹配模式会被静默丢弃。</li>
     * </ul>
     */
    private final boolean handleTimeout;

    /**
     * 构造 NFA（非确定有限状态机）。
     *
     * @param validStates   NFA 中所有的有效状态集合（由 NFACompiler 生成）。
     * @param windowTimes   各状态的窗口时间映射（仅在 `WithinType.PREVIOUS_AND_CURRENT` 下有效）。
     * @param windowTime    全局模式匹配的时间窗口（适用于 `Pattern.within(Time)` 方法）。
     * @param handleTimeout 是否在模式超时时仍然输出匹配结果。
     */
    public NFA(
            final Collection<State<T>> validStates,
            final Map<String, Long> windowTimes,
            final long windowTime,
            final boolean handleTimeout) {
        this.windowTime = windowTime;  // 设置全局窗口时间
        this.handleTimeout = handleTimeout;  // 设置超时处理策略
        this.states = loadStates(validStates);  // 加载 NFA 状态
        this.windowTimes = windowTimes;  // 记录每个状态的窗口时间
    }

    /**
     * 将用户定义的模式转换为 NFA 状态集合。
     *
     * @param validStates NFA 中的有效状态集合。
     * @return 以状态名称为 key、状态对象为 value 的不可变映射。
     */
    private Map<String, State<T>> loadStates(final Collection<State<T>> validStates) {
        Map<String, State<T>> tmp = CollectionUtil.newHashMapWithExpectedSize(4);
        for (State<T> state : validStates) {
            tmp.put(state.getName(), state);
        }
        return Collections.unmodifiableMap(tmp); // 返回不可变 Map，防止外部修改
    }

    /**
     * 获取当前 NFA 模式匹配的窗口时间（适用于 `Pattern.within(Time)` 方法）。
     *
     * @return 当前模式匹配允许的最大时间窗口（毫秒）。
     */
    public long getWindowTime() {
        return windowTime;
    }

    /**
     * 仅用于测试，获取 NFA 所有状态的集合。
     *
     * @return NFA 状态集合。
     */
    @VisibleForTesting
    public Collection<State<T>> getStates() {
        return states.values();
    }

    /**
     * 创建 NFA 的初始状态。初始状态是所有 `isStart()` 返回 `true` 的状态。
     *
     * @return 初始 NFA 状态（包含所有起始状态的队列）。
     */
    public NFAState createInitialNFAState() {
        Queue<ComputationState> startingStates = new LinkedList<>();
        for (State<T> state : states.values()) {
            if (state.isStart()) {
                // 将起始状态加入队列
                startingStates.add(ComputationState.createStartState(state.getName()));
            }
        }
        return new NFAState(startingStates);
    }

    /**
     * 根据 `ComputationState` 获取对应的 `State` 对象。
     *
     * @param state 计算状态
     * @return 该计算状态对应的 `State`。
     */
    private State<T> getState(ComputationState state) {
        return states.get(state.getCurrentStateName());
    }

    /**
     * 判断当前 `ComputationState` 是否是起始状态（Start State）。
     *
     * @param state 计算状态
     * @return 如果该状态是起始状态，则返回 `true`，否则返回 `false`。
     * @throws FlinkRuntimeException 如果状态不存在。
     */
    private boolean isStartState(ComputationState state) {
        State<T> stateObject = getState(state);
        if (stateObject == null) {
            throw new FlinkRuntimeException(
                    "State "
                            + state.getCurrentStateName()
                            + " does not exist in the NFA. NFA has states "
                            + states.values());
        }
        return stateObject.isStart();
    }

    /**
     * 判断当前 `ComputationState` 是否是停止状态（Stop State）。
     *
     * @param state 计算状态
     * @return 如果该状态是停止状态，则返回 `true`，否则返回 `false`。
     * @throws FlinkRuntimeException 如果状态不存在。
     */
    private boolean isStopState(ComputationState state) {
        State<T> stateObject = getState(state);
        if (stateObject == null) {
            throw new FlinkRuntimeException(
                    "State "
                            + state.getCurrentStateName()
                            + " does not exist in the NFA. NFA has states "
                            + states.values());
        }
        return stateObject.isStop();
    }

    /**
     * 判断当前 `ComputationState` 是否是最终状态（Final State）。
     *
     * @param state 计算状态
     * @return 如果该状态是最终状态，则返回 `true`，否则返回 `false`。
     * @throws FlinkRuntimeException 如果状态不存在。
     */
    private boolean isFinalState(ComputationState state) {
        State<T> stateObject = getState(state);
        if (stateObject == null) {
            throw new FlinkRuntimeException(
                    "State "
                            + state.getCurrentStateName()
                            + " does not exist in the NFA. NFA has states "
                            + states.values());
        }
        return stateObject.isFinal();
    }

    /**
     * 初始化 NFA，在处理第一个事件前调用。用于执行模式匹配的初始化工作。
     *
     * @param cepRuntimeContext CEP 运行时环境。
     * @param conf 运行时的配置参数。
     * @throws Exception 发生异常时抛出。
     */
    public void open(RuntimeContext cepRuntimeContext, Configuration conf) throws Exception {
        for (State<T> state : getStates()) {
            for (StateTransition<T> transition : state.getStateTransitions()) {
                // 处理状态转移条件
                IterativeCondition condition = transition.getCondition();
                FunctionUtils.setFunctionRuntimeContext(condition, cepRuntimeContext);
                FunctionUtils.openFunction(condition, DefaultOpenContext.INSTANCE);
            }
        }
    }

    /**
     * 关闭 NFA，在 Flink 任务停止时调用。
     *
     * @throws Exception 发生异常时抛出。
     */
    public void close() throws Exception {
        for (State<T> state : getStates()) {
            for (StateTransition<T> transition : state.getStateTransitions()) {
                IterativeCondition condition = transition.getCondition();
                FunctionUtils.closeFunction(condition);
            }
        }
    }

    /**
     * 处理输入事件，执行 NFA 状态迁移。如果某些计算达到最终状态，则返回匹配结果。
     *
     * <p>如果启用了超时处理（`handleTimeout=true`），超时的部分匹配也会被返回。
     * 如果某个计算状态到达 `Stop State`，则当前路径被丢弃，并返回当前的匹配序列。
     *
     * @param sharedBufferAccessor 共享缓冲区的访问器，用于存储部分匹配的事件。
     * @param nfaState 当前的 NFA 状态，包括所有活动计算状态。
     * @param event 当前处理的事件（如果为 `null`，表示仅进行超时处理）。
     * @param timestamp 事件的时间戳。
     * @param afterMatchSkipStrategy 匹配成功后的跳过策略（用于控制匹配后的行为）。
     * @param timerService 计时器服务，提供时间控制能力。
     * @return 匹配成功的模式集合（如果有超时匹配，则包含超时匹配）。
     * @throws Exception 发生异常时抛出。
     */
    public Collection<Map<String, List<T>>> process(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final NFAState nfaState,
            final T event,
            final long timestamp,
            final AfterMatchSkipStrategy afterMatchSkipStrategy,
            final TimerService timerService)
            throws Exception {
        // 使用 try-with-resources 确保事件处理后释放资源
        try (EventWrapper eventWrapper = new EventWrapper(event, timestamp, sharedBufferAccessor)) {
            return doProcess(
                    sharedBufferAccessor,
                    nfaState,
                    eventWrapper,
                    afterMatchSkipStrategy,
                    timerService);
        }
    }


    /**
     * 该方法用于修剪（Prune）不再需要的状态，假设不会再收到比指定时间戳更早的事件。
     * 它会清理 `SharedBuffer`，并返回所有超时的部分匹配事件。
     *
     * @param sharedBufferAccessor 共享缓冲区访问器，用于操作 `SharedBuffer`
     * @param nfaState 当前 `NFAState`，其中包含所有活动的计算状态
     * @param timestamp 事件时间戳，表示该时间之前的事件不会再出现
     * @param afterMatchSkipStrategy 匹配后的跳过策略，用于确定如何跳过某些匹配
     * @return 包含匹配成功的模式集合和超时的部分匹配的 `Tuple2` 结构
     * @throws Exception 如果无法访问状态，则抛出异常
     */
    public Tuple2<Collection<Map<String, List<T>>>, Collection<Tuple2<Map<String, List<T>>, Long>>>
    advanceTime(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final NFAState nfaState,
            final long timestamp,
            final AfterMatchSkipStrategy afterMatchSkipStrategy)
            throws Exception {

        // 用于存储匹配成功的事件序列
        final List<Map<String, List<T>>> result = new ArrayList<>();
        // 用于存储超时的匹配
        final Collection<Tuple2<Map<String, List<T>>, Long>> timeoutResult = new ArrayList<>();
        // 用于存储仍然有效的部分匹配
        final PriorityQueue<ComputationState> newPartialMatches =
                new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);
        // 用于存储可能需要修剪的匹配
        final PriorityQueue<ComputationState> potentialMatches =
                new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

        for (ComputationState computationState : nfaState.getPartialMatches()) {
            String currentStateName = computationState.getCurrentStateName();

            // 检查当前计算状态是否超时（前一个事件超时）
            boolean isTimeoutForPreviousEvent =
                    windowTimes.containsKey(currentStateName)
                            && isStateTimedOut(
                            computationState,
                            timestamp,
                            computationState.getPreviousTimestamp(),
                            windowTimes.get(currentStateName));
            // 检查是否超出了全局窗口时间（起始事件超时）
            boolean isTimeoutForFirstEvent =
                    isStateTimedOut(
                            computationState,
                            timestamp,
                            computationState.getStartTimestamp(),
                            windowTime);

            if (isTimeoutForPreviousEvent || isTimeoutForFirstEvent) {
                nfaState.setStateChanged();

                if (getState(computationState).isPending()) {
                    // 如果状态是 `Pending`，则存入 `potentialMatches` 以供后续修剪
                    potentialMatches.add(computationState);
                    continue;
                }

                if (handleTimeout) {
                    // 提取超时的匹配模式
                    Map<String, List<T>> timedOutPattern =
                            sharedBufferAccessor.materializeMatch(
                                    extractCurrentMatches(sharedBufferAccessor, computationState));
                    timeoutResult.add(
                            Tuple2.of(
                                    timedOutPattern,
                                    isTimeoutForPreviousEvent
                                            ? computationState.getPreviousTimestamp()
                                            + windowTimes.get(
                                            computationState.getCurrentStateName())
                                            : computationState.getStartTimestamp() + windowTime));
                }

                // 释放超时状态
                sharedBufferAccessor.releaseNode(
                        computationState.getPreviousBufferEntry(), computationState.getVersion());
            } else {
                newPartialMatches.add(computationState);
            }
        }

        // 处理可能需要修剪的匹配
        processMatchesAccordingToSkipStrategy(
                sharedBufferAccessor,
                nfaState,
                afterMatchSkipStrategy,
                potentialMatches,
                newPartialMatches,
                result);

        nfaState.setNewPartialMatches(newPartialMatches);

        // 更新 `SharedBuffer` 的时间
        sharedBufferAccessor.advanceTime(timestamp);

        return Tuple2.of(result, timeoutResult);
    }

    /**
     * 判断给定 `ComputationState` 是否已超时。
     *
     * @param state 计算状态
     * @param timestamp 当前时间戳
     * @param startTimestamp 该状态开始的时间戳
     * @param windowTime 允许的窗口时间
     * @return 如果该状态已超时，则返回 `true`，否则返回 `false`
     */
    private boolean isStateTimedOut(
            final ComputationState state,
            final long timestamp,
            final long startTimestamp,
            final long windowTime) {
        return !isStartState(state) && windowTime > 0L && timestamp - startTimestamp >= windowTime;
    }

    /**
     * 处理当前事件，计算所有可能的状态转换，并根据匹配策略筛选匹配结果。
     *
     * @param sharedBufferAccessor 共享缓冲区访问器，用于存储和检索匹配数据
     * @param nfaState 当前 NFA 状态，包含所有活动计算状态
     * @param event 当前处理的事件包装器
     * @param afterMatchSkipStrategy 匹配成功后的跳过策略
     * @param timerService 提供时间相关的功能
     * @return 匹配成功的事件序列集合
     * @throws Exception 如果无法访问状态，则抛出异常
     */
    private Collection<Map<String, List<T>>> doProcess(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final NFAState nfaState,
            final EventWrapper event,
            final AfterMatchSkipStrategy afterMatchSkipStrategy,
            final TimerService timerService)
            throws Exception {

        // 用于存储新的部分匹配状态，即尚未完成但可以继续匹配的状态
        final PriorityQueue<ComputationState> newPartialMatches =
                new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

        // 用于存储已完成匹配的状态，即满足整个模式匹配规则的事件序列
        PriorityQueue<ComputationState> potentialMatches =
                new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

        // 遍历当前 NFA 状态中的所有活动计算状态
        for (ComputationState computationState : nfaState.getPartialMatches()) {
            // 计算当前状态在给定事件下的所有可能转换
            final Collection<ComputationState> newComputationStates =
                    computeNextStates(sharedBufferAccessor, computationState, event, timerService);

            // 如果产生了新的计算状态，则标记 NFA 状态发生了变化
            if (newComputationStates.size() != 1) {
                nfaState.setStateChanged();
            } else if (!newComputationStates.iterator().next().equals(computationState)) {
                nfaState.setStateChanged();
            }

            // 存储需要保留的计算状态，以便在下一次事件处理时继续
            final Collection<ComputationState> statesToRetain = new ArrayList<>();
            // 标记是否需要丢弃当前路径
            boolean shouldDiscardPath = false;

            // 遍历所有新计算状态，决定如何处理它们
            for (final ComputationState newComputationState : newComputationStates) {

                // 如果新状态是起始状态，并且有有效的起始时间戳，则标记新的部分匹配
                if (isStartState(computationState) && newComputationState.getStartTimestamp() > 0) {
                    nfaState.setNewStartPartiailMatch();
                }

                // 如果新状态是最终状态，则添加到潜在匹配集合
                if (isFinalState(newComputationState)) {
                    potentialMatches.add(newComputationState);
                }
                // 如果新状态是终止状态，则释放该状态的关联节点
                else if (isStopState(newComputationState)) {
                    shouldDiscardPath = true;
                    sharedBufferAccessor.releaseNode(
                            newComputationState.getPreviousBufferEntry(),
                            newComputationState.getVersion());
                }
                // 其他情况，将新状态保留到 `statesToRetain`，以便下一事件处理时使用
                else {
                    statesToRetain.add(newComputationState);
                }
            }

            // 如果路径被标记为丢弃，则释放该路径上所有状态的关联资源
            if (shouldDiscardPath) {
                for (final ComputationState state : statesToRetain) {
                    sharedBufferAccessor.releaseNode(
                            state.getPreviousBufferEntry(), state.getVersion());
                }
            } else {
                // 否则，将新计算状态加入部分匹配状态集合，等待下一次事件匹配
                newPartialMatches.addAll(statesToRetain);
            }
        }

        // 如果发现了潜在的完整匹配状态，则标记 NFA 状态发生了变化
        if (!potentialMatches.isEmpty()) {
            nfaState.setStateChanged();
        }

        // 存储最终匹配结果
        List<Map<String, List<T>>> result = new ArrayList<>();

        // 根据跳过策略处理匹配结果
        processMatchesAccordingToSkipStrategy(
                sharedBufferAccessor,
                nfaState,
                afterMatchSkipStrategy,
                potentialMatches,
                newPartialMatches,
                result);

        // 更新 NFA 状态，存储新的部分匹配状态
        nfaState.setNewPartialMatches(newPartialMatches);

        // 返回匹配的事件序列集合
        return result;
    }


    /**
     * 处理匹配结果，并根据 `AfterMatchSkipStrategy` 进行跳过策略处理。
     *
     * @param sharedBufferAccessor 共享缓冲区访问器
     * @param nfaState 当前 `NFAState`，其中存储了所有部分匹配和完整匹配
     * @param afterMatchSkipStrategy 匹配后的跳过策略，用于确定是否跳过某些匹配
     * @param potentialMatches 可能匹配的状态（需要检查是否应该跳过）
     * @param partialMatches 部分匹配的状态
     * @param result 存储最终的匹配结果
     * @throws Exception 可能抛出的异常
     */
    private void processMatchesAccordingToSkipStrategy(
            SharedBufferAccessor<T> sharedBufferAccessor,
            NFAState nfaState,
            AfterMatchSkipStrategy afterMatchSkipStrategy,
            PriorityQueue<ComputationState> potentialMatches,
            PriorityQueue<ComputationState> partialMatches,
            List<Map<String, List<T>>> result)
            throws Exception {

        // 将所有可能的匹配状态加入到 `completedMatches`，等待进一步检查
        nfaState.getCompletedMatches().addAll(potentialMatches);

        ComputationState earliestMatch;
        // 持续检查最早的匹配状态
        while ((earliestMatch = nfaState.getCompletedMatches().peek()) != null) {

            // 只有在 `afterMatchSkipStrategy` 不是 NO_SKIP 时才需要考虑顺序问题
            if (afterMatchSkipStrategy.isSkipStrategy()) {
                ComputationState earliestPartialMatch = partialMatches.peek();
                if (earliestPartialMatch != null
                        && !isEarlier(earliestMatch, earliestPartialMatch)) {
                    break;  // 终止处理，等待更早的部分匹配
                }
            }

            // 标记 `NFAState` 发生了变更
            nfaState.setStateChanged();
            // 从 `completedMatches` 队列中取出匹配成功的状态
            nfaState.getCompletedMatches().poll();

            // 提取匹配到的事件序列
            List<Map<String, List<EventId>>> matchedResult =
                    sharedBufferAccessor.extractPatterns(
                            earliestMatch.getPreviousBufferEntry(), earliestMatch.getVersion());

            // 根据跳过策略修剪部分匹配
            afterMatchSkipStrategy.prune(partialMatches, matchedResult, sharedBufferAccessor);
            // 根据跳过策略修剪已完成的匹配
            afterMatchSkipStrategy.prune(
                    nfaState.getCompletedMatches(), matchedResult, sharedBufferAccessor);

            // 将匹配结果转换为可读格式并存储
            result.add(sharedBufferAccessor.materializeMatch(matchedResult.get(0)));
            // 释放 `SharedBuffer` 资源，避免占用过多内存
            sharedBufferAccessor.releaseNode(
                    earliestMatch.getPreviousBufferEntry(), earliestMatch.getVersion());
        }

        // 移除所有已经完成的匹配状态（不再需要跟踪）
        nfaState.getPartialMatches()
                .removeIf(pm -> pm.getStartEventID() != null && !partialMatches.contains(pm));
    }

    /**
     * 比较两个 `ComputationState`，判断 `earliestMatch` 是否比 `earliestPartialMatch` 更早发生。
     *
     * @param earliestMatch 最早匹配成功的状态
     * @param earliestPartialMatch 最早的部分匹配状态
     * @return 如果 `earliestMatch` 发生时间早于或等于 `earliestPartialMatch`，返回 `true`，否则返回 `false`
     */
    private boolean isEarlier(
            ComputationState earliestMatch, ComputationState earliestPartialMatch) {
        return NFAState.COMPUTATION_STATE_COMPARATOR.compare(earliestMatch, earliestPartialMatch)
                <= 0;
    }

    /**
     * 检查两个状态是否等价，即它们的名称是否相同。
     *
     * @param s1 状态1
     * @param s2 状态2
     * @return 若状态名称相同，返回 `true`，否则返回 `false`
     */
    private static <T> boolean isEquivalentState(final State<T> s1, final State<T> s2) {
        return s1.getName().equals(s2.getName());
    }

    /**
     * 用于存储解析出的状态转换信息，记录了 `IGNORE` 和 `TAKE` 类型的分支数量。
     *
     * @param <T> 事件类型
     */
    private static class OutgoingEdges<T> {
        private List<StateTransition<T>> edges = new ArrayList<>();
        private final State<T> currentState;
        private int totalTakeBranches = 0;
        private int totalIgnoreBranches = 0;

        OutgoingEdges(final State<T> currentState) {
            this.currentState = currentState;
        }

        /**
         * 添加状态转换，并记录 `IGNORE` 和 `TAKE` 类型的转换数量。
         *
         * @param edge 需要添加的状态转换
         */
        void add(StateTransition<T> edge) {
            if (!isSelfIgnore(edge)) {
                if (edge.getAction() == StateTransitionAction.IGNORE) {
                    totalIgnoreBranches++;
                } else if (edge.getAction() == StateTransitionAction.TAKE) {
                    totalTakeBranches++;
                }
            }
            edges.add(edge);
        }

        /** 获取 `IGNORE` 类型的转换数量。 */
        int getTotalIgnoreBranches() {
            return totalIgnoreBranches;
        }

        /** 获取 `TAKE` 类型的转换数量。 */
        int getTotalTakeBranches() {
            return totalTakeBranches;
        }

        /** 获取所有的状态转换。 */
        List<StateTransition<T>> getEdges() {
            return edges;
        }

        /**
         * 检查是否是自身的 `IGNORE` 变换，即目标状态与当前状态相同，并且转换类型是 `IGNORE`。
         *
         * @param edge 状态转换
         * @return 若是自身 `IGNORE` 变换，返回 `true`，否则返回 `false`
         */
        private boolean isSelfIgnore(final StateTransition<T> edge) {
            return isEquivalentState(edge.getTargetState(), currentState)
                    && edge.getAction() == StateTransitionAction.IGNORE;
        }
    }

    /**
     * `EventWrapper` 类用于包装事件，并确保事件只在生命周期内注册一次，并在关闭时释放。
     * 该类使用 `try-with-resources` 语法来确保事件的自动释放。
     */
    private class EventWrapper implements AutoCloseable {

        private final T event;
        private long timestamp;
        private final SharedBufferAccessor<T> sharedBufferAccessor;
        private EventId eventId;

        /**
         * 构造 `EventWrapper`，用于包装事件。
         *
         * @param event 事件对象
         * @param timestamp 事件的时间戳
         * @param sharedBufferAccessor 共享缓冲区访问器
         */
        EventWrapper(T event, long timestamp, SharedBufferAccessor<T> sharedBufferAccessor) {
            this.event = event;
            this.timestamp = timestamp;
            this.sharedBufferAccessor = sharedBufferAccessor;
        }

        /**
         * 获取 `EventId`，如果尚未注册事件，则进行注册。
         *
         * @return 事件的 `EventId`
         * @throws Exception 可能抛出的异常
         */
        EventId getEventId() throws Exception {
            if (eventId == null) {
                this.eventId = sharedBufferAccessor.registerEvent(event, timestamp);
            }
            return eventId;
        }

        /** 获取事件对象。 */
        T getEvent() {
            return event;
        }

        /** 获取事件时间戳。 */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * 关闭 `EventWrapper` 时释放事件，确保 `SharedBuffer` 资源不会被占用。
         *
         * @throws Exception 可能抛出的异常
         */
        @Override
        public void close() throws Exception {
            if (eventId != null) {
                sharedBufferAccessor.releaseEvent(eventId);
            }
        }
    }


    /**
     * 计算当前 `ComputationState` 的下一个计算状态，基于当前事件、时间戳以及内部状态机的定义。
     *
     * 该方法的主要执行逻辑如下：
     * 1. 确定当前状态可以进行的有效状态转换（`OutgoingEdges`）。
     * 2. 处理状态转换：
     *    - `IGNORE`：忽略当前事件，状态保持不变，或者跳转到新状态但不消耗当前事件。
     *      - 不能对 `Start State` 进行 `IGNORE` 操作（特殊情况）。
     *      - 如果仍然停留在同一个状态，则增加 `DeweyNumber` 版本。
     *      - 若 `IGNORE` 发生在 `PROCEED` 之后，则增加 `DeweyNumber` 阶段，并更新版本信息。
     *      - 锁定 `SharedBuffer` 的相关数据，确保状态不会被意外释放。
     *    - `TAKE`：消耗当前事件，并转换到新的状态。
     *      - 在 `SharedBuffer` 中存储新版本的计算状态。
     *      - 若新状态具有 `PROCEED` 路径到 `Final State`，则创建最终计算状态，并输出匹配结果。
     * 3. 处理 `Start State`，确保它始终存在。
     * 4. 释放 `SharedBuffer` 中不再需要的 `NodeId`，优化资源使用。
     *
     * @param sharedBufferAccessor `SharedBuffer` 访问器，用于管理状态存储
     * @param computationState 当前计算状态
     * @param event 当前处理的事件
     * @param timerService 计时服务，提供时间相关的功能
     * @return 返回从当前计算状态转换出的所有新 `ComputationState`
     * @throws Exception 可能抛出的异常
     */
    private Collection<ComputationState> computeNextStates(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final ComputationState computationState,
            final EventWrapper event,
            final TimerService timerService)
            throws Exception {

        // 创建 `ConditionContext`，用于存储匹配条件的相关信息
        final ConditionContext context =
                new ConditionContext(
                        sharedBufferAccessor, computationState, timerService, event.getTimestamp());

        // 计算当前状态的所有可能转换，并存储到 `OutgoingEdges`
        final OutgoingEdges<T> outgoingEdges =
                createDecisionGraph(context, computationState, event.getEvent());

        // 获取状态转换的所有边
        final List<StateTransition<T>> edges = outgoingEdges.getEdges();
        int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
        int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();
        int totalTakeToSkip = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);

        // 存储计算后的新状态
        final List<ComputationState> resultingComputationStates = new ArrayList<>();
        for (StateTransition<T> edge : edges) {
            switch (edge.getAction()) {
                case IGNORE:
                {
                    if (!isStartState(computationState)) {
                        final DeweyNumber version;
                        if (isEquivalentState(
                                edge.getTargetState(), getState(computationState))) {
                            // 停留在相同状态（可能是循环或单一状态）
                            final int toIncrease =
                                    calculateIncreasingSelfState(
                                            outgoingEdges.getTotalIgnoreBranches(),
                                            outgoingEdges.getTotalTakeBranches());
                            version = computationState.getVersion().increase(toIncrease);
                        } else {
                            // `PROCEED` 之后的 `IGNORE`
                            version =
                                    computationState
                                            .getVersion()
                                            .increase(totalTakeToSkip + ignoreBranchesToVisit)
                                            .addStage();
                            ignoreBranchesToVisit--;
                        }

                        // 添加新的计算状态
                        addComputationState(
                                sharedBufferAccessor,
                                resultingComputationStates,
                                edge.getTargetState(),
                                computationState.getPreviousBufferEntry(),
                                version,
                                computationState.getStartTimestamp(),
                                computationState.getPreviousTimestamp(),
                                computationState.getStartEventID());
                    }
                }
                break;
                case TAKE:
                    final State<T> nextState = edge.getTargetState();
                    final State<T> currentState = edge.getSourceState();

                    final NodeId previousEntry = computationState.getPreviousBufferEntry();

                    // 更新 `DeweyNumber` 版本
                    final DeweyNumber currentVersion =
                            computationState.getVersion().increase(takeBranchesToVisit);
                    final DeweyNumber nextVersion = new DeweyNumber(currentVersion).addStage();
                    takeBranchesToVisit--;

                    // 在 `SharedBuffer` 中存储新的匹配记录
                    final NodeId newEntry =
                            sharedBufferAccessor.put(
                                    currentState.getName(),
                                    event.getEventId(),
                                    previousEntry,
                                    currentVersion);

                    // 计算 `startTimestamp` 和 `startEventId`
                    final long startTimestamp;
                    final EventId startEventId;
                    if (isStartState(computationState)) {
                        startTimestamp = event.getTimestamp();
                        startEventId = event.getEventId();
                    } else {
                        startTimestamp = computationState.getStartTimestamp();
                        startEventId = computationState.getStartEventID();
                    }
                    final long previousTimestamp = event.getTimestamp();

                    // 添加新计算状态
                    addComputationState(
                            sharedBufferAccessor,
                            resultingComputationStates,
                            nextState,
                            newEntry,
                            nextVersion,
                            startTimestamp,
                            previousTimestamp,
                            startEventId);

                    // 检查新状态是否可以直接跳转到 `Final State`
                    final State<T> finalState =
                            findFinalStateAfterProceed(context, nextState, event.getEvent());
                    if (finalState != null) {
                        addComputationState(
                                sharedBufferAccessor,
                                resultingComputationStates,
                                finalState,
                                newEntry,
                                nextVersion,
                                startTimestamp,
                                previousTimestamp,
                                startEventId);
                    }
                    break;
            }
        }

        // 确保 `Start State` 始终存在
        if (isStartState(computationState)) {
            int totalBranches =
                    calculateIncreasingSelfState(
                            outgoingEdges.getTotalIgnoreBranches(),
                            outgoingEdges.getTotalTakeBranches());

            DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
            ComputationState startState =
                    ComputationState.createStartState(
                            computationState.getCurrentStateName(), startVersion);
            resultingComputationStates.add(startState);
        }

        // 释放 `SharedBuffer` 中的 `NodeId`
        if (computationState.getPreviousBufferEntry() != null) {
            sharedBufferAccessor.releaseNode(
                    computationState.getPreviousBufferEntry(), computationState.getVersion());
        }

        return resultingComputationStates;
    }

    /**
     * 添加一个新的 `ComputationState` 到计算状态列表。
     *
     * @param sharedBufferAccessor `SharedBuffer` 访问器
     * @param computationStates 存储新计算状态的列表
     * @param currentState 目标状态
     * @param previousEntry 之前的 `NodeId`
     * @param version `DeweyNumber` 版本信息
     * @param startTimestamp 匹配起始时间戳
     * @param previousTimestamp 当前事件的时间戳
     * @param startEventId 匹配的起始事件 ID
     * @throws Exception 可能抛出的异常
     */
    private void addComputationState(
            SharedBufferAccessor<T> sharedBufferAccessor,
            List<ComputationState> computationStates,
            State<T> currentState,
            NodeId previousEntry,
            DeweyNumber version,
            long startTimestamp,
            long previousTimestamp,
            EventId startEventId)
            throws Exception {
        ComputationState computationState =
                ComputationState.createState(
                        currentState.getName(),
                        previousEntry,
                        version,
                        startTimestamp,
                        previousTimestamp,
                        startEventId);
        computationStates.add(computationState);

        // 锁定 `SharedBuffer`，确保不会被提前释放
        sharedBufferAccessor.lockNode(previousEntry, computationState.getVersion());
    }


    /**
     * 查找从当前状态通过 `PROCEED` 方式可以到达的最终状态 (`Final State`)。
     *
     * <p>遍历 `PROCEED` 方式的状态转换路径，直到找到 `Final State`，否则返回 `null`。
     *
     * @param context 计算状态的条件上下文
     * @param state 当前状态
     * @param event 当前处理的事件
     * @return 若找到 `Final State` 则返回，否则返回 `null`
     */
    private State<T> findFinalStateAfterProceed(ConditionContext context, State<T> state, T event) {
        final Stack<State<T>> statesToCheck = new Stack<>();
        statesToCheck.push(state);
        try {
            while (!statesToCheck.isEmpty()) {
                final State<T> currentState = statesToCheck.pop();
                for (StateTransition<T> transition : currentState.getStateTransitions()) {
                    // 仅处理 `PROCEED` 类型的转换，并检查是否符合条件
                    if (transition.getAction() == StateTransitionAction.PROCEED
                            && checkFilterCondition(context, transition.getCondition(), event)) {
                        // 若目标状态是 `Final State`，则返回
                        if (transition.getTargetState().isFinal()) {
                            return transition.getTargetState();
                        } else {
                            // 继续检查下一个 `PROCEED` 目标状态
                            statesToCheck.push(transition.getTargetState());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failure happened in filter function.", e);
        }

        return null;
    }

    /**
     * 计算 `IGNORE` 和 `TAKE` 规则下的版本增长量。
     *
     * <p>若没有 `IGNORE` 和 `TAKE` 规则，则返回 `0`。
     * 否则，`IGNORE` 数量加上 `TAKE` 数量的最大值（`TAKE` 至少为 1）。
     *
     * @param ignoreBranches `IGNORE` 规则的数量
     * @param takeBranches `TAKE` 规则的数量
     * @return 计算后的版本增长量
     */
    private int calculateIncreasingSelfState(int ignoreBranches, int takeBranches) {
        return takeBranches == 0 && ignoreBranches == 0
                ? 0
                : ignoreBranches + Math.max(1, takeBranches);
    }

    /**
     * 生成当前状态的所有有效状态转换决策图 (`OutgoingEdges`)。
     *
     * <p>该方法遍历当前状态的所有转换，并筛选出符合条件的 `IGNORE`、`TAKE` 和 `PROCEED` 规则。
     *
     * @param context 计算状态的条件上下文
     * @param computationState 当前计算状态
     * @param event 当前处理的事件
     * @return `OutgoingEdges` 结构，包含所有符合条件的转换路径
     */
    private OutgoingEdges<T> createDecisionGraph(
            ConditionContext context, ComputationState computationState, T event) {
        State<T> state = getState(computationState);
        final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(state);

        final Stack<State<T>> states = new Stack<>();
        states.push(state);

        // 遍历所有可能的转换，构建决策图
        while (!states.isEmpty()) {
            State<T> currentState = states.pop();
            Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

            for (StateTransition<T> stateTransition : stateTransitions) {
                try {
                    // 若事件符合转换条件，则添加到决策图
                    if (checkFilterCondition(context, stateTransition.getCondition(), event)) {
                        switch (stateTransition.getAction()) {
                            case PROCEED:
                                // `PROCEED` 直接推进状态（等同于 ε-转换）
                                states.push(stateTransition.getTargetState());
                                break;
                            case IGNORE:
                            case TAKE:
                                outgoingEdges.add(stateTransition);
                                break;
                        }
                    }
                } catch (Exception e) {
                    throw new FlinkRuntimeException("Failure happened in filter function.", e);
                }
            }
        }
        return outgoingEdges;
    }

    /**
     * 检查当前事件是否满足给定的过滤条件。
     *
     * <p>若 `condition` 为空，则默认返回 `true`。
     *
     * @param context 计算状态的条件上下文
     * @param condition `IterativeCondition` 过滤条件
     * @param event 当前处理的事件
     * @return 若符合条件返回 `true`，否则返回 `false`
     * @throws Exception 可能抛出的异常
     */
    private boolean checkFilterCondition(
            ConditionContext context, IterativeCondition<T> condition, T event) throws Exception {
        return condition == null || condition.filter(event, context);
    }

    /**
     * 提取从 `Start State` 到 `computationState` 的所有事件序列。
     *
     * <p>每个事件序列存储为 `Map<String, List<EventId>>`，其中：
     * - `key` 是 `Pattern` 中的状态名称
     * - `List<EventId>` 是该状态下的匹配事件 ID 列表
     *
     * @param sharedBufferAccessor 访问 `SharedBuffer` 以提取匹配信息
     * @param computationState 终止计算状态，即匹配路径的终点
     * @return 匹配到的事件序列映射
     * @throws Exception 可能抛出的异常
     */
    private Map<String, List<EventId>> extractCurrentMatches(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final ComputationState computationState)
            throws Exception {
        // 若 `computationState` 没有前序 `BufferEntry`，说明未匹配到事件，返回空 Map
        if (computationState.getPreviousBufferEntry() == null) {
            return new HashMap<>();
        }

        // 从 `SharedBuffer` 中提取匹配路径
        List<Map<String, List<EventId>>> paths =
                sharedBufferAccessor.extractPatterns(
                        computationState.getPreviousBufferEntry(), computationState.getVersion());

        // 若 `paths` 为空，说明没有匹配到事件
        if (paths.isEmpty()) {
            return new HashMap<>();
        }
        // 一个 `ComputationState` 只应有一个匹配路径
        Preconditions.checkState(paths.size() == 1);

        return paths.get(0);
    }


    /**
     * 用于评估计算状态（ComputationState）的上下文类。
     *
     * 该类为 Flink 复杂事件处理（CEP）模块中的条件评估（Condition Evaluation）提供支持，
     * 允许访问匹配的事件、时间信息等，以便在 `IterativeCondition` 过滤规则中使用。
     */
    private class ConditionContext implements IterativeCondition.Context<T> {

        private final TimerService timerService; // 定时器服务，提供时间相关功能

        private final long eventTimestamp; // 当前事件的时间戳

        /** 当前计算状态（ComputationState），用于存储匹配状态信息 */
        private ComputationState computationState;

        /**
         * 已匹配的事件集合 (`Pattern`)。
         * 该变量仅在 `getEventsForPattern` 方法调用时初始化，以避免不必要的计算开销。
         */
        private Map<String, List<T>> matchedEvents;

        private SharedBufferAccessor<T> sharedBufferAccessor; // 共享缓冲区访问器

        /**
         * 构造 `ConditionContext`，用于评估计算状态的匹配情况。
         *
         * @param sharedBufferAccessor 共享缓冲区访问器
         * @param computationState 当前计算状态
         * @param timerService 定时器服务
         * @param eventTimestamp 当前事件的时间戳
         */
        ConditionContext(
                final SharedBufferAccessor<T> sharedBufferAccessor,
                final ComputationState computationState,
                final TimerService timerService,
                final long eventTimestamp) {
            this.computationState = computationState;
            this.sharedBufferAccessor = sharedBufferAccessor;
            this.timerService = timerService;
            this.eventTimestamp = eventTimestamp;
        }

        /**
         * 获取指定模式（Pattern）匹配的事件列表。
         *
         * @param key 事件模式的键
         * @return 匹配该模式的事件集合
         * @throws Exception 可能抛出的异常
         */
        @Override
        public Iterable<T> getEventsForPattern(final String key) throws Exception {
            Preconditions.checkNotNull(key);

            // 仅在第一次调用时从 `SharedBuffer` 提取匹配事件
            if (matchedEvents == null) {
                this.matchedEvents =
                        sharedBufferAccessor.materializeMatch(
                                extractCurrentMatches(sharedBufferAccessor, computationState));
            }

            return new Iterable<T>() {
                @Override
                public Iterator<T> iterator() {
                    List<T> elements = matchedEvents.get(key);
                    return elements == null
                            ? Collections.emptyIterator()
                            : elements.iterator();
                }
            };
        }

        /**
         * 获取当前事件的时间戳。
         *
         * @return 事件的时间戳
         */
        @Override
        public long timestamp() {
            return eventTimestamp;
        }

        /**
         * 获取当前处理时间（Processing Time）。
         *
         * @return 处理时间
         */
        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

////////////////////  旧版 NFA 迁移工具类  ////////////////////

    /**
     * 旧版本 `NFA`（非确定有限状态机，Non-deterministic Finite Automaton）的迁移包装类。
     *
     * 该类封装了 `ComputationState`（计算状态队列）和 `SharedBuffer`（共享缓冲区），
     * 用于支持 Flink 复杂事件处理（CEP）中的模式匹配逻辑。
     */
    public static class MigratedNFA<T> {

        private final Queue<ComputationState> computationStates; // 计算状态队列
        private final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer; // 共享缓冲区

        /**
         * 获取共享缓冲区 `SharedBuffer`。
         *
         * @return `SharedBuffer` 对象
         */
        public org.apache.flink.cep.nfa.SharedBuffer<T> getSharedBuffer() {
            return sharedBuffer;
        }

        /**
         * 获取计算状态队列。
         *
         * @return `ComputationState` 队列
         */
        public Queue<ComputationState> getComputationStates() {
            return computationStates;
        }

        /**
         * 构造 `MigratedNFA` 对象。
         *
         * @param computationStates 计算状态队列
         * @param sharedBuffer 共享缓冲区
         */
        MigratedNFA(
                final Queue<ComputationState> computationStates,
                final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer) {
            this.sharedBuffer = sharedBuffer;
            this.computationStates = computationStates;
        }
    }

    /**
     * `NFASerializer` 的 `TypeSerializerSnapshot`，用于旧版 NFA 数据结构的迁移。
     *
     * 该类用于 `NFASerializer` 在不同 Flink 版本之间的数据兼容性，
     * 通过存储 `NFASerializer` 的序列化信息，使得旧版数据结构能够正确解析。
     */
    @SuppressWarnings("deprecation")
    public static final class MigratedNFASerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<MigratedNFA<T>, NFASerializer<T>> {

        private static final int VERSION = 2; // 迁移版本号

        /**
         * 默认构造函数。
         */
        public MigratedNFASerializerSnapshot() {}

        /**
         * 通过 `NFASerializer` 创建 `MigratedNFASerializerSnapshot` 实例。
         *
         * @param legacyNfaSerializer 旧版 `NFASerializer`
         */
        MigratedNFASerializerSnapshot(NFASerializer<T> legacyNfaSerializer) {
            super(legacyNfaSerializer);
        }

        /**
         * 获取当前序列化器的版本号。
         *
         * @return 版本号
         */
        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        /**
         * 获取 `NFASerializer` 的嵌套序列化器列表。
         *
         * @param outerSerializer 外层 `NFASerializer`
         * @return 由 `NFASerializer` 依赖的内部序列化器数组
         */
        @Override
        protected TypeSerializer<?>[] getNestedSerializers(NFASerializer<T> outerSerializer) {
            return new TypeSerializer<?>[] {
                    outerSerializer.eventSerializer, outerSerializer.sharedBufferSerializer
            };
        }

        /**
         * 通过嵌套序列化器创建 `NFASerializer`。
         *
         * @param nestedSerializers 嵌套的序列化器数组
         * @return 重新创建的 `NFASerializer`
         */
        @Override
        protected NFASerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> eventSerializer = (TypeSerializer<T>) nestedSerializers[0];

            @SuppressWarnings("unchecked")
            TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer =
                    (TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>>) nestedSerializers[1];

            return new NFASerializer<>(eventSerializer, sharedBufferSerializer);
        }
    }


    /** Only for backward compatibility with <=1.5. */
    @Deprecated
    public static class NFASerializer<T> extends TypeSerializer<MigratedNFA<T>> {

        private static final long serialVersionUID = 2098282423980597010L;

        private final TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>>
                sharedBufferSerializer;

        private final TypeSerializer<T> eventSerializer;

        public NFASerializer(TypeSerializer<T> typeSerializer) {
            this(
                    typeSerializer,
                    new org.apache.flink.cep.nfa.SharedBuffer.SharedBufferSerializer<>(
                            StringSerializer.INSTANCE, typeSerializer));
        }

        NFASerializer(
                TypeSerializer<T> typeSerializer,
                TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {
            this.eventSerializer = typeSerializer;
            this.sharedBufferSerializer = sharedBufferSerializer;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public NFASerializer<T> duplicate() {
            return new NFASerializer<>(eventSerializer.duplicate());
        }

        @Override
        public MigratedNFA<T> createInstance() {
            return null;
        }

        @Override
        public MigratedNFA<T> copy(MigratedNFA<T> from) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MigratedNFA<T> copy(MigratedNFA<T> from, MigratedNFA<T> reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(MigratedNFA<T> record, DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MigratedNFA<T> deserialize(DataInputView source) throws IOException {
            MigrationUtils.skipSerializedStates(source);
            source.readLong();
            source.readBoolean();

            org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer =
                    sharedBufferSerializer.deserialize(source);
            Queue<ComputationState> computationStates =
                    deserializeComputationStates(sharedBuffer, eventSerializer, source);

            return new MigratedNFA<>(computationStates, sharedBuffer);
        }

        @Override
        public MigratedNFA<T> deserialize(MigratedNFA<T> reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || (obj != null
                            && obj.getClass().equals(getClass())
                            && sharedBufferSerializer.equals(
                                    ((NFASerializer) obj).sharedBufferSerializer)
                            && eventSerializer.equals(((NFASerializer) obj).eventSerializer));
        }

        @Override
        public int hashCode() {
            return 37 * sharedBufferSerializer.hashCode() + eventSerializer.hashCode();
        }

        @Override
        public MigratedNFASerializerSnapshot<T> snapshotConfiguration() {
            return new MigratedNFASerializerSnapshot<>(this);
        }
    }
}
