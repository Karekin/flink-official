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

package org.apache.flink.cep.nfa.compiler;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.StateTransition;
import org.apache.flink.cep.nfa.StateTransitionAction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.MalformedPatternException;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.Quantifier.Times;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.cep.pattern.conditions.BooleanConditions;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichAndCondition;
import org.apache.flink.cep.pattern.conditions.RichNotCondition;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static org.apache.flink.cep.nfa.compiler.NFAStateNameHandler.STATE_NAME_DELIM;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 编译器类，包含将 {@link Pattern} 编译为 {@link NFA} 或 {@link NFAFactory} 的方法。
 * 该类用于解析事件模式，并生成相应的状态机（NFA）。
 */
public class NFACompiler {

    /** 终止状态（结束状态）的名称 */
    protected static final String ENDING_STATE_NAME = "$endState$";

    /**
     * 将给定的模式编译为 {@link NFAFactory}，该工厂可用于创建多个 NFA（非确定性有限自动机）。
     *
     * @param pattern 事件序列模式定义
     * @param timeoutHandling 是否启用超时处理（如果启用，NFA 将返回超时事件模式）
     * @param <T> 输入事件的类型
     * @return 生成的 NFA 工厂
     */
    @SuppressWarnings("unchecked")
    public static <T> NFAFactory<T> compileFactory(
            final Pattern<T, ?> pattern, boolean timeoutHandling) {
        if (pattern == null) {
            // 返回一个空的 NFA 工厂
            return new NFAFactoryImpl<>(
                    0,
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    timeoutHandling);
        } else {
            // 创建 NFA 编译器
            final NFAFactoryCompiler<T> nfaFactoryCompiler = new NFAFactoryCompiler<>(pattern);
            // 编译模式，构造 NFA
            nfaFactoryCompiler.compileFactory();
            return new NFAFactoryImpl<>(
                    nfaFactoryCompiler.getWindowTime(),
                    nfaFactoryCompiler.getWindowTimes(),
                    nfaFactoryCompiler.getStates(),
                    timeoutHandling);
        }
    }

    /**
     * 检查给定的模式是否可能匹配空事件序列。
     * 例如：A*、A?、A* B? 等模式可能会匹配空序列。
     *
     * @param pattern 需要检查的模式
     * @return 如果模式可能匹配空事件，则返回 true，否则返回 false
     */
    public static boolean canProduceEmptyMatches(final Pattern<?, ?> pattern) {
        NFAFactoryCompiler<?> compiler = new NFAFactoryCompiler<>(checkNotNull(pattern));
        compiler.compileFactory();
        // 查找起始状态
        State<?> startState =
                compiler.getStates().stream()
                        .filter(State::isStart)
                        .findFirst()
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        "编译器未生成起始状态，这是一个 Bug，请提交 Jira 报告。"));

        // 深度优先搜索（DFS）遍历状态机，检查是否存在空匹配路径
        Set<State<?>> visitedStates = new HashSet<>();
        final Stack<State<?>> statesToCheck = new Stack<>();
        statesToCheck.push(startState);

        while (!statesToCheck.isEmpty()) {
            final State<?> currentState = statesToCheck.pop();
            if (visitedStates.contains(currentState)) {
                continue;
            } else {
                visitedStates.add(currentState);
            }

            for (StateTransition<?> transition : currentState.getStateTransitions()) {
                if (transition.getAction() == StateTransitionAction.PROCEED) {
                    // 如果目标状态是终止状态，说明可能匹配空事件
                    if (transition.getTargetState().isFinal()) {
                        return true;
                    } else {
                        statesToCheck.push(transition.getTargetState());
                    }
                }
            }
        }

        return false;
    }

    /**
     * 内部编译器类，用于将 {@link Pattern} 转换为 {@link State} 的有向图结构，以便在多个方法之间共享编译状态。
     * 该类的作用是解析模式并构建状态机（NFA）。
     *
     * @param <T> 输入事件的类型
     */
    static class NFAFactoryCompiler<T> {

        /** 处理状态名称的工具类，确保状态名称唯一性 */
        private final NFAStateNameHandler stateNameHandler = new NFAStateNameHandler();
        /** 存储 NOT 状态（禁止匹配的状态） */
        private final Map<String, State<T>> stopStates = new HashMap<>();
        /** 存储已创建的状态列表 */
        private final List<State<T>> states = new ArrayList<>();
        /** 记录状态之间的窗口时间 */
        private final Map<String, Long> windowTimes = new HashMap<>();

        /** 可选的窗口时间，表示整个模式的超时时间 */
        private Optional<Long> windowTime;
        /** 当前处理的分组模式（GroupPattern），用于处理复杂的模式组合 */
        private GroupPattern<T, ?> currentGroupPattern;
        /** 记录循环模式（Looping Pattern）的状态 */
        private Map<GroupPattern<T, ?>, Boolean> firstOfLoopMap = new HashMap<>();
        /** 当前正在处理的模式 */
        private Pattern<T, ?> currentPattern;
        /** 当前模式的下一个模式 */
        private Pattern<T, ?> followingPattern;
        /** 事件匹配后如何跳过的策略（如 SkipPastLastEvent, SkipToNextEvent） */
        private final AfterMatchSkipStrategy afterMatchSkipStrategy;
        /** 记录原始状态映射（用于处理 Greedy 量词等情况） */
        private Map<String, State<T>> originalStateMap = new HashMap<>();

        /**
         * 构造函数，初始化 NFA 编译器。
         *
         * @param pattern 需要编译的事件模式
         */
        NFAFactoryCompiler(final Pattern<T, ?> pattern) {
            this.currentPattern = pattern;
            this.afterMatchSkipStrategy = pattern.getAfterMatchSkipStrategy();
            this.windowTime = Optional.empty();
        }

        /**
         * 编译模式为 NFA，转换模式定义为状态机。
         */
        void compileFactory() {
            Pattern<T, ?> lastPattern = currentPattern;

            // 校验模式名称的唯一性，防止冲突
            checkPatternNameUniqueness();
            // 校验 AfterMatchSkipStrategy（匹配后的跳过策略）
            checkPatternSkipStrategy();

            // 逆向遍历模式（从末尾开始），最终状态为终止状态
            State<T> sinkState = createEndingState();
            // 创建中间状态
            sinkState = createMiddleStates(sinkState);
            // 创建起始状态
            createStartState(sinkState);

            // 检查模式中的窗口时间约束
            checkPatternWindowTimes();

            // 验证 NotFollowedBy 不能出现在没有窗口时间的模式末尾
            if (lastPattern.getQuantifier().getConsumingStrategy()
                    == Quantifier.ConsumingStrategy.NOT_FOLLOW
                    && (!windowTimes.containsKey(lastPattern.getName())
                    || windowTimes.get(lastPattern.getName()) <= 0)
                    && getWindowTime() == 0) {
                throw new MalformedPatternException(
                        "NotFollowedBy（未跟随模式）不能在没有窗口时间的情况下出现在模式的最后部分！");
            }
        }

        /**
         * 获取当前模式匹配后的跳过策略。
         *
         * @return AfterMatchSkipStrategy 对象
         */
        AfterMatchSkipStrategy getAfterMatchSkipStrategy() {
            return afterMatchSkipStrategy;
        }

        /**
         * 获取编译后的状态列表。
         *
         * @return 状态列表
         */
        List<State<T>> getStates() {
            return states;
        }

        /**
         * 获取模式的窗口时间（匹配整个模式的最长时间限制）。
         *
         * @return 窗口时间（毫秒）
         */
        long getWindowTime() {
            return windowTime.orElse(0L);
        }

        /**
         * 获取每个状态的窗口时间映射（状态名 -> 窗口时间）。
         *
         * @return 窗口时间映射
         */
        Map<String, Long> getWindowTimes() {
            return windowTimes;
        }

        /**
         * 检查模式定义中的窗口时间约束，确保每个状态的时间约束不会超过全局窗口时间。
         */
        private void checkPatternWindowTimes() {
            windowTime.ifPresent(
                    windowTime -> {
                        if (windowTimes.values().stream().anyMatch(time -> time > windowTime)) {
                            throw new MalformedPatternException(
                                    "模式中的局部窗口时间不能超过整个模式的窗口时间！");
                        }
                    });
        }

        /**
         * 检查 AfterMatchSkipStrategy 中指定的模式名称是否存在于模式定义中。
         */
        private void checkPatternSkipStrategy() {
            if (afterMatchSkipStrategy.getPatternName().isPresent()) {
                String patternName = afterMatchSkipStrategy.getPatternName().get();
                Pattern<T, ?> pattern = currentPattern;
                while (pattern.getPrevious() != null && !pattern.getName().equals(patternName)) {
                    pattern = pattern.getPrevious();
                }

                if (!pattern.getName().equals(patternName)) {
                    throw new MalformedPatternException(
                            "AfterMatchSkipStrategy 中指定的模式名称在当前模式中不存在！");
                }
            }
        }


        /**
         * 检查模式名称的唯一性。如果存在重复名称，则抛出 {@link MalformedPatternException}。
         */
        private void checkPatternNameUniqueness() {
            // 确保不存在名为 "$endState$" 的模式，该名称保留给终止状态
            stateNameHandler.checkNameUniqueness(ENDING_STATE_NAME);

            // 遍历整个模式链，检查所有模式的名称是否唯一
            Pattern patternToCheck = currentPattern;
            while (patternToCheck != null) {
                checkPatternNameUniqueness(patternToCheck);
                patternToCheck = patternToCheck.getPrevious();
            }

            // 清除名称记录，以备下次使用
            stateNameHandler.clear();
        }

        /**
         * 检查指定模式的名称是否唯一。如果名称已被使用，则抛出 {@link MalformedPatternException}。
         *
         * @param pattern 需要检查的模式
         */
        private void checkPatternNameUniqueness(final Pattern pattern) {
            if (pattern instanceof GroupPattern) {
                // 如果模式是一个 GroupPattern（模式组合），需要检查其内部模式的名称
                Pattern patternToCheck = ((GroupPattern) pattern).getRawPattern();
                while (patternToCheck != null) {
                    checkPatternNameUniqueness(patternToCheck);
                    patternToCheck = patternToCheck.getPrevious();
                }
            } else {
                // 直接检查单个模式的名称是否唯一
                stateNameHandler.checkNameUniqueness(pattern.getName());
            }
        }

        /**
         * 获取当前模式链中的 NOT 模式（未匹配条件）及其对应的条件。
         *
         * <p>有两种情况会导致 NOT 条件：
         * <ol>
         *   <li>前一个模式的 Quantifier 消费策略为 {@link Quantifier.ConsumingStrategy#NOT_FOLLOW}
         *   <li>存在一条回溯路径，该路径由 {@link Quantifier.QuantifierProperty#OPTIONAL}（可选模式）
         *       连接至 {@link Quantifier.ConsumingStrategy#NOT_FOLLOW} 模式
         * </ol>
         *
         * <p><b>警告：</b> 对于第二种情况，详细信息请参考 {@link NFAFactoryCompiler#copyWithoutTransitiveNots(State)}
         *
         * @return 由 NOT 条件和对应模式名称组成的列表
         */
        private List<Tuple2<IterativeCondition<T>, String>> getCurrentNotCondition() {
            List<Tuple2<IterativeCondition<T>, String>> notConditions = new ArrayList<>();

            // 追溯前一个模式，检查是否存在 NOT_FOLLOW 关系
            Pattern<T, ? extends T> previousPattern = currentPattern;
            while (previousPattern.getPrevious() != null
                    && (previousPattern.getPrevious().getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL)
                    || previousPattern.getPrevious().getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW)) {

                previousPattern = previousPattern.getPrevious();

                // 如果当前模式的 Quantifier 具有 NOT_FOLLOW 消费策略，则记录 NOT 条件
                if (previousPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW) {
                    final IterativeCondition<T> notCondition = getTakeCondition(previousPattern);
                    notConditions.add(Tuple2.of(notCondition, previousPattern.getName()));
                }
            }
            return notConditions;
        }

        /**
         * 创建 NFA 图的终止状态（Final State）。
         *
         * @return 终止状态
         */
        private State<T> createEndingState() {
            // 创建终止状态，名称固定为 "$endState$"
            State<T> endState = createState(ENDING_STATE_NAME, State.StateType.Final);

            // 从当前模式中提取窗口时间（如果有）
            windowTime = currentPattern.getWindowSize().map(Duration::toMillis);

            return endState;
        }

        /**
         * 创建从起始状态到终止状态之间的所有中间状态。
         *
         * @param sinkState 最终状态，所有路径都应导向该状态（通常是终止状态）
         * @return 构建后的第一个中间状态
         */
        private State<T> createMiddleStates(final State<T> sinkState) {
            State<T> lastSink = sinkState;

            while (currentPattern.getPrevious() != null) {
                if (currentPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_FOLLOW) {
                    // 忽略 NOT_FOLLOW 模式，它们会被转换为边上的条件
                    if ((currentPattern.getWindowSize(WithinType.PREVIOUS_AND_CURRENT).isPresent()
                            || getWindowTime() > 0) && lastSink.isFinal()) {
                        final State<T> notFollow = createState(State.StateType.Pending, true);
                        final IterativeCondition<T> notCondition = getTakeCondition(currentPattern);
                        final State<T> stopState = createStopState(notCondition, currentPattern.getName());

                        notFollow.addProceed(stopState, notCondition);
                        notFollow.addIgnore(new RichNotCondition<>(notCondition));
                        lastSink = notFollow;
                    }
                } else if (currentPattern.getQuantifier().getConsumingStrategy() == Quantifier.ConsumingStrategy.NOT_NEXT) {
                    // 处理 NOT_NEXT 逻辑
                    final State<T> notNext = createState(State.StateType.Normal, true);
                    final IterativeCondition<T> notCondition = getTakeCondition(currentPattern);
                    final State<T> stopState = createStopState(notCondition, currentPattern.getName());

                    if (lastSink.isFinal()) {
                        // 避免在最终状态执行 PROCEED
                        notNext.addIgnore(lastSink, new RichNotCondition<>(notCondition));
                    } else {
                        notNext.addProceed(lastSink, new RichNotCondition<>(notCondition));
                    }
                    notNext.addProceed(stopState, notCondition);
                    lastSink = notNext;
                } else {
                    // 处理常规模式转换
                    lastSink = convertPattern(lastSink);
                }

                // 逆向遍历模式（从后向前）
                followingPattern = currentPattern;
                currentPattern = currentPattern.getPrevious();

                // 计算窗口时间，取所有模式窗口时间的最小值
                currentPattern.getWindowSize()
                        .map(Duration::toMillis)
                        .filter(windowSizeInMillis -> windowSizeInMillis < windowTime.orElse(Long.MAX_VALUE))
                        .ifPresent(windowSizeInMillis -> windowTime = Optional.of(windowSizeInMillis));
            }

            return lastSink;
        }

        /**
         * 创建 NFA 的起始状态（Start State）。
         *
         * @param sinkState 起始状态应指向的下一个状态（通常是第一个中间状态）
         * @return 构建的起始状态
         */
        @SuppressWarnings("unchecked")
        private State<T> createStartState(State<T> sinkState) {
            // 将第一个模式转换为状态
            final State<T> beginningState = convertPattern(sinkState);
            // 标记起始状态
            beginningState.makeStart();
            return beginningState;
        }

        /**
         * 将当前模式转换为 NFA 状态。
         *
         * @param sinkState 目标状态，即当前模式应连接到的状态
         * @return 生成的状态
         */
        private State<T> convertPattern(final State<T> sinkState) {
            final State<T> lastSink;

            final Quantifier quantifier = currentPattern.getQuantifier();
            if (quantifier.hasProperty(Quantifier.QuantifierProperty.LOOPING)) {
                // 处理循环（LOOP）模式
                setCurrentGroupPatternFirstOfLoop(false);
                final State<T> sink = copyWithoutTransitiveNots(sinkState);
                final State<T> looping = createLooping(sink);

                setCurrentGroupPatternFirstOfLoop(true);
                lastSink = createTimesState(looping, sinkState, currentPattern.getTimes());
            } else if (quantifier.hasProperty(Quantifier.QuantifierProperty.TIMES)) {
                // 处理 TIMES（限定次数重复）模式
                lastSink = createTimesState(sinkState, sinkState, currentPattern.getTimes());
            } else {
                // 处理普通模式
                lastSink = createSingletonState(sinkState);
            }

            // 添加 STOP 状态（如果适用）
            addStopStates(lastSink);

            return lastSink;
        }


        /**
         * 创建一个新的状态对象，并在需要时记录窗口时间。
         *
         * @param stateType 状态的类型（例如 Normal, Final, Stop）。
         * @param isTake 该状态是否为 "TAKE"（即该状态是否消耗事件）。
         * @return 创建的状态对象。
         */
        private State<T> createState(State.StateType stateType, boolean isTake) {
            // 根据当前模式的名称创建状态
            State<T> state = createState(currentPattern.getName(), stateType);

            // 如果该状态会消耗事件（TAKE），可能需要记录窗口时间
            if (isTake) {
                Times times = currentPattern.getTimes();
                Optional<Duration> windowSize = currentPattern.getWindowSize(WithinType.PREVIOUS_AND_CURRENT);

                // 如果模式不是 TIMES（即不限制匹配次数），则直接记录窗口时间
                if (times == null) {
                    windowSize.map(Duration::toMillis).ifPresent(
                            windowSizeInMillis -> windowTimes.put(state.getName(), windowSizeInMillis)
                    );
                }
                // 如果状态名称包含分隔符（即它属于一个复杂模式），则检查该模式的窗口时间
                else if (state.getName().contains(STATE_NAME_DELIM)) {
                    times.getWindowSize().map(Duration::toMillis).ifPresent(
                            windowSizeInMillis -> windowTimes.put(state.getName(), windowSizeInMillis)
                    );
                }
            }
            return state;
        }

        /**
         * 创建一个状态，并将其添加到状态集合中。
         * 该方法应优先于直接使用 new 操作符创建状态。
         *
         * @param name 状态的名称。
         * @param stateType 状态的类型（Normal, Stop, Final）。
         * @return 创建的状态对象。
         */
        private State<T> createState(String name, State.StateType stateType) {
            // 生成唯一的状态名称
            String stateName = stateNameHandler.getUniqueInternalName(name);

            // 创建新状态对象
            State<T> state = new State<>(stateName, stateType);

            // 将状态加入已创建状态列表
            states.add(state);

            return state;
        }

        /**
         * 创建一个 STOP 状态（用于处理 NOT 逻辑），如果已存在相同名称的 STOP 状态，则直接返回。
         *
         * @param notCondition NOT 条件，即未匹配模式的过滤条件。
         * @param name STOP 状态的名称。
         * @return 对应的 STOP 状态。
         */
        private State<T> createStopState(final IterativeCondition<T> notCondition, final String name) {
            // 检查该 STOP 状态是否已存在
            State<T> stopState = stopStates.get(name);
            if (stopState == null) {
                stopState = createState(name, State.StateType.Stop);
                stopState.addTake(notCondition);
                stopStates.put(name, stopState);
            }
            return stopState;
        }

        /**
         * 复制状态并去除所有与 NOT 相关的状态转换（仅在某些情况下需要）。
         * 当一个模式是可选的（OPTIONAL），且前面有 NOT 模式时，可能需要创建一个新的路径来避免误匹配。
         *
         * 例如，模式 `begin("a").notFollowedBy("b").followedByAny("c").optional().followedByAny("d")`
         * 允许匹配 `{a c b d}` 但不允许 `{a b d}`，因为 "c" 是可选的。
         *
         * @param sinkState 需要复制的状态。
         * @return 复制后的新状态；如果不需要复制，则返回原状态。
         */
        private State<T> copyWithoutTransitiveNots(final State<T> sinkState) {
            // 获取当前状态涉及的 NOT 条件
            final List<Tuple2<IterativeCondition<T>, String>> currentNotCondition = getCurrentNotCondition();

            // 如果当前模式不是 OPTIONAL，或者没有 NOT 条件，则直接返回原状态，无需复制
            if (currentNotCondition.isEmpty()
                    || !currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
                return sinkState;
            }

            // 创建原状态的副本
            final State<T> copyOfSink = createState(sinkState.getName(), sinkState.getStateType());

            // 遍历原状态的所有状态转换（StateTransition），去除 NOT 相关的转换
            for (StateTransition<T> tStateTransition : sinkState.getStateTransitions()) {
                if (tStateTransition.getAction() == StateTransitionAction.PROCEED) {
                    State<T> targetState = tStateTransition.getTargetState();
                    boolean remove = false;

                    // 检查目标状态是否是 STOP 状态，并且是否与当前 NOT 条件匹配
                    if (targetState.isStop()) {
                        for (Tuple2<IterativeCondition<T>, String> notCondition : currentNotCondition) {
                            if (targetState.getName().equals(notCondition.f1)) {
                                remove = true;
                            }
                        }
                    } else {
                        // 递归处理目标状态，去除 NOT 影响
                        targetState = copyWithoutTransitiveNots(tStateTransition.getTargetState());
                    }

                    // 仅保留未被删除的状态转换
                    if (!remove) {
                        copyOfSink.addStateTransition(
                                tStateTransition.getAction(),
                                targetState,
                                tStateTransition.getCondition());
                    }
                } else {
                    // 处理其他类型的状态转换
                    copyOfSink.addStateTransition(
                            tStateTransition.getAction(),
                            tStateTransition.getTargetState().equals(tStateTransition.getSourceState()) ?
                                    copyOfSink : tStateTransition.getTargetState(),
                            tStateTransition.getCondition());
                }
            }
            return copyOfSink;
        }

        /**
         * 创建并返回状态的副本（不改变状态转换）。
         *
         * @param state 需要复制的状态。
         * @return 复制后的新状态对象。
         */
        private State<T> copy(final State<T> state) {
            // 生成原始状态的唯一名称，并创建新的状态对象
            final State<T> copyOfState =
                    createState(
                            NFAStateNameHandler.getOriginalNameFromInternal(state.getName()),
                            state.getStateType());

            // 复制原状态的所有状态转换
            for (StateTransition<T> tStateTransition : state.getStateTransitions()) {
                copyOfState.addStateTransition(
                        tStateTransition.getAction(),
                        tStateTransition.getTargetState().equals(tStateTransition.getSourceState())
                                ? copyOfState
                                : tStateTransition.getTargetState(),
                        tStateTransition.getCondition());
            }
            return copyOfState;
        }


        /**
         * 为当前状态添加 STOP 状态，用于处理 NOT 模式的情况。
         *
         * 在模式匹配过程中，如果某个模式是不应该出现的（NOT_FOLLOWED_BY），
         * 那么在 NFA（非确定性有限自动机）中就需要一个 STOP 状态来处理这种情况。
         * 该方法会为当前状态添加 STOP 状态，以确保匹配逻辑的正确性。
         *
         * @param state 需要添加 STOP 状态的目标状态
         */
        private void addStopStates(final State<T> state) {
            for (Tuple2<IterativeCondition<T>, String> notCondition : getCurrentNotCondition()) {
                // 创建 STOP 状态
                final State<T> stopState = createStopState(notCondition.f0, notCondition.f1);
                // 添加到当前状态，使其可以跳转到 STOP 状态
                state.addProceed(stopState, notCondition.f0);
            }
        }

        /**
         * 为循环（LOOPING）状态添加 STOP 状态，确保 NOT_FOLLOWED_BY 逻辑的正确性。
         *
         * 该方法用于处理带有 NOT 约束的循环模式，例如 `A*` 但不能被 `B` 跟随。
         * 如果 `B` 在 `A*` 之后出现，则必须停止匹配，以避免错误的匹配结果。
         *
         * @param loopingState 需要添加 STOP 状态的循环状态
         */
        private void addStopStateToLooping(final State<T> loopingState) {
            // 确保后续模式是 NOT_FOLLOWED_BY 类型
            if (followingPattern != null
                    && followingPattern.getQuantifier().getConsumingStrategy()
                    == Quantifier.ConsumingStrategy.NOT_FOLLOW) {
                // 获取 NOT 约束的条件
                final IterativeCondition<T> notCondition = getTakeCondition(followingPattern);
                // 创建 STOP 状态
                final State<T> stopState = createStopState(notCondition, followingPattern.getName());
                // 让循环状态可以跳转到 STOP 状态
                loopingState.addProceed(stopState, notCondition);
            }
        }

        /**
         * 创建一个由多个相同条件的状态组成的 "复杂" 状态，用于 TIMES 模式（如 A{2,4}）。
         *
         * 该方法用于处理 `{min, max}` 形式的模式，例如：
         * - `A{2,4}` 代表 `A` 至少出现 2 次，最多出现 4 次。
         * - `B{1,3}` 代表 `B` 至少出现 1 次，最多出现 3 次。
         *
         * @param sinkState 该状态最终应该指向的目标状态
         * @param proceedState 该状态最终应该 PROCEED 到的状态
         * @param times TIMES 量词，表示状态应被匹配的次数范围
         * @return 复杂状态的起始状态
         */
        @SuppressWarnings("unchecked")
        private State<T> createTimesState(
                final State<T> sinkState, final State<T> proceedState, Times times) {
            State<T> lastSink = sinkState;

            // 设置当前 GroupPattern 是否为 LOOP 结构的头部
            setCurrentGroupPatternFirstOfLoop(false);

            // 获取 UNTIL 终止条件（如果存在）
            final IterativeCondition<T> untilCondition =
                    (IterativeCondition<T>) currentPattern.getUntilCondition();

            // 计算 IGNORE（忽略）条件
            final IterativeCondition<T> innerIgnoreCondition =
                    extendWithUntilCondition(getInnerIgnoreCondition(currentPattern), untilCondition, false);

            // 计算 TAKE（匹配）条件
            final IterativeCondition<T> takeCondition =
                    extendWithUntilCondition(getTakeCondition(currentPattern), untilCondition, true);

            // 如果是 GREEDY（贪心）模式，且 min ≠ max，则需要特殊处理
            if (currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)
                    && times.getFrom() != times.getTo()) {
                if (untilCondition != null) {
                    // 复制 sinkState 以支持贪心匹配
                    State<T> sinkStateCopy = copy(sinkState);
                    originalStateMap.put(sinkState.getName(), sinkStateCopy);
                }
                // 更新 GREEDY 模式下的状态转移条件
                updateWithGreedyCondition(sinkState, takeCondition);
            }

            // 创建最大匹配次数范围（max - min）的中间状态
            for (int i = times.getFrom(); i < times.getTo(); i++) {
                lastSink = createSingletonState(lastSink, proceedState, takeCondition, innerIgnoreCondition, true);
                addStopStateToLooping(lastSink);
            }

            // 创建最小匹配次数范围（min - 1）的中间状态
            for (int i = 0; i < times.getFrom() - 1; i++) {
                lastSink = createSingletonState(lastSink, null, takeCondition, innerIgnoreCondition, false);
                addStopStateToLooping(lastSink);
            }

            // 现在创建整个匹配结构的起始状态
            setCurrentGroupPatternFirstOfLoop(true);
            return createSingletonState(
                    lastSink, proceedState, takeCondition, getIgnoreCondition(currentPattern), isPatternOptional(currentPattern));
        }

        /**
         * 标记当前 GroupPattern 是否是 TIMES 量词（{n, m}）结构的头部。
         *
         * @param isFirstOfLoop 如果是 TIMES 结构的起始状态，则为 true，否则为 false
         */
        @SuppressWarnings("unchecked")
        private void setCurrentGroupPatternFirstOfLoop(boolean isFirstOfLoop) {
            if (currentPattern instanceof GroupPattern) {
                firstOfLoopMap.put((GroupPattern<T, ?>) currentPattern, isFirstOfLoop);
            }
        }

        /**
         * 判断当前 GroupPattern 是否是 TIMES/LOOPING（{n, m}）模式的头部。
         *
         * @return true 如果当前 GroupPattern 是 TIMES/LOOPING 模式的起始状态，否则返回 false
         */
        private boolean isCurrentGroupPatternFirstOfLoop() {
            return firstOfLoopMap.getOrDefault(currentGroupPattern, true);
        }

        /**
         * 判断给定的模式是否是 GroupPattern 的头部（即最左侧模式）。
         *
         * @param pattern 需要检查的模式
         * @return 如果该模式是 GroupPattern 的头部，则返回 true，否则返回 false
         */
        private boolean headOfGroup(Pattern<T, ?> pattern) {
            return currentGroupPattern != null && pattern.getPrevious() == null;
        }

        /**
         * 判断模式是否为 OPTIONAL（可选）。
         *
         * @param pattern 需要检查的模式
         * @return 如果该模式是可选的，则返回 true，否则返回 false
         */
        private boolean isPatternOptional(Pattern<T, ?> pattern) {
            return pattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL);
        }

        /**
         * 判断给定的模式是否是可选 GroupPattern（即 `{}` 或 `*` 的头部）。
         *
         * @param pattern 需要检查的模式
         * @return 如果该模式是可选 GroupPattern 的头部，则返回 true，否则返回 false
         */
        private boolean isHeadOfOptionalGroupPattern(Pattern<T, ?> pattern) {
            if (!headOfGroup(pattern)) {
                return false;
            }
            return isCurrentGroupPatternFirstOfLoop()
                    && currentGroupPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.OPTIONAL);
        }

        /**
         * 创建一个简单的单状态，用于普通模式匹配。
         * 对于 OPTIONAL（可选）模式，该方法会创建一个没有 PROCEED 连接的分支状态，以确保正确的分支逻辑。
         *
         * @param sinkState 该状态最终应指向的目标状态
         * @return 生成的单状态对象
         */
        @SuppressWarnings("unchecked")
        private State<T> createSingletonState(final State<T> sinkState) {
            return createSingletonState(
                    sinkState,
                    sinkState,
                    getTakeCondition(currentPattern),
                    getIgnoreCondition(currentPattern),
                    isPatternOptional(currentPattern));
        }


        /**
         * 创建一个简单的单状态（Singleton State）。
         *
         * - 对于可选（OPTIONAL）状态，还会创建一个不带 PROCEED（前进）边的类似状态，
         *   以确保计算状态图中的每条 PROCEED 迁移路径仅被创建一次。
         *
         * @param sinkState 该状态最终应指向的目标状态
         * @param proceedState 该状态最终应 PROCEED 到的状态
         * @param takeCondition 用于 TAKE 迁移的匹配条件
         * @param ignoreCondition 用于 IGNORE 迁移的匹配条件
         * @param isOptional 是否是可选（OPTIONAL）状态
         * @return 创建的单状态对象
         */
        @SuppressWarnings("unchecked")
        private State<T> createSingletonState(
                final State<T> sinkState,
                final State<T> proceedState,
                final IterativeCondition<T> takeCondition,
                final IterativeCondition<T> ignoreCondition,
                final boolean isOptional) {

            // 如果当前模式是 GroupPattern，则使用 GroupPattern 方式创建状态
            if (currentPattern instanceof GroupPattern) {
                return createGroupPatternState(
                        (GroupPattern<T, ?>) currentPattern, sinkState, proceedState, isOptional);
            }

            // 创建普通状态
            final State<T> singletonState = createState(State.StateType.Normal, true);

            // 如果事件被接受，那么之前的 NOT 约束就不再有效，因此创建一个不含 NOT 约束的目标状态
            final State<T> sink = copyWithoutTransitiveNots(sinkState);

            // 添加 TAKE 迁移（成功匹配）
            singletonState.addTake(sink, takeCondition);

            // 如果不匹配该元素，则 NOT 约束依然有效
            final IterativeCondition<T> proceedCondition = getTrueFunction();

            if (isOptional) {
                // 处理 GREEDY（贪心）匹配模式
                if (currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)) {
                    final IterativeCondition<T> untilCondition =
                            (IterativeCondition<T>) currentPattern.getUntilCondition();
                    if (untilCondition != null) {
                        singletonState.addProceed(
                                originalStateMap.get(proceedState.getName()),
                                new RichAndCondition<>(proceedCondition, untilCondition));
                    }
                    singletonState.addProceed(
                            proceedState,
                            untilCondition != null
                                    ? new RichAndCondition<>(proceedCondition, new RichNotCondition<>(untilCondition))
                                    : proceedCondition);
                } else {
                    singletonState.addProceed(proceedState, proceedCondition);
                }
            }

            // 处理 IGNORE（忽略）逻辑
            if (ignoreCondition != null) {
                final State<T> ignoreState;
                if (isOptional || isHeadOfOptionalGroupPattern(currentPattern)) {
                    // 如果是 OPTIONAL 模式或者是可选 GroupPattern 的头部，则创建 IGNORE 状态
                    ignoreState = createState(State.StateType.Normal, false);
                    ignoreState.addTake(sink, takeCondition);
                    ignoreState.addIgnore(ignoreCondition);
                    addStopStates(ignoreState);
                } else {
                    ignoreState = singletonState;
                }
                singletonState.addIgnore(ignoreState, ignoreCondition);
            }
            return singletonState;
        }

        /**
         * 为 GroupPattern（组模式）创建所有状态。
         *
         * @param groupPattern 需要转换的组模式
         * @param sinkState 该模式转换后应指向的目标状态
         * @param proceedState 该模式转换后应 PROCEED 到的状态
         * @param isOptional 该模式是否为 OPTIONAL（可选）
         * @return 组模式的第一个状态
         */
        private State<T> createGroupPatternState(
                final GroupPattern<T, ?> groupPattern,
                final State<T> sinkState,
                final State<T> proceedState,
                final boolean isOptional) {

            // 处理 GroupPattern 迁移时的默认 PROCEED 条件（永远返回 true）
            final IterativeCondition<T> proceedCondition = getTrueFunction();

            // 记录当前模式和组模式，避免影响递归调用
            Pattern<T, ?> oldCurrentPattern = currentPattern;
            Pattern<T, ?> oldFollowingPattern = followingPattern;
            GroupPattern<T, ?> oldGroupPattern = currentGroupPattern;

            // 递归构建 GroupPattern 内部的状态
            State<T> lastSink = sinkState;
            currentGroupPattern = groupPattern;
            currentPattern = groupPattern.getRawPattern();
            lastSink = createMiddleStates(lastSink);
            lastSink = convertPattern(lastSink);

            // 如果当前 GroupPattern 是可选的（OPTIONAL），
            // 则其 PROCEED 迁移路径应指向后续状态（proceedState）
            if (isOptional) {
                lastSink.addProceed(proceedState, proceedCondition);
            }

            // 恢复当前模式的上下文
            currentPattern = oldCurrentPattern;
            followingPattern = oldFollowingPattern;
            currentGroupPattern = oldGroupPattern;

            return lastSink;
        }

        /**
         * 为 GroupPattern（组模式）创建 LOOPING（循环）状态。
         *
         * 该方法用于处理 `A+` 形式的模式（即 `A` 至少出现一次，可以无限重复）。
         *
         * @param groupPattern 需要转换的组模式
         * @param sinkState 该模式转换后应指向的目标状态
         * @return 组模式的第一个状态
         */
        private State<T> createLoopingGroupPatternState(
                final GroupPattern<T, ?> groupPattern, final State<T> sinkState) {

            // 处理 GroupPattern 迁移时的默认 PROCEED 条件（永远返回 true）
            final IterativeCondition<T> proceedCondition = getTrueFunction();

            // 记录当前模式和组模式，避免影响递归调用
            Pattern<T, ?> oldCurrentPattern = currentPattern;
            Pattern<T, ?> oldFollowingPattern = followingPattern;
            GroupPattern<T, ?> oldGroupPattern = currentGroupPattern;

            // 创建一个空的 Dummy 状态，用于表示循环的起点
            final State<T> dummyState = createState(State.StateType.Normal, true);

            // 递归构建 GroupPattern 内部的状态
            State<T> lastSink = dummyState;
            currentGroupPattern = groupPattern;
            currentPattern = groupPattern.getRawPattern();
            lastSink = createMiddleStates(lastSink);
            lastSink = convertPattern(lastSink);

            // 循环模式的 PROCEED 迁移指向自身，形成循环
            lastSink.addProceed(sinkState, proceedCondition);
            dummyState.addProceed(lastSink, proceedCondition);

            // 恢复当前模式的上下文
            currentPattern = oldCurrentPattern;
            followingPattern = oldFollowingPattern;
            currentGroupPattern = oldGroupPattern;

            return lastSink;
        }


        /**
         * 创建一个 LOOPING（循环）状态。
         *
         * - LOOPING 状态是具有 TAKE 迁移（自循环）和 PROCEED 迁移（前往 sinkState）的状态。
         * - 还包含一个类似的状态，该状态没有 PROCEED 迁移，以确保计算状态图中的每个 PROCEED 迁移分支只被创建一次。
         *
         * @param sinkState 该循环状态应最终指向的目标状态
         * @return 生成的循环状态的起始状态
         */
        @SuppressWarnings("unchecked")
        private State<T> createLooping(final State<T> sinkState) {
            // 如果当前模式是 GroupPattern，则使用 GroupPattern 方式创建循环状态
            if (currentPattern instanceof GroupPattern) {
                return createLoopingGroupPatternState((GroupPattern<T, ?>) currentPattern, sinkState);
            }

            // 获取 UNTIL 条件（如果存在）
            final IterativeCondition<T> untilCondition =
                    (IterativeCondition<T>) currentPattern.getUntilCondition();

            // 计算 IGNORE 迁移（忽略事件）的条件
            final IterativeCondition<T> ignoreCondition =
                    extendWithUntilCondition(
                            getInnerIgnoreCondition(currentPattern), untilCondition, false);

            // 计算 TAKE 迁移（接受事件）的条件
            final IterativeCondition<T> takeCondition =
                    extendWithUntilCondition(
                            getTakeCondition(currentPattern), untilCondition, true);

            // 默认的 PROCEED 迁移条件（一般为 true，允许状态前进）
            IterativeCondition<T> proceedCondition = getTrueFunction();

            // 创建 LOOPING 状态
            final State<T> loopingState = createState(State.StateType.Normal, true);

            // 处理 GREEDY（贪心）模式
            if (currentPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)) {
                if (untilCondition != null) {
                    // 复制 sinkState 以确保在满足 UNTIL 条件时能够提前终止
                    State<T> sinkStateCopy = copy(sinkState);
                    loopingState.addProceed(
                            sinkStateCopy,
                            new RichAndCondition<>(proceedCondition, untilCondition));
                    originalStateMap.put(sinkState.getName(), sinkStateCopy);
                }
                // 继续向 sinkState 进行 PROCEED 迁移（不满足 UNTIL 时）
                loopingState.addProceed(
                        sinkState,
                        untilCondition != null
                                ? new RichAndCondition<>(proceedCondition, new RichNotCondition<>(untilCondition))
                                : proceedCondition);

                // 更新 sinkState，使其包含 GREEDY 条件
                updateWithGreedyCondition(sinkState, getTakeCondition(currentPattern));
            } else {
                // 非贪心模式，直接 PROCEED 迁移到 sinkState
                loopingState.addProceed(sinkState, proceedCondition);
            }

            // 添加 TAKE 迁移（事件匹配成功时的状态转移）
            loopingState.addTake(takeCondition);

            // 在 LOOPING 状态中添加 STOP 终止状态
            addStopStateToLooping(loopingState);

            // 如果存在 IGNORE 迁移（忽略事件时的状态转移）
            if (ignoreCondition != null) {
                final State<T> ignoreState = createState(State.StateType.Normal, false);
                ignoreState.addTake(loopingState, takeCondition);
                ignoreState.addIgnore(ignoreCondition);
                loopingState.addIgnore(ignoreState, ignoreCondition);

                // 在 IGNORE 状态中添加 STOP 终止状态
                addStopStateToLooping(ignoreState);
            }

            return loopingState;
        }

        /**
         * 扩展给定的条件，并与 UNTIL 条件结合（如果适用）。
         *
         * - 如果存在 UNTIL 条件，则使用 `RichAndCondition` 将其与当前条件结合，以确保匹配逻辑符合期望。
         * - 该方法主要用于构建 LOOPING 和 TIMES 状态，使其能够根据 UNTIL 规则进行匹配。
         *
         * @param condition 需要扩展的条件
         * @param untilCondition UNTIL 终止条件（可能为空）
         * @param isTakeCondition 该条件是否用于 TAKE 迁移（如果是，则需要特殊处理）
         * @return 结合 UNTIL 逻辑后的新条件
         */
        private IterativeCondition<T> extendWithUntilCondition(
                IterativeCondition<T> condition,
                IterativeCondition<T> untilCondition,
                boolean isTakeCondition) {
            if (untilCondition != null && condition != null) {
                return new RichAndCondition<>(new RichNotCondition<>(untilCondition), condition);
            } else if (untilCondition != null && isTakeCondition) {
                return new RichNotCondition<>(untilCondition);
            }

            return condition;
        }

        /**
         * 获取当前模式的 IGNORE 迁移条件，并与 UNTIL 条件结合（如果适用）。
         *
         * - 该方法用于 LOOPING 和 TIMES 复杂状态中的内部状态，以处理 IGNORE 逻辑。
         *
         * @param pattern 需要获取 IGNORE 迁移条件的模式
         * @return IGNORE 迁移的匹配条件
         */
        @SuppressWarnings("unchecked")
        private IterativeCondition<T> getInnerIgnoreCondition(Pattern<T, ?> pattern) {
            // 获取当前模式的 IGNORE 消费策略
            Quantifier.ConsumingStrategy consumingStrategy = pattern.getQuantifier().getInnerConsumingStrategy();

            // 如果该模式是 GroupPattern 的头部，则使用 GroupPattern 的策略
            if (headOfGroup(pattern)) {
                consumingStrategy = currentGroupPattern.getQuantifier().getInnerConsumingStrategy();
            }

            IterativeCondition<T> innerIgnoreCondition = null;
            switch (consumingStrategy) {
                case STRICT:
                    innerIgnoreCondition = null;
                    break;
                case SKIP_TILL_NEXT:
                    innerIgnoreCondition = new RichNotCondition<>((IterativeCondition<T>) pattern.getCondition());
                    break;
                case SKIP_TILL_ANY:
                    innerIgnoreCondition = BooleanConditions.trueFunction();
                    break;
            }

            // 结合 UNTIL 条件（如果适用）
            if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
                innerIgnoreCondition =
                        extendWithUntilCondition(
                                innerIgnoreCondition,
                                (IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
                                false);
            }
            return innerIgnoreCondition;
        }


        /**
         * 获取指定模式的 IGNORE 迁移条件，并根据需要扩展 STOP（UNTIL）条件。
         *
         * - IGNORE 迁移用于表示“忽略某些事件继续匹配”的策略。
         * - 该方法根据模式的消费策略（Consuming Strategy）决定 IGNORE 的匹配方式。
         * - 如果模式属于 GroupPattern，并且处于 TIMES/LOOPING 量词的头部，则使用 GroupPattern 的消费策略。
         * - 如果存在 UNTIL 终止条件，则合并到 IGNORE 迁移中。
         *
         * @param pattern 需要获取 IGNORE 迁移条件的模式
         * @return 计算得到的 IGNORE 迁移匹配条件
         */
        @SuppressWarnings("unchecked")
        private IterativeCondition<T> getIgnoreCondition(Pattern<T, ?> pattern) {
            // 获取当前模式的消费策略
            Quantifier.ConsumingStrategy consumingStrategy = pattern.getQuantifier().getConsumingStrategy();

            // 如果该模式是 GroupPattern 的头部，则使用 GroupPattern 的策略
            if (headOfGroup(pattern)) {
                if (isCurrentGroupPatternFirstOfLoop()) {
                    consumingStrategy = currentGroupPattern.getQuantifier().getConsumingStrategy();
                } else {
                    consumingStrategy = currentGroupPattern.getQuantifier().getInnerConsumingStrategy();
                }
            }

            IterativeCondition<T> ignoreCondition = null;
            switch (consumingStrategy) {
                case STRICT:
                    // 严格匹配，不允许跳过任何事件，因此 IGNORE 迁移条件为空
                    ignoreCondition = null;
                    break;
                case SKIP_TILL_NEXT:
                    // 跳过当前匹配事件，直到找到下一个匹配事件
                    ignoreCondition = new RichNotCondition<>((IterativeCondition<T>) pattern.getCondition());
                    break;
                case SKIP_TILL_ANY:
                    // 允许跳过任意事件
                    ignoreCondition = BooleanConditions.trueFunction();
                    break;
            }

            // 如果当前模式属于 GroupPattern，并且存在 UNTIL 终止条件，则扩展 IGNORE 迁移
            if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
                ignoreCondition = extendWithUntilCondition(
                        ignoreCondition,
                        (IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
                        false);
            }
            return ignoreCondition;
        }

        /**
         * 获取指定模式的 TAKE 迁移条件，并根据需要扩展 STOP（UNTIL）条件。
         *
         * - TAKE 迁移用于表示“接受匹配事件”的策略。
         * - 如果模式属于 GroupPattern，并且存在 UNTIL 终止条件，则合并到 TAKE 迁移中。
         *
         * @param pattern 需要获取 TAKE 迁移条件的模式
         * @return 计算得到的 TAKE 迁移匹配条件
         */
        @SuppressWarnings("unchecked")
        private IterativeCondition<T> getTakeCondition(Pattern<T, ?> pattern) {
            // 获取当前模式的匹配条件
            IterativeCondition<T> takeCondition = (IterativeCondition<T>) pattern.getCondition();

            // 如果当前模式属于 GroupPattern，并且存在 UNTIL 终止条件，则扩展 TAKE 迁移
            if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
                takeCondition = extendWithUntilCondition(
                        takeCondition,
                        (IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
                        true);
            }
            return takeCondition;
        }

        /**
         * 获取一个始终返回 `true` 的匹配条件，并根据需要扩展 STOP（UNTIL）条件。
         *
         * - 主要用于 PROCEED 迁移，表示该状态可以继续前进。
         * - 如果模式属于 GroupPattern，并且存在 UNTIL 终止条件，则合并到该匹配条件中。
         *
         * @return 计算得到的匹配条件（始终为 true，可能带有 UNTIL 限制）
         */
        @SuppressWarnings("unchecked")
        private IterativeCondition<T> getTrueFunction() {
            // 默认返回一个始终匹配的条件（true）
            IterativeCondition<T> trueCondition = BooleanConditions.trueFunction();

            // 如果当前模式属于 GroupPattern，并且存在 UNTIL 终止条件，则扩展
            if (currentGroupPattern != null && currentGroupPattern.getUntilCondition() != null) {
                trueCondition = extendWithUntilCondition(
                        trueCondition,
                        (IterativeCondition<T>) currentGroupPattern.getUntilCondition(),
                        true);
            }
            return trueCondition;
        }

        /**
         * 更新状态的 GREEDY（贪心）匹配条件，以确保不会匹配超出预期的事件。
         *
         * - 该方法遍历状态的所有状态迁移，并修改其匹配条件，确保不会额外匹配超出 TAKE 事件的范围。
         * - 主要用于处理 GREEDY 模式，防止不必要的匹配过度扩展。
         *
         * @param state 需要更新 GREEDY 规则的状态
         * @param takeCondition 当前模式的 TAKE 迁移匹配条件
         */
        private void updateWithGreedyCondition(
                State<T> state, IterativeCondition<T> takeCondition) {
            for (StateTransition<T> stateTransition : state.getStateTransitions()) {
                // 修改状态迁移条件，确保不会匹配超出 TAKE 事件范围
                stateTransition.setCondition(
                        new RichAndCondition<>(
                                stateTransition.getCondition(),
                                new RichNotCondition<>(takeCondition)));
            }
        }
    }

    /**
     * NFA（非确定性有限状态机）的工厂接口，用于创建 NFA 实例。
     *
     * @param <T> 处理的输入事件类型
     */
    public interface NFAFactory<T> extends Serializable {
        NFA<T> createNFA();
    }

    /**
     * {@link NFAFactory} 接口的实现类。
     *
     * <p>该实现包含 NFA 的基本配置信息，如窗口时间、状态集合及其状态迁移关系。
     *
     * @param <T> 处理的输入事件类型
     */
    private static class NFAFactoryImpl<T> implements NFAFactory<T> {

        private static final long serialVersionUID = 8939783698296714379L;

        private final long windowTime; // 窗口时间
        private final Map<String, Long> windowTimes; // 记录状态窗口时间的映射
        private final Collection<State<T>> states; // NFA 所有状态的集合
        private final boolean timeoutHandling; // 是否支持超时处理

        /**
         * NFA 工厂的构造函数。
         *
         * @param windowTime 整个 NFA 运行的时间窗口（单位：毫秒）
         * @param windowTimes 记录状态窗口时间的映射
         * @param states NFA 所有状态的集合
         * @param timeoutHandling 是否启用超时处理
         */
        private NFAFactoryImpl(
                long windowTime,
                Map<String, Long> windowTimes,
                Collection<State<T>> states,
                boolean timeoutHandling) {

            this.windowTime = windowTime;
            this.windowTimes = windowTimes;
            this.states = states;
            this.timeoutHandling = timeoutHandling;
        }

        /**
         * 创建一个新的 NFA 实例。
         *
         * @return 生成的 NFA 实例
         */
        @Override
        public NFA<T> createNFA() {
            return new NFA<>(states, windowTimes, windowTime, timeoutHandling);
        }
    }
}
