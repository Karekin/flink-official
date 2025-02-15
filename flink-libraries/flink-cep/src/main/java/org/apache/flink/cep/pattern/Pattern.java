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

package org.apache.flink.cep.pattern;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.Times;
import org.apache.flink.cep.pattern.conditions.BooleanConditions;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichAndCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 定义模式的基类。
 *
 * <p>模式定义由 {@link org.apache.flink.cep.nfa.compiler.NFACompiler} 用于创建一个 {@link NFA}。
 *
 * <pre>{@code
 * Pattern<T, F> pattern = Pattern.<T>begin("start")
 *   .next("middle").subtype(F.class)
 *   .followedBy("end").where(new MyCondition());
 * }</pre>
 *
 * @param <T> 模式中元素的基类类型
 * @param <F> 当前模式操作符所约束的 T 的子类型
 */
public class Pattern<T, F extends T> {

    /** 模式的名称。 */
    private final String name;

    /** 上一个模式。 */
    private final Pattern<T, ? extends T> previous;

    /** 事件必须满足的条件，才能被认为是匹配的。 */
    private IterativeCondition<F> condition;

    /** 模式匹配必须发生的时间窗口长度。 */
    private final Map<WithinType, Duration> windowTimes = new HashMap<>();

    /**
     * 模式的量词。默认设置为 {@link Quantifier#one(ConsumingStrategy)}。
     */
    private Quantifier quantifier = Quantifier.one(ConsumingStrategy.STRICT);

    /** 事件必须满足的条件，以停止收集事件进入循环状态。 */
    private IterativeCondition<F> untilCondition;

    /** 适用于 {@code times} 模式，表示模式需要出现的次数。 */
    private Times times;

    /** 模式匹配后的跳过策略。 */
    private final AfterMatchSkipStrategy afterMatchSkipStrategy;

    /**
     * 构造函数，初始化模式名称、上一个模式、消费策略和跳过策略。
     *
     * @param name 模式名称
     * @param previous 上一个模式
     * @param consumingStrategy 消费策略
     * @param afterMatchSkipStrategy 匹配后的跳过策略
     */
    protected Pattern(
            final String name,
            final Pattern<T, ? extends T> previous,
            final ConsumingStrategy consumingStrategy,
            final AfterMatchSkipStrategy afterMatchSkipStrategy) {
        this.name = name;
        this.previous = previous;
        this.quantifier = Quantifier.one(consumingStrategy);
        this.afterMatchSkipStrategy = afterMatchSkipStrategy;
    }

    /**
     * 获取当前模式的前一个模式。
     *
     * @return 上一个模式
     */
    public Pattern<T, ? extends T> getPrevious() {
        return previous;
    }

    /**
     * 获取当前模式的出现次数。
     *
     * @return 出现次数
     */
    public Times getTimes() {
        return times;
    }

    /**
     * 获取模式的名称。
     *
     * @return 模式名称
     */
    public String getName() {
        return name;
    }

    /** @deprecated 使用 {@link #getWindowSize()} */
    @Deprecated
    @Nullable
    public Time getWindowTime() {
        return getWindowSize().map(Time::of).orElse(null);
    }

    /**
     * 获取当前模式的时间窗口大小。
     *
     * @return 时间窗口大小
     */
    public Optional<Duration> getWindowSize() {
        return getWindowSize(WithinType.FIRST_AND_LAST);
    }

    /** @deprecated 使用 {@link #getWindowSize(WithinType)} */
    @Deprecated
    @Nullable
    public Time getWindowTime(WithinType withinType) {
        return getWindowSize(withinType).map(Time::of).orElse(null);
    }

    /**
     * 获取当前模式在指定类型下的时间窗口大小。
     *
     * @param withinType 窗口类型
     * @return 时间窗口大小
     */
    public Optional<Duration> getWindowSize(WithinType withinType) {
        return Optional.ofNullable(windowTimes.get(withinType));
    }

    /**
     * 获取当前模式的量词。
     *
     * @return 模式的量词
     */
    public Quantifier getQuantifier() {
        return quantifier;
    }

    /**
     * 获取事件需要满足的条件，才能被认为是匹配的。
     *
     * @return 匹配条件
     */
    public IterativeCondition<F> getCondition() {
        if (condition != null) {
            return condition;
        } else {
            return BooleanConditions.trueFunction();
        }
    }

    /**
     * 获取事件停止收集的条件。
     *
     * @return 停止条件
     */
    public IterativeCondition<F> getUntilCondition() {
        return untilCondition;
    }

    /**
     * 开始一个新的模式序列。提供的名称是新序列的初始模式名称。同时，设置事件序列的基类型。
     *
     * @param name 新模式序列的起始模式名称
     * @param <X> 事件模式的基类型
     * @return 新模式序列的第一个模式
     */
    public static <X> Pattern<X, X> begin(final String name) {
        return new Pattern<>(name, null, ConsumingStrategy.STRICT, AfterMatchSkipStrategy.noSkip());
    }

    /**
     * 开始一个新的模式序列。提供的名称是新序列的初始模式名称，同时设置事件序列的基类型。
     * @param name 新模式序列的起始模式名称
     * @param afterMatchSkipStrategy 匹配后的跳过策略
     * @param <X> 事件模式的基类型
     * @return 新模式序列的第一个模式
     */
    public static <X> Pattern<X, X> begin(
            final String name, final AfterMatchSkipStrategy afterMatchSkipStrategy) {
        return new Pattern<X, X>(name, null, ConsumingStrategy.STRICT, afterMatchSkipStrategy);
    }

    /**
     * 为事件添加一个条件，事件必须满足该条件才会被认为是匹配的。如果已经有其他条件，新的条件会与之前的条件通过逻辑 {@code AND} 连接。
     * 否则，这将成为唯一的条件。
     *
     * @param condition 作为 {@link IterativeCondition} 的条件
     * @return 设置了新条件的模式
     */
    public Pattern<T, F> where(IterativeCondition<F> condition) {
        Preconditions.checkNotNull(condition, "The condition cannot be null.");

        ClosureCleaner.clean(condition, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        if (this.condition == null) {
            this.condition = condition;
        } else {
            this.condition = new RichAndCondition<>(this.condition, condition);
        }
        return this;
    }

    /**
     * 为事件添加一个条件，事件必须满足该条件才会被认为是匹配的。如果已经有其他条件，新的条件会与之前的条件通过逻辑 {@code OR} 连接。
     * 否则，这将成为唯一的条件。
     *
     * @param condition 作为 {@link IterativeCondition} 的条件
     * @return 设置了新条件的模式
     */
    public Pattern<T, F> or(IterativeCondition<F> condition) {
        Preconditions.checkNotNull(condition, "The condition cannot be null.");

        ClosureCleaner.clean(condition, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        if (this.condition == null) {
            this.condition = condition;
        } else {
            this.condition = new RichOrCondition<>(this.condition, condition);
        }
        return this;
    }

    /**
     * 对当前模式应用子类型约束。即，事件必须是给定子类型，才能被匹配。
     *
     * @param subtypeClass 子类型的类
     * @param <S> 子类型
     * @return 具有新子类型约束的相同模式
     */
    public <S extends F> Pattern<T, S> subtype(final Class<S> subtypeClass) {
        Preconditions.checkNotNull(subtypeClass, "The class cannot be null.");

        if (condition == null) {
            this.condition = new SubtypeCondition<F>(subtypeClass);
        } else {
            this.condition =
                    new RichAndCondition<>(condition, new SubtypeCondition<F>(subtypeClass));
        }

        @SuppressWarnings("unchecked")
        Pattern<T, S> result = (Pattern<T, S>) this;

        return result;
    }

    /**
     * 应用一个停止条件，用于控制循环状态的停止。它允许清理底层的状态。
     *
     * @param untilCondition 事件必须满足的条件，才能停止将事件收集到循环状态
     * @return 具有应用 untilCondition 的相同模式
     */
    public Pattern<T, F> until(IterativeCondition<F> untilCondition) {
        Preconditions.checkNotNull(untilCondition, "The condition cannot be null");

        // 如果已经设置了 untilCondition，则抛出异常，因为只能应用一个 untilCondition
        if (this.untilCondition != null) {
            throw new MalformedPatternException("Only one until condition can be applied.");
        }

        // 检查当前模式的量词是否支持循环状态
        if (!quantifier.hasProperty(Quantifier.QuantifierProperty.LOOPING)) {
            throw new MalformedPatternException(
                    "The until condition is only applicable to looping states.");
        }

        // 清理条件，确保它可以正确使用
        ClosureCleaner.clean(untilCondition, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.untilCondition = untilCondition;

        return this;
    }

    /**
     * 定义模式匹配必须完成的最大时间间隔，以便被认为是有效的。这个时间间隔表示第一个事件和最后一个事件之间的最大时间差。
     *
     * @param windowTime 匹配窗口的时间
     * @return 具有新窗口长度的相同模式操作符
     * @deprecated 使用 {@link #within(Duration)}。
     */
    @Deprecated
    public Pattern<T, F> within(@Nullable Time windowTime) {
        return within(Time.toDuration(windowTime));
    }

    /**
     * 定义模式匹配必须完成的最大时间间隔，以便被认为是有效的。这个时间间隔表示第一个事件和最后一个事件之间的最大时间差。
     *
     * @param windowTime 匹配窗口的时间
     * @return 具有新窗口长度的相同模式操作符
     */
    public Pattern<T, F> within(@Nullable Duration windowTime) {
        return within(windowTime, WithinType.FIRST_AND_LAST);
    }

    /**
     * 定义模式匹配必须完成的最大时间间隔，以便被认为是有效的。这个时间间隔表示事件之间的最大时间差。
     *
     * @param withinType 事件之间时间间隔的类型
     * @param windowTime 匹配窗口的时间
     * @return 具有新窗口长度的相同模式操作符
     * @deprecated 使用 {@link #within(Duration, WithinType)}。
     */
    @Deprecated
    public Pattern<T, F> within(@Nullable Time windowTime, WithinType withinType) {
        return within(Time.toDuration(windowTime), withinType);
    }

    /**
     * 定义模式匹配必须完成的最大时间间隔，以便被认为是有效的。这个时间间隔表示事件之间的最大时间差。
     *
     * @param withinType 事件之间时间间隔的类型
     * @param windowTime 匹配窗口的时间
     * @return 具有新窗口长度的相同模式操作符
     */
    public Pattern<T, F> within(@Nullable Duration windowTime, WithinType withinType) {
        // 如果提供了时间窗口，存储该时间窗口
        if (windowTime != null) {
            windowTimes.put(withinType, windowTime);
        }

        return this;
    }

    /**
     * 向现有模式追加一个新模式。新模式强制严格的时间顺序连续性。这意味着只有当与当前模式匹配的事件直接跟随在前一个匹配事件后面时，整个模式序列才会匹配。
     * 换句话说，在两个匹配事件之间不能有任何事件。
     *
     * @param name 新模式的名称
     * @return 追加到当前模式后的新模式
     */
    public Pattern<T, T> next(final String name) {
        return new Pattern<>(name, this, ConsumingStrategy.STRICT, afterMatchSkipStrategy);
    }

    /**
     * 向现有模式追加一个新模式。新模式强制要求在前一个匹配事件后不能有与当前模式匹配的事件。
     *
     * @param name 新模式的名称
     * @return 追加到当前模式后的新模式
     */
    public Pattern<T, T> notNext(final String name) {
        // 如果量词包含可选的属性，抛出不支持的操作异常
        if (quantifier.hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
            throw new UnsupportedOperationException(
                    "Specifying a pattern with an optional path to NOT condition is not supported yet. "
                            + "You can simulate such pattern with two independent patterns, one with and the other without "
                            + "the optional part.");
        }
        return new Pattern<>(name, this, ConsumingStrategy.NOT_NEXT, afterMatchSkipStrategy);
    }

    /**
     * 向现有模式追加一个新模式。新模式强制非严格的时间顺序连续性。这意味着当前模式和前一个匹配事件可能会与其他事件交织在一起，而这些交织的事件会被忽略。
     *
     * @param name 新模式的名称
     * @return 追加到当前模式后的新模式
     */
    public Pattern<T, T> followedBy(final String name) {
        return new Pattern<>(name, this, ConsumingStrategy.SKIP_TILL_NEXT, afterMatchSkipStrategy);
    }

    /**
     * 向现有模式追加一个新模式。新模式强制要求在前一个模式和当前模式之间不能有任何匹配事件。
     *
     * <p><b>注意：</b>在此模式后必须有另一个模式。
     *
     * @param name 新模式的名称
     * @return 追加到当前模式后的新模式
     */
    public Pattern<T, T> notFollowedBy(final String name) {
        // 如果量词包含可选的属性，抛出不支持的操作异常
        if (quantifier.hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
            throw new UnsupportedOperationException(
                    "Specifying a pattern with an optional path to NOT condition is not supported yet. "
                            + "You can simulate such pattern with two independent patterns, one with and the other without "
                            + "the optional part.");
        }
        return new Pattern<>(name, this, ConsumingStrategy.NOT_FOLLOW, afterMatchSkipStrategy);
    }

    /**
     * 向现有模式追加一个新模式。新模式强制非严格的时间顺序连续性。这意味着当前模式和前一个匹配事件可能会与其他事件交织在一起，而这些交织的事件会被忽略。
     *
     * @param name 新模式的名称
     * @return 追加到当前模式后的新模式
     */
    public Pattern<T, T> followedByAny(final String name) {
        return new Pattern<>(name, this, ConsumingStrategy.SKIP_TILL_ANY, afterMatchSkipStrategy);
    }

    /**
     * 指定该模式对于模式序列的最终匹配来说是可选的。
     *
     * @return 标记为可选的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> optional() {
        checkIfPreviousPatternGreedy();
        quantifier.optional();
        return this;
    }

    /**
     * 指定该模式可以出现 {@code 一次或多次}。这意味着至少有一个事件和最多无限个事件可以匹配此模式。
     *
     * <p>启用此量词后，假设模式为 {@code A.oneOrMore().followedBy(B)}，事件序列为 {@code A1 A2 B}，
     * 那么将生成两个模式：{@code A1 B} 和 {@code A1 A2 B}。详见 {@link #allowCombinations()}。
     *
     * @return 应用 {@link Quantifier#looping(ConsumingStrategy)} 量词的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> oneOrMore() {
        return oneOrMore((Duration) null);
    }

    /**
     * 指定该模式可以出现 {@code 一次或多次}，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     * 这意味着至少有一个事件和最多无限个事件可以匹配此模式。
     *
     * <p>启用此量词后，假设模式为 {@code A.oneOrMore().followedBy(B)}，事件序列为 {@code A1 A2 B}，
     * 那么将生成两个模式：{@code A1 B} 和 {@code A1 A2 B}。详见 {@link #allowCombinations()}。
     *
     * @param windowTime 事件之间匹配窗口的时间
     * @return 应用 {@link Quantifier#looping(ConsumingStrategy)} 量词的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     * @deprecated 使用 {@link #oneOrMore(Duration)}
     */
    @Deprecated
    public Pattern<T, F> oneOrMore(@Nullable Time windowTime) {
        return oneOrMore(Time.toDuration(windowTime));
    }


    /**
     * 指定该模式可以出现 {@code 一次或多次}，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     * 这意味着至少一个事件和最多无限个事件可以匹配此模式。
     *
     * <p>如果为模式 {@code A.oneOrMore().followedBy(B)} 启用了此量词，并且出现了事件序列 {@code A1 A2 B}，
     * 则会生成以下模式：{@code A1 B} 和 {@code A1 A2 B}。详见 {@link #allowCombinations()}。
     *
     * @param windowTime 每次时间匹配窗口的时间
     * @return 应用 {@link Quantifier#looping(ConsumingStrategy)} 量词的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> oneOrMore(@Nullable Duration windowTime) {
        checkIfNoNotPattern(); // 检查是否没有 NOT 模式
        checkIfQuantifierApplied(); // 检查是否已应用量词
        this.quantifier = Quantifier.looping(quantifier.getConsumingStrategy()); // 设置量词为 looping
        this.times = Times.of(1, windowTime); // 设置出现次数为 1，且包含时间窗口
        return this;
    }

    /**
     * 指定该模式是贪婪的。意味着尽可能多的事件会匹配此模式。
     *
     * @return 设置为贪婪的相同模式，并将 {@link Quantifier#greedy} 设置为 true
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> greedy() {
        checkIfNoNotPattern(); // 检查是否没有 NOT 模式
        checkIfNoGroupPattern(); // 检查是否没有分组模式
        this.quantifier.greedy(); // 设置量词为贪婪
        return this;
    }

    /**
     * 指定该模式应匹配的确切次数。
     *
     * @param times 匹配事件必须出现的次数
     * @return 应用匹配次数的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> times(int times) {
        return times(times, (Duration) null); // 默认情况下没有时间窗口
    }

    /**
     * 指定该模式应匹配的确切次数，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     *
     * @param times 匹配事件必须出现的次数
     * @param windowTime 每次事件之间的匹配窗口时间
     * @return 应用匹配次数和时间窗口的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     * @deprecated 使用 {@link #times(int, Duration)}。
     */
    @Deprecated
    public Pattern<T, F> times(int times, @Nullable Time windowTime) {
        return times(times, Time.toDuration(windowTime)); // 转换 Time 对象为 Duration
    }

    /**
     * 指定该模式应匹配的确切次数，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     *
     * @param times 匹配事件必须出现的次数
     * @param windowTime 每次事件之间的匹配窗口时间
     * @return 应用匹配次数和时间窗口的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> times(int times, @Nullable Duration windowTime) {
        checkIfNoNotPattern(); // 检查是否没有 NOT 模式
        checkIfQuantifierApplied(); // 检查是否已应用量词
        Preconditions.checkArgument(times > 0, "You should give a positive number greater than 0."); // 检查次数必须大于 0
        this.quantifier = Quantifier.times(quantifier.getConsumingStrategy()); // 设置量词为 times
        this.times = Times.of(times, windowTime); // 设置出现次数和时间窗口
        return this;
    }

    /**
     * 指定该模式可以出现从指定次数到指定次数之间的次数。
     *
     * @param from 至少匹配事件必须出现的次数
     * @param to 最多匹配事件必须出现的次数
     * @return 应用次数范围的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> times(int from, int to) {
        return times(from, to, (Duration) null); // 默认情况下没有时间窗口
    }

    /**
     * 指定该模式可以出现从指定次数到指定次数之间的次数，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     *
     * @param from 至少匹配事件必须出现的次数
     * @param to 最多匹配事件必须出现的次数
     * @param windowTime 每次事件之间的匹配窗口时间
     * @return 应用次数范围和时间窗口的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     * @deprecated 使用 {@link #times(int, int, Duration)}。
     */
    @Deprecated
    public Pattern<T, F> times(int from, int to, @Nullable Time windowTime) {
        return times(from, to, Time.toDuration(windowTime)); // 转换 Time 对象为 Duration
    }

    /**
     * 指定该模式可以出现从指定次数到指定次数之间的次数，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     *
     * @param from 至少匹配事件必须出现的次数
     * @param to 最多匹配事件必须出现的次数
     * @param windowTime 每次事件之间的匹配窗口时间
     * @return 应用次数范围和时间窗口的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> times(int from, int to, @Nullable Duration windowTime) {
        checkIfNoNotPattern(); // 检查是否没有 NOT 模式
        checkIfQuantifierApplied(); // 检查是否已应用量词
        this.quantifier = Quantifier.times(quantifier.getConsumingStrategy()); // 设置量词为 times
        if (from == 0) {
            this.quantifier.optional(); // 如果 from 为 0，表示该模式是可选的
            from = 1;
        }
        this.times = Times.of(from, to, windowTime); // 设置次数范围和时间窗口
        return this;
    }

    /**
     * 指定该模式可以至少出现指定的次数。这意味着至少出现指定次数，最多可以无限次匹配此模式。
     *
     * @return 应用 {@link Quantifier#looping(ConsumingStrategy)} 量词的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> timesOrMore(int times) {
        return timesOrMore(times, (Duration) null); // 默认情况下没有时间窗口
    }

    /**
     * 指定该模式可以至少出现指定的次数，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     * 这意味着至少出现指定次数，最多可以无限次匹配此模式。
     *
     * @param times 至少匹配事件必须出现的次数
     * @param windowTime 每次事件之间的匹配窗口时间
     * @return 应用 {@link Quantifier#looping(ConsumingStrategy)} 量词的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     * @deprecated 使用 {@link #timesOrMore(int, Duration)}
     */
    @Deprecated
    public Pattern<T, F> timesOrMore(int times, @Nullable Time windowTime) {
        return timesOrMore(times, Time.toDuration(windowTime)); // 转换 Time 对象为 Duration
    }

    /**
     * 指定该模式可以至少出现指定的次数，并且每次出现的时间间隔对应于前一个事件和当前事件之间的最大时间差。
     * 这意味着至少出现指定次数，最多可以无限次匹配此模式。
     *
     * @param times 至少匹配事件必须出现的次数
     * @param windowTime 每次事件之间的匹配窗口时间
     * @return 应用 {@link Quantifier#looping(ConsumingStrategy)} 量词的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> timesOrMore(int times, @Nullable Duration windowTime) {
        checkIfNoNotPattern(); // 检查是否没有 NOT 模式
        checkIfQuantifierApplied(); // 检查是否已应用量词
        this.quantifier = Quantifier.looping(quantifier.getConsumingStrategy()); // 设置量词为 looping
        this.times = Times.of(times, windowTime); // 设置出现次数和时间窗口
        return this;
    }

    /**
     * 仅适用于 {@link Quantifier#looping(ConsumingStrategy)} 和 {@link Quantifier#times(ConsumingStrategy)} 模式，
     * 此选项允许为匹配事件提供更多灵活性。
     *
     * <p>如果没有为模式 {@code A.oneOrMore().followedBy(B)} 应用 {@code allowCombinations()}，
     * 且事件序列 {@code A1 A2 B} 出现，则会生成模式：{@code A1 B} 和 {@code A1 A2 B}。
     * 如果应用此方法，我们将得到 {@code A1 B}、{@code A2 B} 和 {@code A1 A2 B}。
     *
     * @return 更新量词的相同模式
     * @throws MalformedPatternException 如果量词不适用于此模式
     */
    public Pattern<T, F> allowCombinations() {
        quantifier.combinations(); // 设置量词允许组合
        return this;
    }


    /**
     * 配合 {@link Pattern#oneOrMore()} 或 {@link Pattern#times(int)} 使用。
     * 指定任何不匹配的元素都会中断循环。
     *
     * <p>例如，像这样的模式：
     *
     * <pre>{@code
     * Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
     *      @Override
     *      public boolean filter(Event value) throws Exception {
     *          return value.getName().equals("c");
     *      }
     * })
     * .followedBy("middle").where(new SimpleCondition<Event>() {
     *      @Override
     *      public boolean filter(Event value) throws Exception {
     *          return value.getName().equals("a");
     *      }
     * }).oneOrMore().consecutive()
     * .followedBy("end1").where(new SimpleCondition<Event>() {
     *      @Override
     *      public boolean filter(Event value) throws Exception {
     *          return value.getName().equals("b");
     *      }
     * });
     * }</pre>
     *
     * <p>对于序列：C D A1 A2 A3 D A4 B
     *
     * <p>将生成的匹配结果为：{C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}
     *
     * <p>默认情况下，应用的是宽松的连续性。
     *
     * @return 将模式的连续性改为严格的模式
     */
    public Pattern<T, F> consecutive() {
        quantifier.consecutive(); // 设置量词为严格连续性
        return this;
    }

    /**
     * 开始一个新的模式序列。提供的模式作为新序列的初始模式。
     *
     * @param group 开始的模式
     * @param afterMatchSkipStrategy 匹配后跳过策略
     * @return 模式序列的第一个模式
     */
    public static <T, F extends T> GroupPattern<T, F> begin(
            final Pattern<T, F> group, final AfterMatchSkipStrategy afterMatchSkipStrategy) {
        return new GroupPattern<>(null, group, ConsumingStrategy.STRICT, afterMatchSkipStrategy);
    }

    /**
     * 开始一个新的模式序列。提供的模式作为新序列的初始模式。
     *
     * @param group 开始的模式
     * @return 模式序列的第一个模式
     */
    public static <T, F extends T> GroupPattern<T, F> begin(Pattern<T, F> group) {
        return new GroupPattern<>(
                null, group, ConsumingStrategy.STRICT, AfterMatchSkipStrategy.noSkip());
    }

    /**
     * 将新的组模式追加到现有模式中。新模式强制非严格的时间顺序连续性。
     * 这意味着该模式和前一个匹配事件可能会与其他事件交织，而这些事件将被忽略。
     *
     * @param group 要追加的模式
     * @return 追加到当前模式后的新模式
     */
    public GroupPattern<T, F> followedBy(Pattern<T, F> group) {
        return new GroupPattern<>(
                this, group, ConsumingStrategy.SKIP_TILL_NEXT, afterMatchSkipStrategy);
    }

    /**
     * 将新的组模式追加到现有模式中。新模式强制非严格的时间顺序连续性。
     * 这意味着该模式和前一个匹配事件可能会与其他事件交织，而这些事件将被忽略。
     *
     * @param group 要追加的模式
     * @return 追加到当前模式后的新模式
     */
    public GroupPattern<T, F> followedByAny(Pattern<T, F> group) {
        return new GroupPattern<>(
                this, group, ConsumingStrategy.SKIP_TILL_ANY, afterMatchSkipStrategy);
    }

    /**
     * 将新的组模式追加到现有模式中。新模式强制严格的时间顺序连续性。
     * 这意味着只有当匹配事件紧接在前一个匹配事件后面时，整个模式序列才会匹配。
     * 换句话说，在两个匹配事件之间不能有任何其他事件。
     *
     * @param group 要追加的模式
     * @return 追加到当前模式后的新模式
     */
    public GroupPattern<T, F> next(Pattern<T, F> group) {
        return new GroupPattern<>(this, group, ConsumingStrategy.STRICT, afterMatchSkipStrategy);
    }

    /**
     * 检查是否没有应用 NOT 模式。如果已经应用了，不允许继续设置。
     */
    private void checkIfNoNotPattern() {
        if (quantifier.getConsumingStrategy() == ConsumingStrategy.NOT_FOLLOW
                || quantifier.getConsumingStrategy() == ConsumingStrategy.NOT_NEXT) {
            throw new MalformedPatternException("Option not applicable to NOT pattern");
        }
    }

    /**
     * 检查是否已经应用了量词。如果已应用，抛出异常。
     */
    private void checkIfQuantifierApplied() {
        if (!quantifier.hasProperty(Quantifier.QuantifierProperty.SINGLE)) {
            throw new MalformedPatternException(
                    "Already applied quantifier to this Pattern. "
                            + "Current quantifier is: "
                            + quantifier);
        }
    }

    /**
     * @return 当前模式的 {@link AfterMatchSkipStrategy.SkipStrategy} 配置
     */
    public AfterMatchSkipStrategy getAfterMatchSkipStrategy() {
        return afterMatchSkipStrategy;
    }

    /**
     * 检查是否没有应用组模式。如果应用了组模式，则不允许继续设置。
     */
    private void checkIfNoGroupPattern() {
        if (this instanceof GroupPattern) {
            throw new MalformedPatternException("Option not applicable to group pattern");
        }
    }

    /**
     * 检查前一个模式是否为贪婪模式。如果是，则抛出异常，因为不可在贪婪模式之后应用可选模式。
     */
    private void checkIfPreviousPatternGreedy() {
        if (previous != null
                && previous.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)) {
            throw new MalformedPatternException(
                    "Optional pattern cannot be preceded by greedy pattern");
        }
    }

    /**
     * 重写 toString 方法，返回当前模式的详细信息。
     *
     * @return 当前模式的字符串表示
     */
    @Override
    public String toString() {
        return "Pattern{"
                + "name='"
                + name
                + '\''
                + ", previous="
                + previous
                + ", condition="
                + condition
                + ", windowTimes="
                + windowTimes
                + ", quantifier="
                + quantifier
                + ", untilCondition="
                + untilCondition
                + ", times="
                + times
                + ", afterMatchSkipStrategy="
                + afterMatchSkipStrategy
                + '}';
    }

}
