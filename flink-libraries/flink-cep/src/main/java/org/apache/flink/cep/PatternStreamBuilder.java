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

package org.apache.flink.cep;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.operator.CepOperator;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link PatternStreamBuilder} 是一个用于构建 `PatternStream` 的工具类。
 *
 * <p>它负责：
 * - 绑定输入流 (`DataStream<IN>`) 和匹配模式 (`Pattern<IN, ?>`)。
 * - 设定时间行为 (`ProcessingTime` 或 `EventTime`)。
 * - 设定事件比较器 (`EventComparator`)，用于事件排序。
 * - 设定迟到数据的侧输出流 (`lateDataOutputTag`)。
 * - 通过 `build()` 方法创建 `SingleOutputStreamOperator<OUT>`，用于处理匹配模式。
 *
 * @param <IN> 输入流中的数据类型
 */
@Internal
final class PatternStreamBuilder<IN> {

    /** Flink 输入数据流 */
    private final DataStream<IN> inputStream;

    /** 要匹配的模式 */
    private final Pattern<IN, ?> pattern;

    /** 事件比较器（用于排序），可选 */
    private final EventComparator<IN> comparator;

    /**
     * 侧输出流 `OutputTag`，用于存储迟到数据。
     * 如果未设置，则迟到数据将被丢弃。
     */
    private final OutputTag<IN> lateDataOutputTag;

    /**
     * 时间行为（Processing Time 或 Event Time）。
     * 默认为 `EventTime`。
     */
    private final TimeBehaviour timeBehaviour;

    /**
     * 时间行为枚举类，定义了 Flink CEP 处理模式的时间类型：
     * - `ProcessingTime`：基于处理时间
     * - `EventTime`：基于事件时间
     */
    enum TimeBehaviour {
        ProcessingTime,
        EventTime
    }

    /**
     * 构造 `PatternStreamBuilder`，绑定数据流、模式、时间行为等参数。
     *
     * @param inputStream  输入数据流
     * @param pattern      需要匹配的 `Pattern`
     * @param timeBehaviour  时间行为 (`ProcessingTime` 或 `EventTime`)
     * @param comparator   事件比较器 (可选)
     * @param lateDataOutputTag  侧输出流 `OutputTag` (可选)
     */
    private PatternStreamBuilder(
            final DataStream<IN> inputStream,
            final Pattern<IN, ?> pattern,
            final TimeBehaviour timeBehaviour,
            @Nullable final EventComparator<IN> comparator,
            @Nullable final OutputTag<IN> lateDataOutputTag) {
        this.inputStream = checkNotNull(inputStream);
        this.pattern = checkNotNull(pattern);
        this.timeBehaviour = checkNotNull(timeBehaviour);
        this.comparator = comparator;
        this.lateDataOutputTag = lateDataOutputTag;
    }

    /**
     * 获取输入流的 `TypeInformation`。
     *
     * @return 输入流的类型信息
     */
    TypeInformation<IN> getInputType() {
        return inputStream.getType();
    }

    /**
     * 在 `ExecutionConfig` 启用了闭包清理时，对给定的函数进行闭包清理。
     *
     * @param f 需要清理的函数
     * @param <F> 函数类型
     * @return 清理后的函数
     */
    <F> F clean(F f) {
        return inputStream.getExecutionEnvironment().clean(f);
    }

    /**
     * 设置事件比较器 (EventComparator)，返回新的 `PatternStreamBuilder`。
     *
     * @param comparator 事件比较器
     * @return 配置了 `comparator` 的 `PatternStreamBuilder`
     */
    PatternStreamBuilder<IN> withComparator(final EventComparator<IN> comparator) {
        return new PatternStreamBuilder<>(
                inputStream, pattern, timeBehaviour, checkNotNull(comparator), lateDataOutputTag);
    }

    /**
     * 设置侧输出流 `OutputTag`，用于存储迟到数据，返回新的 `PatternStreamBuilder`。
     *
     * @param lateDataOutputTag 侧输出流 `OutputTag`
     * @return 配置了 `lateDataOutputTag` 的 `PatternStreamBuilder`
     */
    PatternStreamBuilder<IN> withLateDataOutputTag(final OutputTag<IN> lateDataOutputTag) {
        return new PatternStreamBuilder<>(
                inputStream, pattern, timeBehaviour, comparator, checkNotNull(lateDataOutputTag));
    }

    /**
     * 将时间行为设置为 `ProcessingTime`，返回新的 `PatternStreamBuilder`。
     *
     * @return 配置了 `ProcessingTime` 的 `PatternStreamBuilder`
     */
    PatternStreamBuilder<IN> inProcessingTime() {
        return new PatternStreamBuilder<>(
                inputStream, pattern, TimeBehaviour.ProcessingTime, comparator, lateDataOutputTag);
    }

    /**
     * 将时间行为设置为 `EventTime`，返回新的 `PatternStreamBuilder`。
     *
     * @return 配置了 `EventTime` 的 `PatternStreamBuilder`
     */
    PatternStreamBuilder<IN> inEventTime() {
        return new PatternStreamBuilder<>(
                inputStream, pattern, TimeBehaviour.EventTime, comparator, lateDataOutputTag);
    }

    /**
     * 构建 `SingleOutputStreamOperator`，用于处理匹配的事件序列。
     *
     * @param outTypeInfo    输出数据类型信息
     * @param processFunction  处理匹配模式的 `PatternProcessFunction`
     * @param <OUT>  输出数据类型
     * @param <K>    Keyed Stream 的 Key 类型
     * @return `SingleOutputStreamOperator<OUT>`，用于处理匹配的事件序列
     */
    <OUT, K> SingleOutputStreamOperator<OUT> build(
            final TypeInformation<OUT> outTypeInfo,
            final PatternProcessFunction<IN, OUT> processFunction) {

        checkNotNull(outTypeInfo);
        checkNotNull(processFunction);

        // 获取输入数据的序列化器
        final TypeSerializer<IN> inputSerializer =
                inputStream
                        .getType()
                        .createSerializer(inputStream.getExecutionConfig().getSerializerConfig());

        // 判断是否使用 Processing Time
        final boolean isProcessingTime = timeBehaviour == TimeBehaviour.ProcessingTime;

        // 是否需要超时处理
        final boolean timeoutHandling = processFunction instanceof TimedOutPartialMatchHandler;

        // 通过 NFA 编译器构建 NFA
        final NFACompiler.NFAFactory<IN> nfaFactory =
                NFACompiler.compileFactory(pattern, timeoutHandling);

        // 构建 `CepOperator`
        final CepOperator<IN, K, OUT> operator =
                new CepOperator<>(
                        inputSerializer,
                        isProcessingTime,
                        nfaFactory,
                        comparator,
                        pattern.getAfterMatchSkipStrategy(),
                        processFunction,
                        lateDataOutputTag);

        final SingleOutputStreamOperator<OUT> patternStream;

        // 判断是否是 KeyedStream
        if (inputStream instanceof KeyedStream) {
            KeyedStream<IN, K> keyedStream = (KeyedStream<IN, K>) inputStream;
            patternStream = keyedStream.transform("CepOperator", outTypeInfo, operator);
        } else {
            // 使用 `NullByteKeySelector` 进行 keyBy 操作
            KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();
            patternStream =
                    inputStream
                            .keyBy(keySelector)
                            .transform("GlobalCepOperator", outTypeInfo, operator)
                            .forceNonParallel();
        }

        return patternStream;
    }

    // ---------------------------------------- 工厂方法 ---------------------------------------- //

    /**
     * 创建 `PatternStreamBuilder` 实例，绑定输入流和模式，默认使用 `EventTime`。
     *
     * @param inputStream  输入数据流
     * @param pattern  匹配模式
     * @param <IN>  数据类型
     * @return `PatternStreamBuilder<IN>` 实例
     */
    static <IN> PatternStreamBuilder<IN> forStreamAndPattern(
            final DataStream<IN> inputStream, final Pattern<IN, ?> pattern) {
        return new PatternStreamBuilder<>(
                inputStream, pattern, TimeBehaviour.EventTime, null, null);
    }
}

