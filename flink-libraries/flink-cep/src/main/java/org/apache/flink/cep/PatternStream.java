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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.OutputTag;

import java.util.UUID;

import static org.apache.flink.cep.PatternProcessFunctionBuilder.fromFlatSelect;
import static org.apache.flink.cep.PatternProcessFunctionBuilder.fromSelect;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * CEP（复杂事件处理）模式检测的流抽象。
 *
 * <p>PatternStream 是一个流，它会在检测到符合指定模式的事件序列时，输出一个包含事件名称和对应事件的映射（Map）。
 * 该模式检测使用 {@link org.apache.flink.cep.nfa.NFA}（非确定性有限状态机）实现。
 * 用户需要提供 {@link PatternSelectFunction} 或 {@link PatternFlatSelectFunction} 来处理匹配到的事件序列。
 *
 * <p>此外，它还允许处理部分匹配但因超时未完成的事件模式。要处理这些超时模式，用户可以指定
 * {@link PatternTimeoutFunction} 或 {@link PatternFlatTimeoutFunction}。
 *
 * @param <T> 事件的类型
 */
public class PatternStream<T> {

    /** 构建 PatternStream 的内部构造器 */
    private final PatternStreamBuilder<T> builder;

    /** 私有构造方法，确保 PatternStream 只能通过 builder 创建 */
    private PatternStream(final PatternStreamBuilder<T> builder) {
        this.builder = checkNotNull(builder);
    }

    /** 通过输入数据流和模式创建 PatternStream */
    PatternStream(final DataStream<T> inputStream, final Pattern<T, ?> pattern) {
        this(PatternStreamBuilder.forStreamAndPattern(inputStream, pattern));
    }

    /**
     * 指定事件比较器，用于自定义事件排序逻辑。
     *
     * @param comparator 事件比较器
     * @return 新的 PatternStream 实例，应用了指定的事件比较器
     */
    PatternStream<T> withComparator(final EventComparator<T> comparator) {
        return new PatternStream<>(builder.withComparator(comparator));
    }

    /**
     * 将迟到的数据发送到侧输出流（Side Output），以便后续处理。
     * 事件在水位线（Watermark）通过其时间戳后，即被认为是迟到数据。
     *
     * <p>可以通过 {@link SingleOutputStreamOperator#getSideOutput(OutputTag)}
     * 获取该侧输出流的数据。
     *
     * @param lateDataOutputTag 侧输出流的标记
     * @return 新的 PatternStream 实例，支持迟到数据侧输出
     */
    public PatternStream<T> sideOutputLateData(OutputTag<T> lateDataOutputTag) {
        return new PatternStream<>(builder.withLateDataOutputTag(lateDataOutputTag));
    }

    /** 设置时间特征为处理时间（Processing Time）。 */
    public PatternStream<T> inProcessingTime() {
        return new PatternStream<>(builder.inProcessingTime());
    }

    /** 设置时间特征为事件时间（Event Time）。 */
    public PatternStream<T> inEventTime() {
        return new PatternStream<>(builder.inEventTime());
    }

    /**
     * 对检测到的模式序列应用处理函数。
     * 每次检测到匹配的事件序列，都会调用提供的 {@link PatternProcessFunction} 进行处理。
     * 若希望处理超时的部分匹配事件，可以实现 {@link TimedOutPartialMatchHandler} 接口。
     *
     * @param patternProcessFunction 处理匹配模式的函数
     * @param <R> 处理后的结果类型
     * @return 包含处理结果的 {@link DataStream}
     */
    public <R> SingleOutputStreamOperator<R> process(
            final PatternProcessFunction<T, R> patternProcessFunction) {

        // 提取模式处理函数的返回类型
        final TypeInformation<R> returnType =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternProcessFunction,
                        PatternProcessFunction.class,
                        0,
                        1,
                        TypeExtractor.NO_INDEX,
                        builder.getInputType(),
                        null,
                        false);

        return process(patternProcessFunction, returnType);
    }

    /**
     * 对检测到的模式序列应用处理函数，并显式指定返回类型。
     * 每次检测到匹配的事件序列，都会调用提供的 {@link PatternProcessFunction} 进行处理。
     * 若希望处理超时的部分匹配事件，可以实现 {@link TimedOutPartialMatchHandler} 接口。
     *
     * @param patternProcessFunction 处理匹配模式的函数
     * @param <R> 处理后的结果类型
     * @param outTypeInfo 明确指定的输出类型信息
     * @return 包含处理结果的 {@link DataStream}
     */
    public <R> SingleOutputStreamOperator<R> process(
            final PatternProcessFunction<T, R> patternProcessFunction,
            final TypeInformation<R> outTypeInfo) {

        return builder.build(outTypeInfo, builder.clean(patternProcessFunction));
    }

    /**
     * 对检测到的模式序列应用选择函数。
     * 每次检测到匹配的事件序列，都会调用提供的 {@link PatternSelectFunction} 进行处理。
     * 选择函数只能生成一个结果元素。
     *
     * @param patternSelectFunction 选择函数
     * @param <R> 处理后的结果类型
     * @return 包含处理结果的 {@link DataStream}
     */
    public <R> SingleOutputStreamOperator<R> select(
            final PatternSelectFunction<T, R> patternSelectFunction) {

        // 由于 TypeExtractor 不能直接推导出 PatternSelectFunction 的返回类型
        // 这里需要手动提取输出类型
        final TypeInformation<R> returnType =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternSelectFunction,
                        PatternSelectFunction.class,
                        0,
                        1,
                        TypeExtractor.NO_INDEX,
                        builder.getInputType(),
                        null,
                        false);

        return select(patternSelectFunction, returnType);
    }


    /**
     * 应用 `select` 函数处理已匹配的模式序列。
     *
     * <p>对于每个匹配的模式序列，会调用提供的 {@link PatternSelectFunction} 进行处理。
     * 该 `select` 函数会返回一个 `DataStream`，其中包含模式匹配后的结果，每个模式序列对应一个输出元素。
     *
     * @param patternSelectFunction 处理匹配模式的选择函数
     * @param <R> 处理后返回的元素类型
     * @param outTypeInfo 明确指定的输出类型信息
     * @return 包含 `select` 处理结果的 {@link DataStream}
     */
    public <R> SingleOutputStreamOperator<R> select(
            final PatternSelectFunction<T, R> patternSelectFunction,
            final TypeInformation<R> outTypeInfo) {

        // 将 `PatternSelectFunction` 封装为 `PatternProcessFunction`
        final PatternProcessFunction<T, R> processFunction =
                fromSelect(builder.clean(patternSelectFunction)).build();

        // 调用 `process` 方法执行匹配后的处理逻辑
        return process(processFunction, outTypeInfo);
    }

    /**
     * 处理匹配的模式序列，并应用 `select` 逻辑，同时处理超时的部分匹配事件。
     *
     * <p>对于匹配到的模式，会调用提供的 {@link PatternSelectFunction} 进行处理，并返回一个 `DataStream`。
     * <p>对于部分匹配但因超时未完成的模式，会调用提供的 {@link PatternTimeoutFunction} 处理超时数据，并将结果发送到侧输出流（Side Output）。
     *
     * <p>侧输出流可以通过 {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} 获取。
     *
     * @param timedOutPartialMatchesTag 侧输出流的 {@link OutputTag}，用于标识超时模式
     * @param patternTimeoutFunction 处理超时模式的函数
     * @param patternSelectFunction 处理匹配成功模式的函数
     * @param <L> 超时模式的输出类型
     * @param <R> 匹配成功模式的输出类型
     * @return 处理结果的 {@link DataStream}，其中包含匹配结果，超时数据会存入侧输出流
     */
    public <L, R> SingleOutputStreamOperator<R> select(
            final OutputTag<L> timedOutPartialMatchesTag,
            final PatternTimeoutFunction<T, L> patternTimeoutFunction,
            final PatternSelectFunction<T, R> patternSelectFunction) {

        // 解析 `PatternSelectFunction` 的返回类型
        final TypeInformation<R> rightTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternSelectFunction,
                        PatternSelectFunction.class,
                        0,
                        1,
                        TypeExtractor.NO_INDEX,
                        builder.getInputType(),
                        null,
                        false);

        // 调用重载方法，执行 `select` 逻辑
        return select(
                timedOutPartialMatchesTag,
                patternTimeoutFunction,
                rightTypeInfo,
                patternSelectFunction);
    }

    /**
     * 处理匹配的模式序列，并应用 `select` 逻辑，同时处理超时的部分匹配事件。
     *
     * <p>对于匹配到的模式，会调用提供的 {@link PatternSelectFunction} 进行处理，并返回一个 `DataStream`。
     * <p>对于部分匹配但因超时未完成的模式，会调用提供的 {@link PatternTimeoutFunction} 处理超时数据，并将结果发送到侧输出流（Side Output）。
     *
     * <p>侧输出流可以通过 {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} 获取。
     *
     * @param timedOutPartialMatchesTag 侧输出流的 {@link OutputTag}，用于标识超时模式
     * @param patternTimeoutFunction 处理超时模式的函数
     * @param outTypeInfo 明确指定的输出类型信息
     * @param patternSelectFunction 处理匹配成功模式的函数
     * @param <L> 超时模式的输出类型
     * @param <R> 匹配成功模式的输出类型
     * @return 处理结果的 {@link DataStream}，其中包含匹配结果，超时数据会存入侧输出流
     */
    public <L, R> SingleOutputStreamOperator<R> select(
            final OutputTag<L> timedOutPartialMatchesTag,
            final PatternTimeoutFunction<T, L> patternTimeoutFunction,
            final TypeInformation<R> outTypeInfo,
            final PatternSelectFunction<T, R> patternSelectFunction) {

        // 构建 `PatternProcessFunction`，同时处理匹配事件和超时事件
        final PatternProcessFunction<T, R> processFunction =
                fromSelect(builder.clean(patternSelectFunction))
                        .withTimeoutHandler(
                                timedOutPartialMatchesTag, builder.clean(patternTimeoutFunction))
                        .build();

        // 调用 `process` 方法执行匹配后的处理逻辑
        return process(processFunction, outTypeInfo);
    }


    /**
     * 对检测到的模式序列应用 `select` 逻辑，同时处理超时的部分匹配事件。
     *
     * <p>对于匹配到的模式，会调用提供的 {@link PatternSelectFunction} 进行处理，并返回一个 `DataStream`。
     * <p>对于部分匹配但因超时未完成的模式，会调用提供的 {@link PatternTimeoutFunction} 处理超时数据，并将结果封装在 `Either<L, R>` 类型中，
     * 其中 `L` 代表超时数据类型，`R` 代表正常匹配结果类型。
     *
     * <p><b>此方法已被弃用</b>，建议使用 {@link PatternStream#select(OutputTag, PatternTimeoutFunction, PatternSelectFunction)}，
     * 该方法可以将超时事件作为侧输出（Side Output）进行处理。
     *
     * @param patternTimeoutFunction 处理超时模式的函数
     * @param patternSelectFunction 处理匹配成功模式的函数
     * @param <L> 超时模式的输出类型
     * @param <R> 匹配成功模式的输出类型
     * @return 处理结果的 {@link DataStream}，其中包含匹配结果或超时结果（使用 `Either<L, R>` 进行封装）
     */
    @Deprecated
    public <L, R> SingleOutputStreamOperator<Either<L, R>> select(
            final PatternTimeoutFunction<T, L> patternTimeoutFunction,
            final PatternSelectFunction<T, R> patternSelectFunction) {

        // 提取 `PatternSelectFunction` 返回的输出类型信息
        final TypeInformation<R> mainTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternSelectFunction,
                        PatternSelectFunction.class,
                        0,
                        1,
                        TypeExtractor.NO_INDEX,
                        builder.getInputType(),
                        null,
                        false);

        // 提取 `PatternTimeoutFunction` 返回的超时数据类型信息
        final TypeInformation<L> timeoutTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternTimeoutFunction,
                        PatternTimeoutFunction.class,
                        0,
                        1,
                        TypeExtractor.NO_INDEX,
                        builder.getInputType(),
                        null,
                        false);

        // 组合 `Either<L, R>` 类型信息，用于同时存储匹配结果和超时数据
        final TypeInformation<Either<L, R>> outTypeInfo =
                new EitherTypeInfo<>(timeoutTypeInfo, mainTypeInfo);

        // 生成唯一标识的 `OutputTag`，用于存储超时数据的侧输出流
        final OutputTag<L> outputTag =
                new OutputTag<>(UUID.randomUUID().toString(), timeoutTypeInfo);

        // 构建 `PatternProcessFunction`，用于同时处理匹配数据和超时数据
        final PatternProcessFunction<T, R> processFunction =
                fromSelect(builder.clean(patternSelectFunction))
                        .withTimeoutHandler(outputTag, builder.clean(patternTimeoutFunction))
                        .build();

        // 处理主匹配流
        final SingleOutputStreamOperator<R> mainStream = process(processFunction, mainTypeInfo);
        // 获取超时数据的侧输出流
        final DataStream<L> timedOutStream = mainStream.getSideOutput(outputTag);

        // 连接匹配流和超时流，并映射到 `Either<L, R>` 结构
        return mainStream.connect(timedOutStream).map(new CoMapTimeout<>()).returns(outTypeInfo);
    }

    /**
     * 对检测到的模式序列应用 `flatSelect` 逻辑。
     *
     * <p>对于每个匹配的模式序列，会调用提供的 {@link PatternFlatSelectFunction} 进行处理，并可以生成多个输出元素。
     *
     * @param patternFlatSelectFunction 处理匹配模式的 `flatSelect` 逻辑
     * @param <R> 处理后返回的元素类型
     * @return 包含 `flatSelect` 处理结果的 {@link DataStream}
     */
    public <R> SingleOutputStreamOperator<R> flatSelect(
            final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {
        // 由于 `TypeExtractor` 无法直接从 `MapFunction` 中提取类型，因此手动获取返回类型信息

        final TypeInformation<R> outTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternFlatSelectFunction,
                        PatternFlatSelectFunction.class,
                        0,
                        1,
                        new int[] {1, 0}, // 指定索引以匹配 `flatSelect` 返回的泛型类型
                        builder.getInputType(),
                        null,
                        false);

        return flatSelect(patternFlatSelectFunction, outTypeInfo);
    }

    /**
     * 对检测到的模式序列应用 `flatSelect` 逻辑，并显式指定输出类型信息。
     *
     * <p>对于每个匹配的模式序列，会调用提供的 {@link PatternFlatSelectFunction} 进行处理，并可以生成多个输出元素。
     *
     * @param patternFlatSelectFunction 处理匹配模式的 `flatSelect` 逻辑
     * @param <R> 处理后返回的元素类型
     * @param outTypeInfo 显式指定的输出类型信息
     * @return 包含 `flatSelect` 处理结果的 {@link DataStream}
     */
    public <R> SingleOutputStreamOperator<R> flatSelect(
            final PatternFlatSelectFunction<T, R> patternFlatSelectFunction,
            final TypeInformation<R> outTypeInfo) {

        // 构建 `PatternProcessFunction`，用于 `flatSelect` 处理
        final PatternProcessFunction<T, R> processFunction =
                fromFlatSelect(builder.clean(patternFlatSelectFunction)).build();

        return process(processFunction, outTypeInfo);
    }


    /**
     * 对检测到的模式序列应用 `flatSelect` 逻辑，同时处理超时的部分匹配事件。
     *
     * <p>对于每个匹配的模式序列，会调用提供的 {@link PatternFlatSelectFunction} 进行处理，可以生成多个输出元素。
     * <p>对于部分匹配但因超时未完成的模式，会调用提供的 {@link PatternFlatTimeoutFunction} 处理超时数据，
     * 并将结果存储在侧输出流中（Side Output）。
     *
     * <p>可以通过 {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} 获取超时事件的流，
     * 该流的 {@link OutputTag} 需与此方法的 `timedOutPartialMatchesTag` 相同。
     *
     * @param timedOutPartialMatchesTag {@link OutputTag}，用于标识超时模式的侧输出流
     * @param patternFlatTimeoutFunction 处理超时模式的函数
     * @param patternFlatSelectFunction 处理匹配模式的函数
     * @param <L> 超时模式的输出数据类型
     * @param <R> 匹配模式的输出数据类型
     * @return 包含匹配结果的 {@link DataStream}，同时超时数据存储在侧输出流中
     */
    public <L, R> SingleOutputStreamOperator<R> flatSelect(
            final OutputTag<L> timedOutPartialMatchesTag,
            final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
            final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

        // 提取 `PatternFlatSelectFunction` 返回的输出类型信息
        final TypeInformation<R> rightTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternFlatSelectFunction,
                        PatternFlatSelectFunction.class,
                        0,
                        1,
                        new int[] {1, 0}, // 指定索引以匹配 `flatSelect` 返回的泛型类型
                        builder.getInputType(),
                        null,
                        false);

        return flatSelect(
                timedOutPartialMatchesTag,
                patternFlatTimeoutFunction,
                rightTypeInfo,
                patternFlatSelectFunction);
    }

    /**
     * 对检测到的模式序列应用 `flatSelect` 逻辑，并显式指定输出类型，同时处理超时的部分匹配事件。
     *
     * <p>对于每个匹配的模式序列，会调用提供的 {@link PatternFlatSelectFunction} 进行处理，可以生成多个输出元素。
     * <p>对于部分匹配但因超时未完成的模式，会调用提供的 {@link PatternFlatTimeoutFunction} 处理超时数据，
     * 并将结果存储在侧输出流中（Side Output）。
     *
     * <p>可以通过 {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} 获取超时事件的流，
     * 该流的 {@link OutputTag} 需与此方法的 `timedOutPartialMatchesTag` 相同。
     *
     * @param timedOutPartialMatchesTag {@link OutputTag}，用于标识超时模式的侧输出流
     * @param patternFlatTimeoutFunction 处理超时模式的函数
     * @param patternFlatSelectFunction 处理匹配模式的函数
     * @param outTypeInfo 显式指定的输出类型信息
     * @param <L> 超时模式的输出数据类型
     * @param <R> 匹配模式的输出数据类型
     * @return 包含匹配结果的 {@link DataStream}，同时超时数据存储在侧输出流中
     */
    public <L, R> SingleOutputStreamOperator<R> flatSelect(
            final OutputTag<L> timedOutPartialMatchesTag,
            final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
            final TypeInformation<R> outTypeInfo,
            final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

        // 构建 `PatternProcessFunction`，用于 `flatSelect` 处理，同时处理超时数据
        final PatternProcessFunction<T, R> processFunction =
                fromFlatSelect(builder.clean(patternFlatSelectFunction))
                        .withTimeoutHandler(
                                timedOutPartialMatchesTag,
                                builder.clean(patternFlatTimeoutFunction))
                        .build();

        return process(processFunction, outTypeInfo);
    }


    /**
     * 对检测到的模式序列应用 `flatSelect` 逻辑，并处理超时的部分匹配事件。
     *
     * <p>对于每个匹配的模式序列，会调用提供的 {@link PatternFlatSelectFunction} 进行处理，能够产生多个输出元素。
     * <p>对于部分匹配但因超时未完成的模式，会调用提供的 {@link PatternFlatTimeoutFunction} 处理超时数据，
     * 该方法可以返回多个超时元素。
     *
     * <p>**注意：该方法已被弃用**，请使用 {@link PatternStream#flatSelect(OutputTag, PatternFlatTimeoutFunction,
     * PatternFlatSelectFunction)} 代替，该方法会将超时事件存储到侧输出流（Side Output）。
     *
     * @param patternFlatTimeoutFunction 处理超时事件的 `flatTimeout` 处理函数
     * @param patternFlatSelectFunction 处理匹配事件的 `flatSelect` 处理函数
     * @param <L> 超时事件的输出类型
     * @param <R> 匹配事件的输出类型
     * @deprecated 请使用 {@link PatternStream#flatSelect(OutputTag, PatternFlatTimeoutFunction, PatternFlatSelectFunction)}
     * @return 包含匹配结果的 {@link DataStream}，同时超时事件以 {@link Either} 类型的形式返回
     */
    @Deprecated
    public <L, R> SingleOutputStreamOperator<Either<L, R>> flatSelect(
            final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
            final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

        // 提取 `PatternFlatTimeoutFunction` 返回的超时事件的类型信息
        final TypeInformation<L> timedOutTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternFlatTimeoutFunction,
                        PatternFlatTimeoutFunction.class,
                        0,
                        1,
                        new int[] {2, 0}, // 指定索引，解析 flatTimeout 返回的泛型类型
                        builder.getInputType(),
                        null,
                        false);

        // 提取 `PatternFlatSelectFunction` 返回的正常匹配事件的类型信息
        final TypeInformation<R> mainTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        patternFlatSelectFunction,
                        PatternFlatSelectFunction.class,
                        0,
                        1,
                        new int[] {1, 0}, // 指定索引，解析 flatSelect 返回的泛型类型
                        builder.getInputType(),
                        null,
                        false);

        // 创建唯一的 `OutputTag` 用于标识超时事件的侧输出流
        final OutputTag<L> outputTag =
                new OutputTag<>(UUID.randomUUID().toString(), timedOutTypeInfo);

        // 构建 `PatternProcessFunction`，用于执行 flatSelect 逻辑，同时处理超时事件
        final PatternProcessFunction<T, R> processFunction =
                fromFlatSelect(builder.clean(patternFlatSelectFunction))
                        .withTimeoutHandler(outputTag, builder.clean(patternFlatTimeoutFunction))
                        .build();

        // 处理匹配事件，并获取主数据流
        final SingleOutputStreamOperator<R> mainStream = process(processFunction, mainTypeInfo);

        // 从主数据流中获取超时事件的侧输出流
        final DataStream<L> timedOutStream = mainStream.getSideOutput(outputTag);

        // 将超时事件与正常匹配事件的类型封装为 `Either` 类型，支持两种类型的数据
        final TypeInformation<Either<L, R>> outTypeInfo =
                new EitherTypeInfo<>(timedOutTypeInfo, mainTypeInfo);

        // 通过 `connect` 将匹配事件流和超时事件流合并，并转换为 `Either` 类型
        return mainStream.connect(timedOutStream).map(new CoMapTimeout<>()).returns(outTypeInfo);
    }

    /**
     * 用于合并匹配结果和超时侧输出结果，以确保 API 兼容性。
     *
     * <p>此类实现了 `CoMapFunction`，用于将正常匹配事件 (`Right`) 和超时事件 (`Left`) 转换为 `Either<L, R>` 类型。
     *
     * @param <R> 匹配事件的类型
     * @param <L> 超时事件的类型
     */
    @Internal
    public static class CoMapTimeout<R, L> implements CoMapFunction<R, L, Either<L, R>> {

        private static final long serialVersionUID = 2059391566945212552L;

        /**
         * 处理正常匹配事件，将其封装为 `Either.Right`。
         *
         * @param value 匹配到的事件
         * @return `Either.Right(value)`，表示正常匹配的结果
         */
        @Override
        public Either<L, R> map1(R value) {
            return Either.Right(value);
        }

        /**
         * 处理超时事件，将其封装为 `Either.Left`。
         *
         * @param value 发生超时的事件
         * @return `Either.Left(value)`，表示超时事件
         */
        @Override
        public Either<L, R> map2(L value) {
            return Either.Left(value);
        }
    }

}
