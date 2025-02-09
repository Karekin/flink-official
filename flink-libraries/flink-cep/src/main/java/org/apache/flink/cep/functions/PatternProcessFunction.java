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

package org.apache.flink.cep.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.cep.time.TimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * `PatternProcessFunction` 用于处理 Flink CEP 模式匹配后生成的结果。
 *
 * <p>当流数据匹配到指定的 {@link org.apache.flink.cep.pattern.Pattern} 模式后，
 * Flink 会调用 `processMatch` 方法，并传入匹配到的事件列表。用户可以在 `processMatch`
 * 方法中自定义逻辑，例如转换数据、聚合、发送到侧输出等。
 *
 * <p>**示例代码:**
 *
 * <pre>{@code
 * // 定义一个模式匹配流
 * PatternStream<Event> patternStream = ...
 *
 * // 处理匹配到的模式
 * DataStream<Result> resultStream = patternStream.process(new MyPatternProcessFunction());
 * }</pre>
 *
 * @param <IN>  输入数据类型，即流中原始事件类型
 * @param <OUT> 输出数据类型，即匹配后生成的结果类型
 */
@PublicEvolving
public abstract class PatternProcessFunction<IN, OUT> extends AbstractRichFunction {

    /**
     * 处理匹配成功的事件，并生成输出数据。
     *
     * <p>当 `CEP` 匹配到模式后，会调用 `processMatch` 方法，并将匹配到的事件传递进来。
     * 这些事件以 `Map<String, List<IN>>` 的形式存储，键为模式中的事件名称，值为匹配的事件列表。
     *
     * <p>**时间相关信息:**
     * - `Context.timestamp()` 返回的是 **最后一个匹配到的事件的时间戳**，即导致该匹配成功的事件时间。
     * - 可以使用 `ctx.output()` 将数据发送到侧输出流。
     *
     * <p>**示例代码:**
     * <pre>{@code
     * public class MyPatternProcessFunction extends PatternProcessFunction<Event, String> {
     *     @Override
     *     public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
     *         Event startEvent = match.get("start").get(0);
     *         Event endEvent = match.get("end").get(0);
     *         out.collect("Detected Pattern: " + startEvent + " -> " + endEvent);
     *     }
     * }
     * }</pre>
     *
     * @param match 匹配的事件集合，键是模式中定义的事件名称，值是匹配到的事件列表
     * @param ctx 上下文对象，提供时间信息和侧输出功能
     * @param out 结果收集器，用于向主流输出匹配的结果
     * @throws Exception 可能会抛出异常，抛出异常会导致任务失败，并可能触发恢复机制
     */
    public abstract void processMatch(
            final Map<String, List<IN>> match, final Context ctx, final Collector<OUT> out)
            throws Exception;

    /**
     * `Context` 接口提供了时间相关信息，并支持将数据发送到侧输出流。
     */
    public interface Context extends TimeContext {

        /**
         * 发送数据到指定的侧输出流 (Side Output)。
         *
         * <p>可以使用 `OutputTag<X>` 标识侧输出流，并通过 `context.output()` 将数据发送到该流。
         * 这在处理迟到数据或需要分流不同类型数据时非常有用。
         *
         * <p>**示例代码:**
         * <pre>{@code
         * OutputTag<Event> lateDataTag = new OutputTag<Event>("late-data"){};
         *
         * public class MyPatternProcessFunction extends PatternProcessFunction<Event, String> {
         *     @Override
         *     public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
         *         Event event = match.get("start").get(0);
         *         if (event.isLate()) {
         *             ctx.output(lateDataTag, event);
         *         } else {
         *             out.collect("Matched: " + event);
         *         }
         *     }
         * }
         * }</pre>
         *
         * @param outputTag 侧输出流的 `OutputTag` 标识符
         * @param value 需要发送到侧输出流的值
         * @param <X> 侧输出流的数据类型
         */
        <X> void output(final OutputTag<X> outputTag, final X value);
    }
}

