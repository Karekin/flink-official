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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;


/**
 * {@code Triggerable} 是一个 **回调接口**，用于 **处理 Flink 内部定时器（Timer）的触发事件**。
 *
 * <p>它由 {@link InternalTimerService} 调用，支持 **事件时间（Event Time）和 处理时间（Processing Time）** 两种类型的定时器。
 *
 * <h2>核心作用</h2>
 * <ul>
 *     <li>当 **事件时间定时器** 触发时，调用 {@code onEventTime(InternalTimer<K, N> timer)} 方法。</li>
 *     <li>当 **处理时间定时器** 触发时，调用 {@code onProcessingTime(InternalTimer<K, N> timer)} 方法。</li>
 * </ul>
 *
 * <h2>定时器的工作原理</h2>
 * <p>在 Flink 任务中，可以通过 `TimerService` 或 `InternalTimerService` 注册定时器。
 * 当定时器到期时，Flink 会回调 **算子（Operator）或 KeyedState** 中实现的 `Triggerable` 方法，触发相应的业务逻辑。</p>
 *
 * <h3>事件流转流程：</h3>
 * <pre>
 *     1. 在 Flink 任务中，用户可以调用 `timerService.registerEventTimeTimer()` 或 `timerService.registerProcessingTimeTimer()` 注册定时器。
 *     2. Flink 在 **内部 TimerQueue** 维护定时器，并在对应的时间点触发它。
 *     3. 定时器触发后，Flink **回调算子（Operator）中的 `Triggerable` 实现类**：
 *        - **事件时间定时器** 触发时，调用 `onEventTime(InternalTimer<K, N> timer)` 方法。
 *        - **处理时间定时器** 触发时，调用 `onProcessingTime(InternalTimer<K, N> timer)` 方法。
 * </pre>
 *
 * <h2>泛型参数</h2>
 * <ul>
 *     <li><b>K</b>：定时器作用的 Key 类型（适用于 KeyedStream）。</li>
 *     <li><b>N</b>：定时器作用的命名空间（适用于窗口等场景）。</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <p>在自定义 `KeyedProcessOperator` 中，可以实现 `Triggerable` 接口来处理定时器事件：</p>
 *
 * <pre>{@code
 * public class MyKeyedProcessOperator<KEY, IN, OUT> extends AbstractStreamOperator<OUT>
 *         implements OneInputStreamOperator<IN, OUT>,
 *         Triggerable<KEY, VoidNamespace> {  // 实现 Triggerable
 *
 *     private transient TimerService timerService;
 *
 *     @Override
 *     public void open() throws Exception {
 *         super.open();
 *         InternalTimerService<VoidNamespace> internalTimerService =
 *                 getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
 *         timerService = new SimpleTimerService(internalTimerService);
 *     }
 *
 *     @Override
 *     public void processElement(StreamRecord<IN> element) throws Exception {
 *         // 注册事件时间定时器
 *         timerService.registerEventTimeTimer(element.getTimestamp() + 10);
 *     }
 *
 *     @Override
 *     public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
 *         LOG.info("事件时间定时器触发: {}", timer);
 *     }
 *
 *     @Override
 *     public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
 *         LOG.info("处理时间定时器触发: {}", timer);
 *     }
 * }
 * }</pre>
 *
 * <h2>总结</h2>
 * <ul>
 *     <li>**Triggerable** 是 Flink 内部的 **定时器回调接口**，用于处理 **事件时间 & 处理时间** 定时器触发的回调。</li>
 *     <li>它由 {@link InternalTimerService} 调用，广泛用于 **窗口（Window）、KeyedProcessFunction** 等场景。</li>
 *     <li>定时器触发后，Flink 会回调 `onEventTime()` 或 `onProcessingTime()` 方法，执行相应的业务逻辑。</li>
 * </ul>
 *
 * @param <K> 定时器作用的 Key 类型（适用于 KeyedStream）。
 * @param <N> 定时器作用的命名空间（适用于窗口等场景）。
 */
@Internal
public interface Triggerable<K, N> {

    /**
     * 当 **事件时间定时器** 触发时调用。
     *
     * <p>该方法由 Flink **内部定时器服务（InternalTimerService）** 触发。
     * 适用于基于 **事件时间（Event Time）** 的定时任务，例如：
     * <ul>
     *     <li>基于 **Watermark** 触发窗口计算。</li>
     *     <li>基于 **事件时间** 执行某些延迟任务。</li>
     * </ul>
     *
     * <p>注意：
     * <ul>
     *     <li>定时器的触发时间 **取决于 Flink 处理的 Watermark**。</li>
     *     <li>当 Watermark >= Timer 的注册时间时，Flink 执行此方法。</li>
     * </ul>
     *
     * @param timer 触发的定时器实例，包含 key 和时间信息。
     * @throws Exception 可能抛出的异常。
     */
    void onEventTime(InternalTimer<K, N> timer) throws Exception;

    /**
     * 当 **处理时间定时器** 触发时调用。
     *
     * <p>该方法由 Flink **内部定时器服务（InternalTimerService）** 触发。
     * 适用于基于 **处理时间（Processing Time）** 的定时任务，例如：
     * <ul>
     *     <li>定期执行清理任务，例如清理过期状态。</li>
     *     <li>在指定时间间隔内执行周期性任务。</li>
     * </ul>
     *
     * <p>注意：
     * <ul>
     *     <li>定时器的触发时间 **取决于 Flink 运行环境的系统时间**。</li>
     *     <li>当系统时间 >= Timer 注册时间时，Flink 触发该方法。</li>
     * </ul>
     *
     * @param timer 触发的定时器实例，包含 key 和时间信息。
     * @throws Exception 可能抛出的异常。
     */
    void onProcessingTime(InternalTimer<K, N> timer) throws Exception;
}

