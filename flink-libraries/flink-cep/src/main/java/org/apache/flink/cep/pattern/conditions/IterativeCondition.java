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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.cep.time.TimeContext;

import java.io.Serializable;

/**
 * 用户自定义的条件，用于决定模式中的元素是否应该被接受。
 * 接受一个元素也意味着相应的 {@link org.apache.flink.cep.nfa.NFA} 进行状态转换。
 *
 * <p>该条件可以是一个简单的过滤条件，也可以是一个更复杂的条件，
 * 该条件需要遍历模式中已经接受的元素，并基于这些元素的某些统计信息来决定是否接受新元素。
 *
 * <p>如果只是进行简单过滤，应该继承 {@link SimpleCondition} 类；
 * 如果需要访问已经接受的元素（即迭代条件），则应继承本类 {@link IterativeCondition}，
 * 通过 {@link Context} 访问之前匹配的元素。
 *
 * <p>例如，下面的迭代条件：
 * 1. 只接受名称为 "middle" 的事件；
 * 2. 之前所有已接受事件的 price 总和加上当前事件的 price 不能超过 5。
 *
 * <pre>{@code
 * private class MyCondition extends IterativeCondition<Event> {
 *
 *     @Override
 *     public boolean filter(Event value, Context<Event> ctx) throws Exception {
 *         // 只有名称为 "middle" 的事件才会被接受
 *         if (!value.getName().equals("middle")) {
 *             return false;
 *         }
 *
 *         // 计算所有已接受的事件的 price 总和
 *         double sum = 0.0;
 *         for (Event e : ctx.getEventsForPattern("middle")) {
 *             sum += e.getPrice();
 *         }
 *         sum += value.getPrice();
 *
 *         // 只有 price 总和不超过 5 才接受该事件
 *         return Double.compare(sum, 5.0) <= 0;
 *     }
 * }
 * }</pre>
 *
 * <b>注意：</b> 调用 {@link Context#getEventsForPattern(String) getEventsForPattern(...)}
 * 需要从 NFA 存储的所有元素中找到属于该模式的元素。
 * 这个操作的开销可能较大，因此在实现自定义条件时，应尽量减少该方法的调用次数。
 */
@PublicEvolving
public abstract class IterativeCondition<T> implements Function, Serializable {

    private static final long serialVersionUID = 7067817235759351255L;

    /**
     * 过滤函数，用于评估条件是否满足。
     *
     * <p><strong>重要：</strong> 系统假定该函数不会修改应用于过滤的元素。
     * 违反该假设可能导致错误的计算结果。
     *
     * @param value 需要测试的值（当前事件）。
     * @param ctx   {@link Context} 提供对已经接受的事件的访问能力，
     *              例如使用 {@link Context#getEventsForPattern(String)} 访问之前匹配的事件。
     * @return 如果返回 {@code true}，表示该值应被接受；返回 {@code false}，表示该值应被过滤掉。
     * @throws Exception 该方法可能抛出异常。抛出异常将导致操作失败，并可能触发故障恢复。
     */
    public abstract boolean filter(T value, Context<T> ctx) throws Exception;

    /**
     * 评估 {@link IterativeCondition 条件} 时使用的上下文对象。
     */
    public interface Context<T> extends TimeContext {

        /**
         * 获取已经接受的、属于指定模式的事件集合。
         * 事件按照它们被插入模式的顺序进行迭代。
         *
         * @param name 模式的名称。
         * @return 一个 {@link Iterable}，用于遍历该模式下已经接受的所有事件。
         * @throws Exception 可能抛出的异常。
         */
        Iterable<T> getEventsForPattern(String name) throws Exception;
    }
}

