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

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 用于复杂事件处理的工具类。
 *
 * <p>此类包含将 {@link DataStream} 转换为 {@link PatternStream} 以执行复杂事件处理（CEP）的方法。
 */
public class CEP {

    /**
     * 从输入数据流和模式创建一个 {@link PatternStream}。
     *
     * @param input 包含输入事件的 DataStream
     * @param pattern 要检测的模式规范
     * @param <T> 输入事件的类型
     * @return 生成的模式流
     */
    public static <T> PatternStream<T> pattern(DataStream<T> input, Pattern<T, ?> pattern) {
        return new PatternStream<>(input, pattern); // 创建一个新的 PatternStream 对象并返回
    }

    /**
     * 从输入数据流和模式创建一个 {@link PatternStream}，并提供一个比较器来排序具有相同时间戳的事件。
     *
     * @param input 包含输入事件的 DataStream
     * @param pattern 要检测的模式规范
     * @param comparator 用于排序具有相同时间戳事件的比较器
     * @param <T> 输入事件的类型
     * @return 生成的模式流，使用了自定义比较器
     */
    public static <T> PatternStream<T> pattern(
            DataStream<T> input, Pattern<T, ?> pattern, EventComparator<T> comparator) {
        final PatternStream<T> stream = new PatternStream<>(input, pattern); // 创建一个新的 PatternStream 对象
        return stream.withComparator(comparator); // 设置自定义的比较器并返回
    }
}

