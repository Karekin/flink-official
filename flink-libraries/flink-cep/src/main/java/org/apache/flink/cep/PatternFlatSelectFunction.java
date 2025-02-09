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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * `PatternFlatSelectFunction` 是一个模式选择函数的基础接口，它允许在模式匹配成功时生成**多个**输出结果。
 *
 * <p>与 {@link PatternSelectFunction} 不同，该接口不会返回单个结果，而是使用 `Collector`
 * 发送**零个或多个**输出结果。用户可以根据匹配到的事件模式，通过 `Collector` 进行结果收集。
 *
 * <p>模式匹配时，该函数会被调用，并传入一个 `Map`，其中包含匹配的事件，事件由 `Pattern` 定义的名称标识。
 *
 * <p>示例代码：
 * <pre>{@code
 * PatternStream<IN> pattern = ...;
 *
 * DataStream<OUT> result = pattern.flatSelect(new MyPatternFlatSelectFunction());
 * }</pre>
 *
 * @param <IN> 输入事件类型
 * @param <OUT> 输出结果类型
 */
public interface PatternFlatSelectFunction<IN, OUT> extends Function, Serializable {

    /**
     * 在检测到匹配的事件模式后，该方法被调用，并根据提供的事件 `Map` 生成**零个或多个**输出结果。
     *
     * <p>`pattern` 参数是一个 `Map`，其中：
     * - **key**：模式中的事件名称（由 `Pattern` 定义）
     * - **value**：符合该模式的事件列表
     *
     * <p>用户需要调用 `Collector` 的 `collect()` 方法来输出匹配的结果。如果没有匹配到合适的结果，可以不调用 `collect()`，
     * 这意味着不会产生任何输出。
     *
     * <p>**注意：**
     * - 该方法可以返回**多个**结果，而 {@link PatternSelectFunction} 仅能返回一个。
     * - 该方法可能会抛出异常，异常会导致任务失败，并可能触发 Flink 作业恢复。
     *
     * @param pattern 包含匹配到的模式事件的 `Map`，其中 key 为事件名称，value 为对应的事件列表
     * @param out 用于输出结果的 `Collector`，用户需要调用 `collect()` 方法收集输出数据
     * @throws Exception 可能抛出的异常，若抛出异常，则该操作会失败，并可能触发恢复
     */
    void flatSelect(Map<String, List<IN>> pattern, Collector<OUT> out) throws Exception;
}
