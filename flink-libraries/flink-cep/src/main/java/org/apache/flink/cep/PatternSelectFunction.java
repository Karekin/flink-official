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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * `PatternSelectFunction` 是一个模式选择函数的基础接口，用于在检测到特定模式时生成输出结果。
 *
 * <p>当模式匹配成功时，该函数会被调用，并接收到一个包含匹配事件的 `Map`，用户可以根据事件名称访问匹配的事件。
 * 事件名称取决于 {@link org.apache.flink.cep.pattern.Pattern} 的定义。
 *
 * <p>该 `select` 方法返回**恰好一个**输出结果。如果需要返回多个结果，可以使用 {@link PatternFlatSelectFunction}。
 *
 * <p>示例代码：
 * <pre>{@code
 * PatternStream<IN> pattern = ...;
 *
 * DataStream<OUT> result = pattern.select(new MyPatternSelectFunction());
 * }</pre>
 *
 * @param <IN> 输入事件类型
 * @param <OUT> 输出结果类型
 */
public interface PatternSelectFunction<IN, OUT> extends Function, Serializable {

    /**
     * 在检测到匹配的事件模式后，该方法被调用，并根据提供的事件 `Map` 生成一个输出结果。
     *
     * <p>`pattern` 参数是一个 `Map`，其中 key 是模式中的事件名称（由 `Pattern` 定义），
     * value 是符合该模式的事件列表。用户可以使用该 `Map` 来提取匹配到的事件，并进行处理。
     *
     * <p>**注意：**
     * - 该方法**必须**返回**恰好一个**结果。
     * - 如果需要返回多个结果，请使用 {@link PatternFlatSelectFunction}。
     * - 该方法可能会抛出异常，异常会导致任务失败，并可能触发 Flink 作业恢复。
     *
     * @param pattern 包含匹配到的模式事件的 `Map`，其中 key 为事件名称，value 为对应的事件列表
     * @return 处理后的单个结果
     * @throws Exception 可能抛出的异常，若抛出异常，则该操作会失败，并可能触发恢复
     */
    OUT select(Map<String, List<IN>> pattern) throws Exception;
}

