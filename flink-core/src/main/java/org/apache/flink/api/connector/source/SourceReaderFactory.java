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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * SourceReaderFactory 提供了创建 SourceReader 实例的能力。
 * <p>在 Flink 中，每个 SourceReader 负责从一个或多个 Split（分片）中读取数据并将其转化为
 * 下游可以处理的格式。此工厂接口定义了如何创建一个无状态的 SourceReader，用于在作业开始时
 * 或在故障恢复（需要重新分配分片时）进行新的读取任务。</p>
 *
 * @param <T>      此 SourceReader 生成的输出元素类型。
 * @param <SplitT> 此 SourceReader 读取的分片类型，必须实现 {@link SourceSplit}。
 */
@Public
public interface SourceReaderFactory<T, SplitT extends SourceSplit> extends Serializable {

    /**
     * 创建一个新的 SourceReader，用于读取分配到的分片数据。此方法所返回的 SourceReader
     * 在初始状态下并没有任何待处理的分片，直到框架分配分片给它后才会开始工作。
     *
     * @param readerContext 当前 SourceReader 的上下文环境。包含诸如并行子任务索引（subtask index）
     *                      等信息，以及与外部系统交互的基础设施（如线程模型、状态后端等）。
     * @return 新创建的 SourceReader 实例。
     * @throws Exception 如果在创建 SourceReader 过程中出现任何异常，实现方可以直接抛出。抛出异常后，
     *                   任务将标记为失败并触发故障恢复流程。
     */
    SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception;
}

