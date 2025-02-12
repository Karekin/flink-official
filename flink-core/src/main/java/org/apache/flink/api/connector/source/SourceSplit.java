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

/**
 * SourceSplit 是所有分片（Split）类型的通用接口。
 * <p>在 Flink 的数据读取框架中，Source 需要将数据拆分为多个独立的片段（Split），
 * 这些 Split 会被分配给不同的 SourceReader 并行任务进行读取。
 * 该接口定义了 Split 需要具备的基本属性，即一个唯一的 Split ID。</p>
 *
 * <p>通常，每种数据源（如文件、Kafka 分区、数据库表等）会定义自己的 Split 实现，
 * 例如：
 * <ul>
 *     <li>文件存储：每个文件或文件的某一范围（偏移量）可以是一个 Split。</li>
 *     <li>Kafka：每个 Kafka 分区可以是一个 Split。</li>
 *     <li>数据库 CDC：每个数据库表或表的某个时间段的数据变更可以是一个 Split。</li>
 * </ul>
 * </p>
 */
@Public
public interface SourceSplit {

    /**
     * 获取此 Split 的唯一标识 ID。
     * <p>该 ID 用于标识不同的 Split，并在任务恢复时保持一致性。例如，在文件读取场景下，
     * Split ID 可能是文件路径或文件名；在 Kafka 读取场景下，Split ID 可能是 "topic-partition"。</p>
     *
     * @return 该分片的唯一标识符（字符串格式）。
     */
    String splitId();
}

