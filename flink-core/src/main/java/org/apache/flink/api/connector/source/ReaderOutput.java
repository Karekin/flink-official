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
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;

/**
 * Flink 运行时提供的 ReaderOutput 接口，供 {@link SourceReader} 用于向下游算子发送记录和水位线（Watermark）。
 *
 * <p>{@code ReaderOutput} 继承自 {@link SourceOutput}，可直接用于输出数据流中的事件。
 * 对于仅处理单个 Split 的 Source，或无需 Split 级别特性（如每个 Split 单独的水位线、空闲检测等）的 Source，
 * 可以直接使用该接口进行数据输出。例如：处理批量数据（Bounded Source）时，不需要关注 Split 级别的时间管理。</p>
 *
 * <p>对于大多数流式数据源（Streaming Source），通常应使用 Split 级别的 {@code SourceOutput}，
 * 以便在每个 Split 级别执行水位线生成、空闲检测等逻辑。要为特定的 Split 创建 {@code SourceOutput}，
 * 可以调用 {@link ReaderOutput#createOutputForSplit(String)} 方法，并传入 Split ID。
 * 在处理完成该 Split 后，请务必调用 {@link ReaderOutput#releaseOutputForSplit(String)} 释放资源，
 * 否则可能会影响水位线计算，导致全局水位线停滞。</p>
 */
@Public
public interface ReaderOutput<T> extends SourceOutput<T> {

    /**
     * 发送一个不带时间戳的记录。
     *
     * <p>如果数据源不包含时间戳（如纯文本文件读取），可以使用此方法。
     * 下游算子可以通过 {@link TimestampAssigner} 为记录附加时间戳。
     * 例如：如果数据源是 JSON 格式的文件，文件本身没有时间戳信息，则可以使用该方法输出记录，
     * 然后让 {@code TimestampAssigner} 从 JSON 字段中提取时间戳。</p>
     *
     * @param record 要发送的记录。
     */
    @Override
    void collect(T record);

    /**
     * 发送一个带时间戳的记录。
     *
     * <p>如果数据源本身携带时间戳信息（如 Kafka、日志系统等），可以使用此方法。
     * 典型场景包括：
     * <ul>
     *     <li>Kafka 主题中的消息通常带有时间戳，可以直接使用。</li>
     *     <li>日志流处理（如 Flink 处理 ClickStream）时，日志记录中可能有时间戳字段。</li>
     * </ul>
     * 该时间戳可以被 {@link TimestampAssigner} 进一步处理，决定是否使用此时间戳，或从记录内部提取新的时间戳。</p>
     *
     * @param record    要发送的记录。
     * @param timestamp 记录的时间戳（通常为事件时间）。
     */
    @Override
    void collect(T record, long timestamp);

    /**
     * 发送水位线（Watermark）。
     *
     * <p>水位线用于事件时间窗口计算，表示截至某个时间点，所有早于该时间的事件均已处理完成。
     * 发送水位线后，会自动将当前流标记为 **活跃状态**，取消之前的空闲（Idle）标记。</p>
     *
     * @param watermark 要发送的水位线对象。
     */
    @Override
    void emitWatermark(Watermark watermark);

    /**
     * 将当前输出标记为 **空闲**（Idle），表示下游算子无需再等待该流的数据或水位线。
     *
     * <p>在流式数据处理中，部分 Source 可能会长时间不产生数据（如 Kafka 某个分区无新消息）。
     * 标记 Idle 后，下游算子（如窗口算子）不会再依赖此流的水位线，以避免窗口计算被阻塞。</p>
     *
     * <p>一旦该 Source 重新发送水位线，该流会自动恢复为活跃状态。</p>
     */
    @Override
    void markIdle();

    /**
     * 为指定的 Source Split 创建一个独立的 {@code SourceOutput}，用于处理 Split 级别的水位线管理等逻辑。
     *
     * <p>某些数据源（如 Kafka、文件流）可能需要针对不同的 Split（如 Kafka 分区或文件块）独立生成水位线，
     * 此方法允许为特定 Split 创建独立的输出。</p>
     *
     * <p>如果相同的 Split ID 已经创建过，则返回已有的实例，确保每个 Split 仅有一个对应的 {@code SourceOutput}。</p>
     *
     * <p><b>重要提醒：</b> 处理完某个 Split 后，必须调用 {@link #releaseOutputForSplit(String)}
     * 释放该 Split 的 {@code SourceOutput}，否则该 Split 仍会参与全局水位线计算，可能导致水位线滞后。</p>
     *
     * @param splitId 需要创建输出的 Split ID（例如 Kafka 分区 ID、文件块 ID）。
     * @return 该 Split 对应的 {@code SourceOutput} 实例。
     * @see #releaseOutputForSplit(String)
     */
    SourceOutput<T> createOutputForSplit(String splitId);

    /**
     * 释放指定 Split ID 关联的 {@code SourceOutput}。
     *
     * <p>当 Source 完成某个 Split 的数据处理后，必须调用此方法释放资源，否则该 Split 仍会影响水位线计算，
     * 可能导致 Flink 任务长时间停滞。</p>
     *
     * @param splitId 需要释放的 Split ID。
     * @see #createOutputForSplit(String)
     */
    void releaseOutputForSplit(String splitId);
}

