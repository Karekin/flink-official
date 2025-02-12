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
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Source 接口用于定义一个数据源的主要行为，包括：
 * 1) 如何创建并还原 SplitEnumerator（分片枚举器）；
 * 2) 如何创建并还原 SourceReader（分片读取器）；
 * 3) 针对分片（Split）与枚举器状态（Enumerator Checkpoint）的序列化/反序列化方式。
 *
 * <p>该接口继承了 {@link SourceReaderFactory}，通常在流式或批式场景下，Flink 会先从此 Source 中
 * 获取对应的 SplitEnumerator，再由 enumerator 将分片信息分配给 SourceReader 并协调读取过程。</p>
 *
 * @param <T>       通过此 Source 产出的记录类型。
 * @param <SplitT>  此 Source 处理的分片类型，必须实现 {@link SourceSplit}。
 * @param <EnumChkT>在分片枚举器进行快照（checkpoint）时使用的状态类型。
 */
@Public
public interface Source<T, SplitT extends SourceSplit, EnumChkT>
        extends SourceReaderFactory<T, SplitT> {

    /**
     * 获取此 Source 的有界性（Boundedness）。
     * <p>如果返回 {@link Boundedness#BOUNDED}，表示源数据量有限（批处理模式）；
     * 如果返回 {@link Boundedness#CONTINUOUS_UNBOUNDED}，表示源是无界流（实时流模式）。</p>
     *
     * @return 有界性枚举，指示此 Source 是有限/无限数据源。
     */
    Boundedness getBoundedness();

    /**
     * 创建一个新的 SplitEnumerator，用于开始一个新的输入数据读取流程。
     * <p>分片枚举器的主要职责是：
     * 1) 根据外部系统/文件的元数据，将数据拆分为若干分片（SplitT）；
     * 2) 将分片分配给各个并行任务的 SourceReader；
     * 3) 在流式场景下实时监控外部源的变化（如新分片），并在运行时不断分配新的分片。</p>
     *
     * @param enumContext SplitEnumeratorContext 提供了当前分配器需要的上下文信息，如并行度、任务通信等。
     * @return 一个新的 SplitEnumerator 实例。
     * @throws Exception 当方法内部出现异常（如无法连接源端、读取元数据信息失败等），可直接抛出。
     *                   若抛出异常会导致 JobManager 进行 failover 或重启流程。
     */
    SplitEnumerator<SplitT, EnumChkT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext)
            throws Exception;

    /**
     * 从已有的checkpoint状态中恢复分片枚举器。
     * <p>当 Flink 执行 checkpoint/restart 流程时，会将枚举器的状态进行快照并在恢复时传给此方法，
     * 从而保证断点续跑时分片分配的一致性和正确性。</p>
     *
     * @param enumContext SplitEnumeratorContext 提供了恢复后分配器需要的上下文环境。
     * @param checkpoint  上一次 checkpoint 时保存的枚举器状态。
     * @return 根据给定 checkpoint 恢复出的 SplitEnumerator。
     * @throws Exception 当恢复过程中出现异常，可以抛出并触发 JobManager 的故障恢复。
     */
    SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception;

    // ------------------------------------------------------------------------
    //  serializers for the metadata
    // ------------------------------------------------------------------------

    /**
     * 创建序列化器以序列化/反序列化分片（SplitT）。
     * <p>分片信息在以下场景需要被序列化：
     * 1) SplitEnumerator 向 SourceReader 分发分片时，需要将分片通过网络传输给各个 Task；
     * 2) SourceReader 在做 checkpoint 时，需要将当前正在处理的分片信息序列化到状态后端。</p>
     *
     * @return SplitT 对应的序列化器 {@link SimpleVersionedSerializer}。
     */
    SimpleVersionedSerializer<SplitT> getSplitSerializer();

    /**
     * 创建枚举器 checkpoint 的序列化器。该序列化器会用于序列化 SplitEnumerator 的状态。
     * <p>当执行 {@link SplitEnumerator#snapshotState(long)} 时，返回的状态会用此序列化器进行持久化，
     * 在作业恢复时通过 {@link #restoreEnumerator(SplitEnumeratorContext, Object)} 来反序列化并构造
     * 对应的枚举器。</p>
     *
     * @return 用于序列化 SplitEnumerator checkpoint 的 {@link SimpleVersionedSerializer}。
     */
    SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer();
}

