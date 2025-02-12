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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * SourceReaderContext 是一个接口，提供 Flink 运行时环境的一些上下文信息给 {@link SourceReader}。
 *
 * <p>主要作用：
 * <ul>
 *     <li>1. 获取 Flink 任务的配置信息（Configuration）。</li>
 *     <li>2. 提供当前任务的主机名，用于本地性优化（Locality Optimization）。</li>
 *     <li>3. 提供当前任务的 Subtask Index，帮助任务进行并行计算。</li>
 *     <li>4. 允许 SourceReader 发送 Split 请求给 SplitEnumerator。</li>
 *     <li>5. 允许 SourceReader 发送自定义事件给 SourceCoordinator。</li>
 *     <li>6. 提供自定义类加载器，用于加载用户自定义的 JAR 文件。</li>
 *     <li>7. 获取当前 Source 的并行度（Parallelism）。</li>
 * </ul>
 */
@Public
public interface SourceReaderContext {

    /**
     * 获取当前 Source 相关的度量指标组（Metric Group）。
     *
     * <p>Flink 允许 Source 组件暴露一些内部状态和统计数据，例如：
     * <ul>
     *     <li>读取的数据量</li>
     *     <li>数据吞吐量</li>
     *     <li>处理时间</li>
     * </ul>
     *
     * @return Source 组件的指标信息，用于 Flink 任务监控。
     */
    SourceReaderMetricGroup metricGroup();

    /**
     * 获取 Flink 任务的配置信息。
     *
     * <p>Flink 任务启动时，会加载用户提供的 Flink 配置，该方法可以用于获取这些配置信息，
     * 例如：
     * <ul>
     *     <li>任务并行度</li>
     *     <li>Flink Checkpoint 配置</li>
     *     <li>Source 组件的自定义参数</li>
     * </ul>
     *
     * @return Flink 任务的配置信息。
     */
    Configuration getConfiguration();

    /**
     * 获取当前任务运行的主机名。
     *
     * <p>该方法可用于本地性优化（Locality Optimization），即：
     * <ul>
     *     <li>如果数据源存储在多个节点上，可以优先从本地节点读取数据，提高性能。</li>
     *     <li>减少网络传输开销，降低数据读取延迟。</li>
     * </ul>
     *
     * @return 当前任务所在的主机名。
     */
    String getLocalHostName();

    /**
     * 获取当前任务的 Subtask Index。
     *
     * <p>Flink 任务通常是并行执行的，每个任务实例都有一个唯一的编号（从 0 开始）。
     * 该方法返回当前任务的编号，常用于：
     * <ul>
     *     <li>分区数据的分配，例如 Kafka 分区到不同的 SourceReader。</li>
     *     <li>日志跟踪，区分不同任务实例的日志输出。</li>
     * </ul>
     *
     * @return 当前任务的 Subtask Index。
     */
    int getIndexOfSubtask();

    /**
     * 向 SplitEnumerator 发送 Split 请求。
     *
     * <p>当 SourceReader 需要更多数据时，可以调用该方法请求新的 Split，
     * 这会触发 {@link SplitEnumerator#handleSplitRequest(int, String)} 方法，
     * 并传递当前任务的 Subtask ID 及主机名。
     *
     * <p>这个机制适用于：
     * <ul>
     *     <li>动态数据分配，比如 Kafka 动态获取新的 Topic 分区。</li>
     *     <li>任务扩容时，新任务请求未分配的 Split。</li>
     * </ul>
     */
    void sendSplitRequest();

    /**
     * 发送自定义 Source 事件到 SourceCoordinator。
     *
     * <p>该方法提供了一种通信机制，允许 SourceReader 向 SourceCoordinator 发送事件，
     * 例如：
     * <ul>
     *     <li>Source 组件的健康状态上报。</li>
     *     <li>通知 SourceCoordinator 需要更新数据分片。</li>
     *     <li>自定义协议，用于控制 Source 组件的行为。</li>
     * </ul>
     *
     * @param sourceEvent 需要发送的自定义事件。
     */
    void sendSourceEventToCoordinator(SourceEvent sourceEvent);

    /**
     * 获取自定义类加载器，用于加载用户提供的 JAR 文件。
     *
     * <p>Flink 允许用户提交带有自定义依赖的 JAR 文件，该方法可以用于加载这些类。
     * 例如：
     * <ul>
     *     <li>加载用户自定义的序列化类。</li>
     *     <li>加载自定义数据解析器（如 JSON、Avro 解析器）。</li>
     * </ul>
     *
     * @return 用户自定义的类加载器 {@link UserCodeClassLoader}。
     */
    UserCodeClassLoader getUserCodeClassLoader();

    /**
     * 获取当前 Source 任务的并行度。
     *
     * <p>Flink 允许用户在运行时动态调整 Source 的并行度，该方法返回当前 Source 的并行度。
     *
     * <p>注意：该方法的默认实现会抛出 {@link UnsupportedOperationException}，
     * 需要 SourceReaderContext 的实现类进行覆盖。
     *
     * @return 当前 Source 任务的并行度。
     */
    default int currentParallelism() {
        throw new UnsupportedOperationException();
    }
}

