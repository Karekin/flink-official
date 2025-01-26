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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 一个具有单个抓取线程（I/O线程）的抓取器管理器，该线程能够并发处理所有分片。
 *
 * <p>这种模式适用于例如文件读取器、Apache Kafka 读取器等连接器。
 * 对于 Kafka 示例，单个线程使用同一个客户端读取所有分片（主题分区）。
 * 对于文件源示例，单个线程依次读取文件。
 */
@PublicEvolving
public class SingleThreadFetcherManager<E, SplitT extends SourceSplit>
        extends SplitFetcherManager<E, SplitT> {

    /**
     * 创建一个具有单个 I/O 线程的分片抓取器管理器。
     *
     * @param elementsQueue 用于从 I/O 线程（抓取器）向读取器传递数据的队列，
     *                      读取器负责发出记录和维护状态。此队列实例必须与传递给
     *                      {@link SourceReaderBase} 的实例相同。
     * @param splitReaderSupplier 用于连接到源系统的分片读取器工厂。
     * @deprecated 请使用 {@link #SingleThreadFetcherManager(Supplier, Configuration)} 替代。
     */
    @Deprecated
    public SingleThreadFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier, new Configuration());
    }

    /**
     * 创建一个具有单个 I/O 线程的分片抓取器管理器。
     *
     * @param elementsQueue 用于从 I/O 线程（抓取器）向读取器传递数据的队列。
     * @param splitReaderSupplier 用于连接到源系统的分片读取器工厂。
     * @param configuration 创建抓取器管理器的配置。
     * @deprecated 请使用 {@link #SingleThreadFetcherManager(Supplier, Configuration)} 替代。
     */
    @Deprecated
    public SingleThreadFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            Configuration configuration) {
        super(elementsQueue, splitReaderSupplier, configuration);
    }

    /**
     * 创建一个具有单个 I/O 线程的分片抓取器管理器。
     *
     * @param elementsQueue 用于从 I/O 线程（抓取器）向读取器传递数据的队列。
     * @param splitReaderSupplier 用于连接到源系统的分片读取器工厂。
     * @param configuration 创建抓取器管理器的配置。
     * @param splitFinishedHook 用于处理分片完成的钩子。
     * @deprecated 请使用 {@link #SingleThreadFetcherManager(Supplier, Configuration, Consumer)} 替代。
     */
    @VisibleForTesting
    @Deprecated
    public SingleThreadFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, configuration, splitFinishedHook);
    }

    /**
     * 创建一个具有单个 I/O 线程的分片抓取器管理器。
     *
     * @param splitReaderSupplier 用于连接到源系统的分片读取器工厂。
     */
    public SingleThreadFetcherManager(Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        super(splitReaderSupplier, new Configuration());
    }

    /**
     * 创建一个具有单个 I/O 线程的分片抓取器管理器。
     *
     * @param splitReaderSupplier 用于连接到源系统的分片读取器工厂。
     * @param configuration 创建抓取器管理器的配置。
     */
    public SingleThreadFetcherManager(
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier, Configuration configuration) {
        super(splitReaderSupplier, configuration);
    }

    /**
     * 创建一个具有单个 I/O 线程的分片抓取器管理器。
     *
     * @param splitReaderSupplier 用于连接到源系统的分片读取器工厂。
     * @param configuration 创建抓取器管理器的配置。
     * @param splitFinishedHook 用于处理分片完成的钩子。
     */
    public SingleThreadFetcherManager(
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        super(splitReaderSupplier, configuration, splitFinishedHook);
    }

    /**
     * 添加分片到抓取器。
     *
     * @param splitsToAdd 要添加的分片列表。
     */
    @Override
    public void addSplits(List<SplitT> splitsToAdd) {
        // 获取当前运行中的分片抓取器
        SplitFetcher<E, SplitT> fetcher = getRunningFetcher();
        if (fetcher == null) {
            // 如果没有运行中的抓取器，创建一个新的抓取器
            fetcher = createSplitFetcher();
            // 将分片分配给抓取器
            fetcher.addSplits(splitsToAdd);
            // 启动抓取器
            startFetcher(fetcher);
        } else {
            // 如果有运行中的抓取器，直接添加分片
            fetcher.addSplits(splitsToAdd);
        }
    }

    /**
     * 从抓取器中移除指定的分片。
     *
     * @param splitsToRemove 要移除的分片列表。
     */
    @Override
    public void removeSplits(List<SplitT> splitsToRemove) {
        // 获取当前运行中的分片抓取器
        SplitFetcher<E, SplitT> fetcher = getRunningFetcher();
        if (fetcher != null) {
            // 如果存在运行中的抓取器，移除分片
            fetcher.removeSplits(splitsToRemove);
        }
    }

    /**
     * 获取当前运行中的分片抓取器。
     *
     * @return 运行中的分片抓取器，如果没有，则返回 null。
     */
    protected SplitFetcher<E, SplitT> getRunningFetcher() {
        // 如果抓取器映射为空，返回 null，否则返回第一个抓取器
        return fetchers.isEmpty() ? null : fetchers.values().iterator().next();
    }
}

