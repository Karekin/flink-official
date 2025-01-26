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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.configuration.PipelineOptions.ALLOW_UNALIGNED_SOURCE_SPLITS;

/**
 * 负责启动 {@link SplitFetcher} 并管理其生命周期的类。
 * 此类与 {@link SourceReaderBase} 配合使用。
 *
 * <p>通过不同实现 {@link #addSplits(List)} 方法，分片抓取器管理器可以支持不同的线程模型。
 * 例如：单线程分片抓取器管理器只会启动一个抓取器并将所有分片分配给它；
 * 而每个分片一个线程的抓取器会在每次分配新分片时创建一个新线程。
 */
@PublicEvolving
public abstract class SplitFetcherManager<E, SplitT extends SourceSplit> {
    // 日志记录器，用于记录相关的日志信息
    private static final Logger LOG = LoggerFactory.getLogger(SplitFetcherManager.class);

    // 用于处理错误的消费者（函数式接口），接收Throwable对象
    private final Consumer<Throwable> errorHandler;

    /** 原子整数，用于生成递增的抓取器ID。 */
    private final AtomicInteger fetcherIdGenerator;

    /** 提供分片读取器的供应器（工厂模式）。 */
    private final Supplier<SplitReader<E, SplitT>> splitReaderFactory;

    /** 保存分片抓取器中未捕获的异常。 */
    private final AtomicReference<Throwable> uncaughtFetcherException;

    /** 分片抓取器将数据元素放入的队列。 */
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

    /** 一个映射，用于跟踪所有分片抓取器的状态，键为抓取器ID，值为对应的抓取器对象。 */
    protected final Map<Integer, SplitFetcher<E, SplitT>> fetchers;

    /**
     * 执行服务，拥有两个线程：
     * 一个用于分片抓取器，另一个用于完成Future的线程。
     */
    private final ExecutorService executors;

    /** 标志分片抓取器管理器是否已关闭。 */
    private volatile boolean closed;

    /**
     * 分片完成时的处理钩子（函数式接口），通常用于测试 {@link SplitFetcher} 和 {@link SplitReader}
     * 的分片完成行为。接收一个分片ID集合作为输入参数。
     */
    private final Consumer<Collection<String>> splitFinishedHook;

    /** 是否允许非对齐的源分片。 */
    private final boolean allowUnalignedSourceSplits;

    /**
     * 创建一个分片抓取器管理器。
     *
     * @param elementsQueue 分片读取器将数据元素放入的队列。
     * @param splitReaderFactory 一个用于创建分片读取器的供应器。
     * @param configuration 此抓取器管理器的配置。
     * @deprecated 请使用 {@link #SplitFetcherManager(Supplier, Configuration)} 替代。
     */
    @Deprecated
    public SplitFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderFactory,
            Configuration configuration) {
        // 调用另一个构造函数，并传递一个空的完成分片处理钩子。
        this(elementsQueue, splitReaderFactory, configuration, (ignore) -> {});
    }

    /**
     * 创建一个分片抓取器管理器。
     *
     * @param elementsQueue 分片读取器将数据元素放入的队列。
     * @param splitReaderFactory 一个用于创建分片读取器的供应器。
     * @param configuration 此抓取器管理器的配置。
     * @param splitFinishedHook 用于处理分片完成的钩子函数。
     * @deprecated 请使用 {@link #SplitFetcherManager(Supplier, Configuration, Consumer)} 替代。
     */
    @Deprecated
    @VisibleForTesting
    public SplitFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderFactory,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        // 初始化分片读取器数据队列
        this.elementsQueue = elementsQueue;

        // 初始化错误处理器，处理抓取器中未捕获的异常
        this.errorHandler = new Consumer<Throwable>() {
            @Override
            public void accept(Throwable t) {
                // 记录异常日志
                LOG.error("收到未捕获的异常。", t);
                // 如果当前未记录任何异常，则记录此异常；否则将异常添加到原有异常的 suppressed 列表中
                if (!uncaughtFetcherException.compareAndSet(null, t)) {
                    uncaughtFetcherException.get().addSuppressed(t);
                }
                // 通知主线程异常发生，唤醒队列以触发后续处理
                elementsQueue.notifyAvailable();
            }
        };

        // 初始化分片读取器的工厂供应器
        this.splitReaderFactory = splitReaderFactory;

        // 设置分片完成的处理钩子
        this.splitFinishedHook = splitFinishedHook;

        // 初始化未捕获的异常引用
        this.uncaughtFetcherException = new AtomicReference<>(null);

        // 初始化分片抓取器ID生成器
        this.fetcherIdGenerator = new AtomicInteger(0);

        // 初始化抓取器映射，用于跟踪所有的分片抓取器
        this.fetchers = new ConcurrentHashMap<>();

        // 从配置中读取是否允许非对齐的源分片
        this.allowUnalignedSourceSplits = configuration.get(ALLOW_UNALIGNED_SOURCE_SPLITS);

        // 创建执行器服务，使用自定义线程工厂以确保当抓取线程异常退出时通知源读取器
        final String taskThreadName = Thread.currentThread().getName();
        this.executors = Executors.newCachedThreadPool(r -> new Thread(r, "Source Data Fetcher for " + taskThreadName));

        // 初始化关闭标志为 false
        this.closed = false;
    }


    /**
     * 创建一个分片抓取器管理器。
     *
     * @param splitReaderFactory 用于创建分片读取器的供应器（工厂方法）。
     * @param configuration 此分片抓取器管理器的配置。
     */
    public SplitFetcherManager(
            Supplier<SplitReader<E, SplitT>> splitReaderFactory, Configuration configuration) {
        // 调用另一个构造函数并传入空的分片完成钩子函数
        this(splitReaderFactory, configuration, (ignore) -> {});
    }

    /**
     * 创建一个分片抓取器管理器。
     *
     * @param splitReaderFactory 用于创建分片读取器的供应器。
     * @param configuration 此分片抓取器管理器的配置。
     * @param splitFinishedHook 分片完成时的钩子函数，用于处理完成的分片。
     */
    public SplitFetcherManager(
            Supplier<SplitReader<E, SplitT>> splitReaderFactory,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        // 初始化元素队列，用于存储分片读取器产生的元素，队列容量从配置中读取
        this.elementsQueue =
                new FutureCompletingBlockingQueue<>(
                        configuration.get(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY));

        // 初始化错误处理器，处理分片抓取器未捕获的异常
        this.errorHandler = new Consumer<Throwable>() {
            @Override
            public void accept(Throwable t) {
                LOG.error("收到未捕获的异常。", t);
                if (!uncaughtFetcherException.compareAndSet(null, t)) {
                    uncaughtFetcherException.get().addSuppressed(t);
                }
                // 唤醒主线程，通知异常发生
                elementsQueue.notifyAvailable();
            }
        };

        // 初始化分片读取器工厂和分片完成钩子
        this.splitReaderFactory = splitReaderFactory;
        this.splitFinishedHook = splitFinishedHook;

        // 初始化未捕获异常引用
        this.uncaughtFetcherException = new AtomicReference<>(null);

        // 初始化抓取器ID生成器，从0开始递增
        this.fetcherIdGenerator = new AtomicInteger(0);

        // 初始化分片抓取器映射，用于管理所有的抓取器实例
        this.fetchers = new ConcurrentHashMap<>();

        // 从配置中读取是否允许非对齐的源分片
        this.allowUnalignedSourceSplits = configuration.get(ALLOW_UNALIGNED_SOURCE_SPLITS);

        // 创建线程池，确保抓取器线程异常退出时可以通知源读取器
        final String taskThreadName = Thread.currentThread().getName();
        this.executors =
                Executors.newCachedThreadPool(
                        r -> new Thread(r, "Source Data Fetcher for " + taskThreadName));

        // 初始化关闭标志为 false
        this.closed = false;
    }

    /**
     * 添加分片的抽象方法，需由具体子类实现。
     *
     * @param splitsToAdd 要添加的分片列表。
     */
    public abstract void addSplits(List<SplitT> splitsToAdd);

    /**
     * 移除分片的抽象方法，需由具体子类实现。
     *
     * @param splitsToRemove 要移除的分片列表。
     */
    public abstract void removeSplits(List<SplitT> splitsToRemove);

    /**
     * 暂停或恢复指定的分片。
     *
     * @param splitIdsToPause 要暂停的分片ID集合。
     * @param splitIdsToResume 要恢复的分片ID集合。
     */
    public void pauseOrResumeSplits(
            Collection<String> splitIdsToPause, Collection<String> splitIdsToResume) {
        for (SplitFetcher<E, SplitT> fetcher : fetchers.values()) {
            // 获取当前抓取器中分片的分配映射
            Map<String, SplitT> idToSplit = fetcher.assignedSplits();

            // 查找需要暂停和恢复的分片
            List<SplitT> splitsToPause = lookupInAssignment(splitIdsToPause, idToSplit);
            List<SplitT> splitsToResume = lookupInAssignment(splitIdsToResume, idToSplit);

            // 如果存在需要暂停或恢复的分片，则调用抓取器的暂停或恢复方法
            if (!splitsToPause.isEmpty() || !splitsToResume.isEmpty()) {
                fetcher.pauseOrResumeSplits(splitsToPause, splitsToResume);
            }
        }
    }

    /**
     * 根据分片ID集合从分配映射中查找对应的分片。
     *
     * @param splitIds 要查找的分片ID集合。
     * @param assignment 分片的分配映射。
     * @return 找到的分片列表。
     */
    private List<SplitT> lookupInAssignment(
            Collection<String> splitIds, Map<String, SplitT> assignment) {
        List<SplitT> splits = new ArrayList<>();
        for (String s : splitIds) {
            SplitT split = assignment.get(s);
            if (split != null) {
                splits.add(split);
            }
        }
        return splits;
    }

    /**
     * 启动分片抓取器，将其提交到执行器中运行。
     *
     * @param fetcher 要启动的分片抓取器。
     */
    protected void startFetcher(SplitFetcher<E, SplitT> fetcher) {
        executors.submit(fetcher);
    }

    /**
     * 同步方法，确保在抓取器管理器关闭后不再创建抓取器。
     *
     * @return 创建的分片抓取器。
     * @throws IllegalStateException 如果抓取器管理器已关闭。
     */
    protected synchronized SplitFetcher<E, SplitT> createSplitFetcher() {
        if (closed) {
            throw new IllegalStateException("分片抓取器管理器已关闭。");
        }

        // 使用供应器创建分片读取器
        SplitReader<E, SplitT> splitReader = splitReaderFactory.get();

        // 生成抓取器ID
        int fetcherId = fetcherIdGenerator.getAndIncrement();

        // 创建新的分片抓取器
        SplitFetcher<E, SplitT> splitFetcher =
                new SplitFetcher<>(
                        fetcherId,
                        elementsQueue,
                        splitReader,
                        errorHandler,
                        () -> {
                            fetchers.remove(fetcherId);
                            // 同步抓取器的状态，通知队列分片状态的变化
                            elementsQueue.notifyAvailable();
                        },
                        this.splitFinishedHook,
                        allowUnalignedSourceSplits);

        // 将抓取器添加到映射中
        fetchers.put(fetcherId, splitFetcher);

        return splitFetcher;
    }


    /**
     * 检查并关闭已完成工作的分片抓取器。
     *
     * @return 如果所有抓取器都已完成工作，返回 true；否则返回 false。
     */
    public boolean maybeShutdownFinishedFetchers() {
        // 遍历当前所有分片抓取器
        Iterator<Map.Entry<Integer, SplitFetcher<E, SplitT>>> iter = fetchers.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, SplitFetcher<E, SplitT>> entry = iter.next();
            SplitFetcher<E, SplitT> fetcher = entry.getValue();

            // 如果抓取器处于空闲状态（已完成工作）
            if (fetcher.isIdle()) {
                LOG.info("关闭分片抓取器 {}，因为它处于空闲状态。", entry.getKey());
                fetcher.shutdown(); // 关闭抓取器
                iter.remove(); // 从映射中移除
            }
        }

        // 如果所有抓取器都已被移除，则返回 true；否则返回 false
        return fetchers.isEmpty();
    }

    /**
     * 返回包含分片抓取器产生数据的队列。
     * 此方法为内部方法，仅供 {@link SourceReaderBase} 使用。
     *
     * @return 包含数据的队列。
     */
    @Internal
    public FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> getQueue() {
        return elementsQueue;
    }

    /**
     * 关闭分片抓取器管理器。
     *
     * @param timeoutMs 等待关闭的最大时间（毫秒）。
     * @throws Exception 如果关闭分片抓取器管理器失败。
     */
    public synchronized void close(long timeoutMs) throws Exception {
        // 标记抓取器管理器为已关闭
        closed = true;

        // 关闭所有分片抓取器
        fetchers.values().forEach(SplitFetcher::shutdown);

        // 关闭执行器服务
        executors.shutdown();

        // 等待执行器中的所有任务在指定时间内完成
        if (!executors.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
            LOG.warn(
                    "在 {} 毫秒内未能关闭源读取器。仍有 {} 个分片抓取器正在运行。",
                    timeoutMs,
                    fetchers.size());
        }
    }

    /**
     * 检查分片抓取器中的异常。
     * 如果有未捕获的异常，则抛出一个运行时异常。
     */
    public void checkErrors() {
        if (uncaughtFetcherException.get() != null) {
            throw new RuntimeException(
                    "一个或多个抓取器遇到了异常。",
                    uncaughtFetcherException.get());
        }
    }

    /**
     * 返回当前存活的分片抓取器数量。
     * 此方法仅用于测试目的。
     *
     * @return 当前存活的分片抓取器数量。
     */
    @VisibleForTesting
    public int getNumAliveFetchers() {
        return fetchers.size();
    }

}
