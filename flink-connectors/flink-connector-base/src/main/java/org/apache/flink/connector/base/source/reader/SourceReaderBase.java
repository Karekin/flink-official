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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * 这段代码是一个Java类的注释部分，用于解释这个类的功能和用途。下面是对这段注释的详细解释：
 *
 * 1. **类功能**：
 *    - 这个类是一个抽象类，实现了`SourceReader`接口。`SourceReader`通常用于读取数据源的数据。
 *    - 这个抽象类提供了一些同步机制，用于在邮件系统主线程和`SourceReader`内部线程之间进行同步。这有助于确保数据读取的线程安全。
 *    - 用户只需要提供`SplitReader`和快照分割状态（split state）即可，无需处理复杂的同步逻辑。
 *
 * 2. **内置指标**：
 *    - 这个实现提供了一些开箱即用的指标（metrics）：
 *      - `OperatorIOMetricGroup#getNumRecordsInCounter()`：这个指标用于统计进入操作符的记录数。
 *
 * 3. **泛型参数**：
 *    - `<E>`：表示包含分割状态更新信息或时间戳提取信息的丰富元素类型。
 *    - `<T>`：表示最终要发射的元新信息或时间戳提取信息的丰富元素类型素类型。
 *    - `<SplitT>`：表示不可变的分割类型。
 *    - `<SplitStateT>`：表示可变的分割状态类型。
 *
 * 4. **用途**：
 *    - 这个类主要用于数据源读取，特别是在需要处理分割状态和同步的场景中。
 *      例如，在分布式系统中，数据源可能被分割成多个部分，每个部分由不同的线程处理。
 *      这个类可以帮助管理这些分割状态，并确保线程安全。
 *
 * 5. **注意事项**：
 *    - 由于这是一个抽象类，用户需要继承这个类并实现具体的数据读取逻辑。
 *    - 用户需要提供具体的`SplitReader`实现，用于读取数据。
 *    - 用户还需要处理分割状态的快照，以便在需要时恢复数据读取的状态。
 *
 * 总的来说，这个类提供了一个框架，用于简化数据源读取的实现，特别是在需要处理多线程和分割状态的情况下。
 * 通过提供内置的同步机制和指标，它帮助开发者更轻松地实现复杂的数据读取逻辑。
 */
@PublicEvolving
public abstract class SourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
        implements SourceReader<T, SplitT> {
    private static final Logger LOG = LoggerFactory.getLogger(SourceReaderBase.class);

    /** A queue to buffer the elements fetched by the fetcher thread. */
    // 定义一个队列，用于缓存由获取线程获取的元素
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

    /** The state of the splits. */
    // 定义一个映射，用于存储split的状态
    private final Map<String, SplitContext<T, SplitStateT>> splitStates;

    /** The record emitter to handle the records read by the SplitReaders. */
    // 定义一个记录发射器，用于处理SplitReaders读取的记录
    protected final RecordEmitter<E, T, SplitStateT> recordEmitter;

    /** The split fetcher manager to run split fetchers. */
    // 定义一个split获取管理器，用于运行split获取器
    protected final SplitFetcherManager<E, SplitT> splitFetcherManager;

    /** The configuration for the reader. */
    // 定义读取器的配置
    protected final SourceReaderOptions options;

    /** The raw configurations that may be used by subclasses. */
    // 定义原始配置，可能被子类使用
    protected final Configuration config;

    // 定义一个计数器，用于记录输入的记录数
    private final Counter numRecordsInCounter;


    /** The context of this source reader. */
    // 定义此source reader的上下文
    protected SourceReaderContext context;

    /** The latest fetched batch of records-by-split from the split reader. */
    // 定义从split reader获取的最新一批按split划分的记录
    @Nullable private RecordsWithSplitIds<E> currentFetch;

    // 定义当前split的上下文
    @Nullable private SplitContext<T, SplitStateT> currentSplitContext;

    // 定义当前split的输出
    @Nullable private SourceOutput<T> currentSplitOutput;


    /** Indicating whether the SourceReader will be assigned more splits or not. */
    // 指示SourceReader是否将被分配更多的splits
    private boolean noMoreSplitsAssignment;


    // T是一个泛型类型，表示记录的类型
    // 该变量用于评估记录是否为EOF（文件结束符）记录
    @Nullable protected final RecordEvaluator<T> eofRecordEvaluator;

    /**
     * @deprecated Please use {@link #SourceReaderBase(SplitFetcherManager, RecordEmitter,
     *     Configuration, SourceReaderContext)} instead.
     */
    // 标记该构造函数为已弃用，不建议在新代码中使用
    @Deprecated
    public SourceReaderBase(
            // 用于存储待处理记录的阻塞队列，包含记录及其所属的split ID
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            // 负责管理split抓取任务的SplitFetcherManager实例
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            // 负责从split中读取记录并发射的RecordEmitter实例
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            // 配置对象，用于传递配置参数，此处为null，表示不使用额外的配置
            Configuration config,
            // SourceReader的上下文信息，包含运行时的环境和参数
            SourceReaderContext context) {
        // 调用另一个重载的构造函数，传入elementsQueue, splitFetcherManager, recordEmitter, null, config, context
        // 其中第四个参数为null，表示不传递额外的参数
        this(elementsQueue, splitFetcherManager, recordEmitter, null, config, context);
    }

    /**
     * @deprecated Please use {@link #SourceReaderBase(SplitFetcherManager, RecordEmitter,
     *     RecordEvaluator, Configuration, SourceReaderContext)} instead.
     */
    @Deprecated
    public SourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        this.elementsQueue = elementsQueue;
        this.splitFetcherManager = splitFetcherManager;
        this.recordEmitter = recordEmitter;
        this.splitStates = new HashMap<>();
        this.options = new SourceReaderOptions(config);
        this.config = config;
        this.context = context;
        this.noMoreSplitsAssignment = false;
        this.eofRecordEvaluator = eofRecordEvaluator;

        numRecordsInCounter = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
    }

    /**
     * The primary constructor for the source reader.
     *
     * <p>The reader will use a handover queue sized as configured via {@link
     * SourceReaderOptions#ELEMENT_QUEUE_CAPACITY}.
     */
    // 构造函数，用于初始化SourceReaderBase对象
    public SourceReaderBase(
            // SplitFetcherManager对象，用于管理和获取数据源的split信息
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            // RecordEmitter对象，用于从split中读取数据并发射记录
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            // Configuration对象，包含配置信息
            Configuration config,
            // SourceReaderContext对象，提供上下文信息，如当前任务的运行环境等
            SourceReaderContext context) {
        // 调用另一个构造函数，传入splitFetcherManager、recordEmitter、null、config和context
        this(splitFetcherManager, recordEmitter, null, config, context);
    }

    // 构造函数，用于初始化SourceReaderBase对象
    public SourceReaderBase(
            // 分割获取管理器，用于管理数据分割的获取
            SplitFetcherManager<E, SplitT> splitFetcherManager,
            // 记录发射器，用于处理和发射数据记录
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            // 记录评估器，用于评估EOF（文件结束）记录，可以为null
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            // 配置对象，包含各种配置参数
            Configuration config,
            // 源读取上下文，提供运行时的上下文信息
            SourceReaderContext context) {
        // 初始化元素队列，从分割获取管理器中获取队列
        this.elementsQueue = splitFetcherManager.getQueue();
        // 初始化分割获取管理器
        this.splitFetcherManager = splitFetcherManager;
        // 初始化记录发射器
        this.recordEmitter = recordEmitter;
        // 初始化分割状态映射，用于存储和管理分割状态
        this.splitStates = new HashMap<>();
        // 初始化源读取选项，基于传入的配置对象
        this.options = new SourceReaderOptions(config);
        // 保存配置对象
        this.config = config;
        // 保存源读取上下文
        this.context = context;
        // 初始化分割分配标志，表示是否还有更多的分割需要分配
        this.noMoreSplitsAssignment = false;
        // 初始化EOF记录评估器
        this.eofRecordEvaluator = eofRecordEvaluator;

        // 获取并初始化记录输入计数器，用于统计输入的记录数
        numRecordsInCounter = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
    }

    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        // 确保我们有一个正在处理的fetch，或者移动到下一个fetch
        RecordsWithSplitIds<E> recordsWithSplitId = this.currentFetch;
        if (recordsWithSplitId == null) {
            recordsWithSplitId = getNextFetch(output);
            if (recordsWithSplitId == null) {
                return trace(finishedOrAvailableLater());
            }
        }

        // 我们需要在这里循环，因为我们可能需要跨多个split
        while (true) {
            // 处理一条记录。
            final E record = recordsWithSplitId.nextRecordFromSplit();
            if (record != null) {
                // 发射这条记录。
                numRecordsInCounter.inc(1);
                recordEmitter.emitRecord(record, currentSplitOutput, currentSplitContext.state);
                LOG.trace("Emitted record: {}", record);

                // 我们总是在这里发射MORE_AVAILABLE，即使我们并不严格知道是否还有更多可用的记录。
                // 如果没有更多可用的记录，下一次调用将发现这一点并返回正确的状态。
                // 这意味着我们偶尔会发出'false positive'的可用性信号，但这可以节省我们对每条记录进行检查。
                // 最终，这是更经济的。
                return trace(InputStatus.MORE_AVAILABLE);
            } else if (!moveToNextSplit(recordsWithSplitId, output)) {
                // fetch已经完成，我们刚刚发现这一点，并且还没有发射任何记录。
                // 我们需要移动到下一个fetch。作为快捷方式，我们在这里再次调用pollNext()，
                // 而不是发射空值并等待调用者再次调用我们。
                return pollNext(output);
            }
        }
    }


    // 定义一个私有方法，用于跟踪输入状态
    private InputStatus trace(InputStatus status) {
        // 使用日志记录器记录输入状态的详细信息，方便调试和监控
        LOG.trace("Source reader status: {}", status);
        // 返回传入的输入状态，该方法的主要目的是记录状态，不改变状态
        return status;
    }

    @Nullable
    private RecordsWithSplitIds<E> getNextFetch(final ReaderOutput<T> output) {
        // 检查splitFetcherManager是否有错误
        splitFetcherManager.checkErrors();

        // 记录日志，表示正在从队列中获取下一个数据批次
        LOG.trace("Getting next source data batch from queue");
        // 从elementsQueue队列中取出下一个数据批次
        final RecordsWithSplitIds<E> recordsWithSplitId = elementsQueue.poll();
        // 如果取出的数据为空或者无法移动到下一个split，则返回null
        if (recordsWithSplitId == null || !moveToNextSplit(recordsWithSplitId, output)) {
            // No element available, set to available later if needed.
            return null;
        }

        currentFetch = recordsWithSplitId;
        return recordsWithSplitId;
    }

    // 定义一个私有方法，用于完成当前的获取操作
    private void finishCurrentFetch(
            final RecordsWithSplitIds<E> fetch, final ReaderOutput<T> output) {
        // 将当前获取的记录、当前分片上下文和当前分片输出置为null
        currentFetch = null;
        currentSplitContext = null;
        currentSplitOutput = null;

        // 获取已完成的分片ID集合
        final Set<String> finishedSplits = fetch.finishedSplits();
        // 如果有已完成的分片
        if (!finishedSplits.isEmpty()) {
            // 记录日志，表示已完成的分片
            LOG.info("Finished reading split(s) {}", finishedSplits);
            // 创建一个Map用于存储已完成分片的状态
            Map<String, SplitStateT> stateOfFinishedSplits = new HashMap<>();
            // 遍历已完成的分片ID集合
            for (String finishedSplitId : finishedSplits) {
                // 将已完成分片的状态从splitStates中移除并放入stateOfFinishedSplits
                stateOfFinishedSplits.put(
                        finishedSplitId, splitStates.remove(finishedSplitId).state);
                // 释放对应分片的输出资源
                output.releaseOutputForSplit(finishedSplitId);
            }
            // 调用onSplitFinished方法处理已完成分片的状态
            onSplitFinished(stateOfFinishedSplits);
        }

        // 回收fetch对象，释放资源
        fetch.recycle();
    }

    // 移动到下一个数据分片并处理其记录
    private boolean moveToNextSplit(
            RecordsWithSplitIds<E> recordsWithSplitIds, ReaderOutput<T> output) {
        // 获取下一个数据分片的ID
        final String nextSplitId = recordsWithSplitIds.nextSplit();
        // 如果没有下一个数据分片，表示当前获取已完成
        if (nextSplitId == null) {
            LOG.trace("Current fetch is finished."); // 记录日志，表示当前获取已完成
            finishCurrentFetch(recordsWithSplitIds, output); // 结束当前获取
            return false; // 返回false表示没有更多的数据分片
        }

        // 获取当前数据分片的上下文
        currentSplitContext = splitStates.get(nextSplitId);
        // 检查当前数据分片上下文是否为空，确保数据分片已被注册
        checkState(currentSplitContext != null, "Have records for a split that was not registered");

        // 定义EOF记录处理函数，用于判断是否到达数据流末尾
        Function<T, Boolean> eofRecordHandler = null;
        if (eofRecordEvaluator != null) {
            eofRecordHandler =
                    record -> {
                        // 如果记录不是数据流末尾，返回false
                        if (!eofRecordEvaluator.isEndOfStream(record)) {
                            return false;
                        }
                        // 如果是数据流末尾，将数据分片从获取管理器中移除
                        SplitT split =
                                toSplitType(currentSplitContext.splitId, currentSplitContext.state);
                        splitFetcherManager.removeSplits(Collections.singletonList(split));
                        return true; // 返回true表示到达数据流末尾
                    };
        }
        // 获取或创建当前数据分片的输出
        currentSplitOutput = currentSplitContext.getOrCreateSplitOutput(output, eofRecordHandler);
        LOG.trace("Emitting records from fetch for split {}", nextSplitId); // 记录日志，表示正在输出当前数据分片的记录
        return true; // 返回true表示有更多的数据分片
    }

    @Override
    // 重写父类或接口中的isAvailable方法
    public CompletableFuture<Void> isAvailable() {
        // 返回一个CompletableFuture<Void>类型的对象，表示异步操作的结果
        return currentFetch != null
                // 检查currentFetch是否不为null
                ? FutureCompletingBlockingQueue.AVAILABLE
                // 如果currentFetch不为null，返回一个表示可用的CompletableFuture对象
                : elementsQueue.getAvailabilityFuture();
                // 如果currentFetch为null，调用elementsQueue的getAvailabilityFuture方法获取其可用性Future
    }

    @Override
    // 重写父类或接口中的方法，表示该方法是一个重写的方法
    public List<SplitT> snapshotState(long checkpointId) {
        // 定义一个方法，用于在检查点（checkpoint）时快照当前的状态
        // 参数checkpointId表示当前的检查点ID
        List<SplitT> splits = new ArrayList<>();
        // 创建一个空的ArrayList，用于存储SplitT类型的对象
        splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
        // 遍历splitStates这个Map，其中id是键，context是值
        // 对于每一个键值对，调用toSplitType方法将id和context.state转换为SplitT类型，并添加到splits列表中
        return splits;
        // 返回包含所有SplitT对象的列表
    }

    @Override
    public void addSplits(List<SplitT> splits) {
        // 记录日志，输出正在添加的split(s)到reader的信息
        LOG.info("Adding split(s) to reader: {}", splits);
        // Initialize the state for each split.
        splits.forEach(
                s ->
                        splitStates.put(
                                s.splitId(), new SplitContext<>(s.splitId(), initializedState(s))));
        // Hand over the splits to the split fetcher to start fetch.
        splitFetcherManager.addSplits(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        // 记录日志信息，表示Reader接收到NoMoreSplits事件
        LOG.info("Reader received NoMoreSplits event.");
        // 设置标志位，表示没有更多的split可以分配
        noMoreSplitsAssignment = true;
        // 通知elementsQueue，表示有新的元素可用
        elementsQueue.notifyAvailable();
    }

    // 重写handleSourceEvents方法，用于处理未处理的源事件
    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        // 记录日志，输出接收到的未处理源事件信息
        LOG.info("Received unhandled source event: {}", sourceEvent);
    }

    // 重写pauseOrResumeSplits方法，用于暂停或恢复指定的split
    @Override
    public void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        // 调用splitFetcherManager的pauseOrResumeSplits方法，传入需要暂停和恢复的split集合
        splitFetcherManager.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    // 重写close方法，用于关闭源读取器
    @Override
    public void close() throws Exception {
        // 记录日志，输出关闭源读取器的信息
        LOG.info("Closing Source Reader.");
        // 调用splitFetcherManager的close方法，传入关闭超时时间
        splitFetcherManager.close(options.sourceReaderCloseTimeout);
    }

    /**
     * 获取当前分配给读取器的分片数量。
     *
     * <p>这些是通过 {@link #addSplits(List)} 添加的分片，并且尚未通过从 {@link SplitReader#fetch()} 返回
     * 作为 {@link RecordsWithSplitIds#finishedSplits()} 的一部分完成。
     */
    public int getNumberOfCurrentlyAssignedSplits() {
        return splitStates.size();
    }


// -------------------- 抽象方法，允许不同的实现 ------------------

    /** 处理完成的分片，根据需要清理状态。 */
    protected abstract void onSplitFinished(Map<String, SplitStateT> finishedSplitIds);

    /**
     * 当新的分片被添加到读取器时，初始化新分片的状态。
     *
     * @param split 新添加的分片。
     */
    protected abstract SplitStateT initializedState(SplitT split);

    /**
     * 将可变的 SplitStateT 转换为不可变的 SplitT。
     *
     * @param splitState 分片状态。
     * @return 不可变的分片状态。
     */
    protected abstract SplitT toSplitType(String splitId, SplitStateT splitState);


    // ------------------ private helper methods ---------------------

    // 定义一个私有方法，用于检查输入状态，判断是否完成或稍后可用
    private InputStatus finishedOrAvailableLater() {
        // 调用splitFetcherManager的maybeShutdownFinishedFetchers方法，尝试关闭已完成的fetcher
        // 该方法返回一个布尔值，表示是否所有fetcher都已关闭
        final boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
        // 检查是否没有更多的splits分配且所有fetcher都已关闭
        // 如果不满足这两个条件，则返回InputStatus.NOTHING_AVAILABLE，表示当前没有可用数据
        if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        // 检查elementsQueue是否为空
        if (elementsQueue.isEmpty()) {
            // We may reach here because of exceptional split fetcher, check it.
            splitFetcherManager.checkErrors();
            return InputStatus.END_OF_INPUT;
        } else {
            // We can reach this case if we just processed all data from the queue and finished a
            // split,
            // and concurrently the fetcher finished another split, whose data is then in the queue.
            return InputStatus.MORE_AVAILABLE;
        }
    }

    // ------------------ private helper classes ---------------------

    // 定义一个私有静态内部类SplitContext，用于管理分片上下文
    private static final class SplitContext<T, SplitStateT> {

        // 分片的唯一标识符
        final String splitId;
        // 分片的状态
        final SplitStateT state;
        // 可能为空的源输出，用于处理分片的输出
        @Nullable SourceOutput<T> sourceOutput;

        // 构造函数，初始化分片ID和分片状态
        private SplitContext(String splitId, SplitStateT state) {
            this.state = state;
            this.splitId = splitId;
        }

        // 获取或创建分片输出的方法
        SourceOutput<T> getOrCreateSplitOutput(
                ReaderOutput<T> mainOutput, @Nullable Function<T, Boolean> eofRecordHandler) {
            // 如果sourceOutput为空，则创建新的分片输出
            if (sourceOutput == null) {
                // The split output should have been created when AddSplitsEvent was processed in
                // SourceOperator. Here we just use this method to get the previously created
                // output.
                sourceOutput = mainOutput.createOutputForSplit(splitId);
                if (eofRecordHandler != null) {
                    sourceOutput = new SourceOutputWrapper<>(sourceOutput, eofRecordHandler);
                }
            }
            return sourceOutput;
        }
    }

    /** This output will stop sending records after receiving the eof record. */
    private static final class SourceOutputWrapper<T> implements SourceOutput<T> {
        final SourceOutput<T> sourceOutput;
        final Function<T, Boolean> eofRecordHandler;

        private boolean isStreamEnd = false;

        public SourceOutputWrapper(
                SourceOutput<T> sourceOutput, Function<T, Boolean> eofRecordHandler) {
            this.sourceOutput = sourceOutput;
            this.eofRecordHandler = eofRecordHandler;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            sourceOutput.emitWatermark(watermark);
        }

        @Override
        public void markIdle() {
            sourceOutput.markIdle();
        }

        @Override
        public void markActive() {
            sourceOutput.markActive();
        }

        @Override
        public void collect(T record) {
            if (!isEndOfStreamReached(record)) {
                sourceOutput.collect(record);
            }
        }

        @Override
        public void collect(T record, long timestamp) {
            if (!isEndOfStreamReached(record)) {
                sourceOutput.collect(record, timestamp);
            }
        }

        /**
         * Judge and handle the eof record.
         *
         * @return whether the record is the eof record.
         */
        private boolean isEndOfStreamReached(T record) {
            if (isStreamEnd) {
                return true;
            }
            if (eofRecordHandler.apply(record)) {
                isStreamEnd = true;
            }
            return isStreamEnd;
        }
    }
}
