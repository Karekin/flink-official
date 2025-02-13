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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 负责从外部系统中轮询消息（Records）的内部 fetcher 线程执行体（Runnable）。
 *
 * <p>它会从队列中提取任务（{@link SplitFetcherTask}）并执行对应的读取操作（Fetch），
 * 同时管理已分配的 Source Split（存放在 {@code assignedSplits} 中）。当全部拆分
 * 读取完成时，会触发回调通知。
 */
@PublicEvolving
public class SplitFetcher<E, SplitT extends SourceSplit> implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SplitFetcher.class);

    // 该 fetcher 在系统内的唯一标识（通常为序号）
    private final int id;

    // 当需要执行新的任务时，将任务推入此队列。由锁保护，线程安全。
    @GuardedBy("lock")
    private final Deque<SplitFetcherTask> taskQueue = new ArrayDeque<>();

    /**
     * 记录当前 fetcher 管理的所有已分配 splits，以便在没有 splits 时暂停读取器，
     * 避免空转。
     */
    private final Map<String, SplitT> assignedSplits = new HashMap<>();

    /**
     * 用于在 fetcher 与 reader 之间传递数据的队列。它会通知消费者当前是否可用数据，
     * 并支持在容量不足时阻塞，或在需要时唤醒。
     */
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

    // 实际读取逻辑由 SplitReader 完成
    private final SplitReader<E, SplitT> splitReader;

    // 出现不可恢复错误时调用的错误处理器
    private final Consumer<Throwable> errorHandler;

    // 在所有流程结束后执行的回调，如关闭资源、做清理等
    private final Runnable shutdownHook;

    /**
     * 标识该 fetcher 是否处于关闭状态或暂停状态等，由锁保护，保证线程可见性。
     */
    @GuardedBy("lock")
    private boolean closed;

    @GuardedBy("lock")
    private boolean paused;

    /**
     * 每次需要轮询数据时使用的任务。它会将从 {@link SplitReader} 中获取的
     * {@link RecordsWithSplitIds} 放入 {@code elementsQueue}。
     */
    private final FetchTask<E, SplitT> fetchTask;

    /**
     * 正在执行的任务（可被唤醒或中断）。执行完成后会被置空。
     */
    @GuardedBy("lock")
    @Nullable
    private SplitFetcherTask runningTask = null;

    // 用于并发安全地控制 fetcher 内部状态的重入锁
    private final ReentrantLock lock = new ReentrantLock();

    // 当队列为空时，等待该 Condition 被唤醒
    @GuardedBy("lock")
    private final Condition nonEmpty = lock.newCondition();

    // 当 fetcher 处于 paused 状态时，等待该 Condition 被唤醒
    @GuardedBy("lock")
    private final Condition resumed = lock.newCondition();

    // 标识是否允许未对齐的 Source Split（特定场景下的功能需求）
    private final boolean allowUnalignedSourceSplits;

    /**
     * 当某些 splits 完成读取后，需要调用此回调，告知系统（如上层协调器）
     * 已完成读取的 splits ID 集合。
     */
    private final Consumer<Collection<String>> splitFinishedHook;

    /**
     * SplitFetcher 的构造函数。
     *
     * @param id 当前 fetcher 的唯一标识
     * @param elementsQueue 用于放置读取到的数据的队列
     * @param splitReader 实际执行读取的对象
     * @param errorHandler 出现不可恢复错误时的错误处理回调
     * @param shutdownHook 在 fetcher 关闭时执行的收尾操作
     * @param splitFinishedHook 在某些 split 完成时的回调函数
     * @param allowUnalignedSourceSplits 是否允许未对齐的 Source Split
     */
    SplitFetcher(
            int id,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SplitReader<E, SplitT> splitReader,
            Consumer<Throwable> errorHandler,
            Runnable shutdownHook,
            Consumer<Collection<String>> splitFinishedHook,
            boolean allowUnalignedSourceSplits) {

        this.id = id;
        this.elementsQueue = checkNotNull(elementsQueue);
        this.splitReader = checkNotNull(splitReader);
        this.errorHandler = checkNotNull(errorHandler);
        this.shutdownHook = checkNotNull(shutdownHook);
        this.allowUnalignedSourceSplits = allowUnalignedSourceSplits;
        this.splitFinishedHook = splitFinishedHook;

        // 创建 fetchTask：读取数据，将已完成的 Splits 移除并触发回调
        this.fetchTask =
                new FetchTask<>(
                        splitReader,
                        elementsQueue,
                        ids -> {
                            // 当某些 splits 读取完成，从 assignedSplits 移除
                            ids.forEach(assignedSplits::remove);
                            // 调用外部传入的回调函数
                            splitFinishedHook.accept(ids);
                            LOG.info("Finished reading from splits {}", ids);
                        },
                        id);
    }

    /**
     * 线程主循环：不断执行 runOnce() 来处理任务队列和读取操作，直到出现错误或关闭。
     */
    @Override
    public void run() {
        LOG.info("Starting split fetcher {}", id);
        try {
            // 循环执行 runOnce()
            while (runOnce()) {
                // 此处无需额外操作，流程都在 runOnce() 内部
            }
        } catch (Throwable t) {
            // 捕获到任何异常后交给外部错误处理器
            errorHandler.accept(t);
        } finally {
            // 收尾阶段：关闭 reader，执行 shutdownHook
            try {
                splitReader.close();
            } catch (Exception e) {
                errorHandler.accept(e);
            } finally {
                LOG.info("Split fetcher {} exited.", id);
                shutdownHook.run();
            }
        }
    }

    /**
     * 运行一次任务处理逻辑。若发现已关闭，则返回 false 中断循环；否则执行下一个任务或等待。
     *
     * @return true 表示继续循环，false 表示退出
     */
    boolean runOnce() {
        // 1. 获取下一个待处理的任务（若无，则可能阻塞等待）
        SplitFetcherTask task;
        lock.lock();
        try {
            if (closed) {
                return false; // fetcher 已关闭，停止执行
            }

            task = getNextTaskUnsafe();
            if (task == null) {
                // 若没有新任务，可能是因为唤醒了但是队列为空，
                // 因此返回 true 再次循环检查
                return true;
            }

            LOG.debug("Prepare to run {}", task);
            // 记录当前正在执行的任务，以便唤醒、暂停等操作
            this.runningTask = task;
        } finally {
            lock.unlock();
        }

        // 2. 在锁外执行任务，防止长时间占用锁
        boolean taskFinished;
        try {
            taskFinished = task.run();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "SplitFetcher thread %d encountered an unexpected exception while polling records",
                            id),
                    e);
        }

        // 3. 再次加锁，处理任务执行结果
        lock.lock();
        try {
            this.runningTask = null;
            processTaskResultUnsafe(task, taskFinished);
        } finally {
            lock.unlock();
        }
        return true;
    }

    /**
     * 处理任务执行结果，比如如果任务已完成，则检查队列中是否还有剩余任务。
     */
    private void processTaskResultUnsafe(SplitFetcherTask task, boolean taskFinished) {
        assert lock.isHeldByCurrentThread();
        if (taskFinished) {
            LOG.debug("Finished running task {}", task);
            // 如果当前没有任何分配的 splits，且队列中也没有下一个任务，
            // 那么可以通知 elementsQueue，本 fetcher 暂时空闲
            if (assignedSplits.isEmpty() && taskQueue.isEmpty()) {
                elementsQueue.notifyAvailable();
            }
        } else if (task != fetchTask) {
            // 如果任务没有完成且不是 fetchTask，说明被唤醒中断了，需要重新加入队列
            taskQueue.addFirst(task);
            LOG.debug("Re-enqueuing woken task {}", task);
        }
    }

    /**
     * 从队列或 assignedSplits 中获取下一个可执行任务，若都没有则阻塞等待。
     *
     * @return 下一个待执行的任务或 null
     */
    @Nullable
    private SplitFetcherTask getNextTaskUnsafe() {
        assert lock.isHeldByCurrentThread();
        try {
            // 如果是暂停状态，需要等待 resumed 信号
            if (paused) {
                resumed.await();
                // 若恢复后发现已关闭或无任务，则返回 null
                return null;
            }
            // 若队列中有任务，则优先处理队列前端任务（FIFO）
            if (!taskQueue.isEmpty()) {
                return taskQueue.poll();
            } else if (!assignedSplits.isEmpty()) {
                // 如果还分配有 splits，但队列为空，则执行 fetchTask
                return fetchTask;
            } else {
                // 都没有时，阻塞等待 nonEmpty 信号
                nonEmpty.await();
                // 等待被唤醒后，返回队列头部任务
                return taskQueue.poll();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "The thread was interrupted while waiting for a fetcher task.");
        }
    }

    /**
     * Add splits to the split fetcher. This operation is asynchronous.
     *
     * <p>中文说明：异步地向当前 fetcher 添加新的 splits。方法内部会将对应的 AddSplitsTask 入队，
     * 并唤醒可能在等待任务的工作线程。
     *
     * @param splitsToAdd the splits to add.
     */
    public void addSplits(List<SplitT> splitsToAdd) {
        lock.lock();
        try {
            // 将添加 splits 的动作封装成一个 AddSplitsTask 并放入任务队列
            enqueueTaskUnsafe(new AddSplitsTask<>(splitReader, splitsToAdd, assignedSplits));
            // 唤醒 fetcher 线程，使其能够立刻处理该任务
            wakeUpUnsafe(true);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notice the split fetcher that some splits finished. This operation is asynchronous.
     *
     * <p>中文说明：异步地通知 fetcher 有部分 splits 已完成读取。这里会将 RemoveSplitsTask 放入队列，
     * 以便 fetcher 后续清除已完成的 splits。
     *
     * @param splitsToRemove the splits need to be removed.
     */
    public void removeSplits(List<SplitT> splitsToRemove) {
        lock.lock();
        try {
            enqueueTaskUnsafe(
                    new RemoveSplitsTask<>(
                            splitReader, splitsToRemove, assignedSplits, splitFinishedHook));
            wakeUpUnsafe(true);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Called when some splits of this source instance progressed too much beyond the global
     * watermark of all subtasks. If the split reader implements {@link SplitReader}, it will relay
     * the information asynchronously through the split fetcher thread.
     *
     * <p>中文说明：当某些 splits 的进度超出全局 watermark 较多时，可使用此方法通知 fetcher。
     * 若 SplitReader 支持相关接口，则会通过 PauseOrResumeSplitsTask 来执行对这些 splits 的暂停或恢复操作。
     *
     * @param splitsToPause the splits to pause
     * @param splitsToResume the splits to resume
     */
    public void pauseOrResumeSplits(
            Collection<SplitT> splitsToPause, Collection<SplitT> splitsToResume) {
        lock.lock();
        try {
            enqueueTaskUnsafe(
                    new PauseOrResumeSplitsTask<>(
                            splitReader,
                            splitsToPause,
                            splitsToResume,
                            allowUnalignedSourceSplits));
            wakeUpUnsafe(true);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enqueue a custom task to the fetcher. This method does not wake up the fetcher thread
     * automatically.
     *
     * <p>中文说明：将自定义的 SplitFetcherTask 入队，但不会主动唤醒 fetcher。
     * 适用于无需立刻执行或由外部调用者控制唤醒时机的场景。
     *
     * @param task 要添加的自定义任务
     */
    public void enqueueTask(SplitFetcherTask task) {
        lock.lock();
        try {
            enqueueTaskUnsafe(task);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将任务加入到任务队列（无额外唤醒操作），由 lock 保护调用。
     */
    private void enqueueTaskUnsafe(SplitFetcherTask task) {
        assert lock.isHeldByCurrentThread();
        taskQueue.add(task);
        // 通知可能正在等待非空队列的线程（见 getNextTaskUnsafe() 中的 nonEmpty.await()）
        nonEmpty.signal();
    }

    /**
     * 获取当前的 SplitReader。
     * <p>中文说明：通常由外部类（例如 SourceReaderBase）在构造或管理过程中获取该 fetcher 的 reader。
     */
    public SplitReader<E, SplitT> getSplitReader() {
        return splitReader;
    }

    /**
     * 返回当前 fetcher 的唯一 ID。
     * <p>中文说明：常用来识别不同的 fetcher 线程或区分并行度。
     */
    public int fetcherId() {
        return id;
    }

    /**
     * Shutdown the split fetcher.
     *
     * <p>中文说明：关闭当前 fetcher，更新状态（closed = true，paused = false），
     * 并唤醒可能等待中的线程，以便及时退出。
     */
    public void shutdown() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                paused = false;
                LOG.info("Shutting down split fetcher {}", id);
                // 传入 false 表示唤醒 fetcher 线程本身，以便让其停止执行
                wakeUpUnsafe(false);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Package private for unit test.
     *
     * <p>中文说明：获取当前已分配的所有 splits（仅供测试使用）。
     *
     * @return the assigned splits.
     */
    Map<String, SplitT> assignedSplits() {
        return assignedSplits;
    }

    /**
     * Package private for unit test.
     *
     * <p>中文说明：判断当前 fetcher 是否空闲（既无正在执行的任务，也没有已分配的 splits）。
     *
     * @return true if task queue is empty, false otherwise.
     */
    boolean isIdle() {
        lock.lock();
        try {
            return assignedSplits.isEmpty() && taskQueue.isEmpty() && runningTask == null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wake up the fetcher thread. There are only two blocking points in a running fetcher:
     * 1. Waiting for the next task in an idle fetcher.
     * 2. Running a task.
     *
     * <p>They need to be waken up differently. If the fetcher is blocking waiting on the next task
     * in the task queue, we should just notify that a task is available. If the fetcher is running
     * the user split reader, we should call SplitReader.wakeUp() instead.
     *
     * <p>The correctness can be thought of in the following way. The purpose of wake up is to let
     * the fetcher thread go to the very beginning of the running loop.
     *
     * <p>中文说明：唤醒 fetcher 线程，可能是在空闲等待任务（需要向任务队列发信号），
     * 或在执行用户逻辑（需要调用正在执行的 task.wakeUp() 来中断）。taskOnly 参数用于区分仅唤醒
     * 任务等待还是也要唤醒正在执行的逻辑。
     *
     * @param taskOnly if true, only wakes up the task queue part. Otherwise also tries to wake up
     *                 the task being processed.
     */
    void wakeUp(boolean taskOnly) {
        lock.lock();
        try {
            wakeUpUnsafe(taskOnly);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 内部唤醒逻辑。在锁保护下进行，避免并发冲突。根据当前是否有正在执行的任务（runningTask），
     * 决定调用 runningTask.wakeUp() 还是通知 nonEmpty/ resumed 条件队列。
     */
    private void wakeUpUnsafe(boolean taskOnly) {
        assert lock.isHeldByCurrentThread();

        SplitFetcherTask currentTask = runningTask;
        if (currentTask != null) {
            // 如果当前有在执行的任务，则调用其 wakeUp()
            LOG.debug("Waking up running task {}", currentTask);
            currentTask.wakeUp();
        } else if (!taskOnly) {
            // 若没有正在执行的任务，但 taskOnly = false，说明需要唤醒可能空闲等待的 fetcher
            LOG.debug("Waking up fetcher thread.");
            nonEmpty.signal();
            resumed.signal();
        }
    }

    /**
     * Temporarily pause the fetcher, preventing it from taking new tasks or fetch operations.
     * <p>中文说明：将 fetcher 标记为暂停状态，fetcher 在任务循环中会检测该标志，进入等待。
     */
    public void pause() {
        lock.lock();
        try {
            paused = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resumes the fetcher if it was paused, allowing it to continue task processing.
     * <p>中文说明：解除 fetcher 的暂停状态，并唤醒其等待的线程。
     */
    public void resume() {
        lock.lock();
        try {
            paused = false;
            resumed.signal();
        } finally {
            lock.unlock();
        }
    }
}
