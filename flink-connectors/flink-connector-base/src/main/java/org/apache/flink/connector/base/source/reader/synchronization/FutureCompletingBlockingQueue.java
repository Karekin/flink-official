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

package org.apache.flink.connector.base.source.reader.synchronization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.concurrent.GuardedBy;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 这是一个结合了阻塞队列和 {@link CompletableFuture} 的自定义实现，用于在生产者线程和消费者线程之间
 * 传递数据。该类提供以下主要特性：
 *
 * <h3>Consumer Notifications（消费者通知）</h3>
 *
 * <p>与传统的 {@link #take()}（阻塞等待） 或 {@link #poll()}（轮询） 不同，本类通过
 * {@link #getAvailabilityFuture()} 提供一个可用性 CompletableFuture，队列非空时自动完成。
 * 消费者可利用异步回调来获取通知，还可通过 {@link #notifyAvailable()} 人工触发通知。
 *
 * <p>当 {@link #poll()} 或 {@link #take()} 使队列变空时，会重置可用性；此外，
 * “虚假唤醒”也是正常且可接受的，消费者应在循环中调用 {@code poll()} 重新获取新 Future。
 *
 * <h3>Producer Wakeup（生产者唤醒）</h3>
 *
 * <p>当队列满了导致生产者阻塞时，可通过 {@link #wakeUpPuttingThread(int)} 优雅地唤醒生产者线程，
 * 而无需调用中断机制。
 *
 * @param <T> the type of the elements in the queue.
 */
@Internal
public class FutureCompletingBlockingQueue<T> {

    /**
     * 一个表示队列“可用”状态的常量 Future。若队列已确定非空，可直接返回它以避免额外同步操作。
     */
    public static final CompletableFuture<Void> AVAILABLE = getAvailableFuture();

    // ------------------------------------------------------------------------

    /** 队列的最大容量。 */
    private final int capacity;

    /**
     * 表示当前可用性状态的 Future，与队列的“非空”条件耦合。
     * 当队列变为空时，该 Future 需被重置；当队列有数据时，则完成该 Future。
     */
    private CompletableFuture<Void> currentFuture;

    /** 用于多线程同步的可重入锁。 */
    private final Lock lock;

    /**
     * 实际存放元素的队列。所有对该队列的操作都需要先获取 lock。
     */
    @GuardedBy("lock")
    private final Queue<T> queue;

    /**
     * 存放“等待放入元素”的线程条件队列。当队列满时，生产者会在此等待可用空间。
     */
    @GuardedBy("lock")
    private final Queue<Condition> notFull;

    /**
     * 每个线程都有自己的 ConditionAndFlag 对象，用于唤醒标志和等待条件的管理。
     */
    @GuardedBy("lock")
    private ConditionAndFlag[] putConditionAndFlags;

    /**
     * 使用默认容量（来自 {@link SourceReaderOptions#ELEMENT_QUEUE_CAPACITY}）的构造函数。
     */
    public FutureCompletingBlockingQueue() {
        this(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue());
    }

    /**
     * 指定最大容量的构造函数。
     *
     * @param capacity 队列最大容量，必须大于 0。
     */
    public FutureCompletingBlockingQueue(int capacity) {
        checkArgument(capacity > 0, "capacity must be > 0");
        this.capacity = capacity;
        this.queue = new ArrayDeque<>(capacity);
        this.lock = new ReentrantLock();
        this.putConditionAndFlags = new ConditionAndFlag[1];
        this.notFull = new ArrayDeque<>();

        // 初始队列为空，因此当前 future 是一个未完成的 CompletableFuture
        this.currentFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  Future / Notification logic （队列可用性通知逻辑）
    // ------------------------------------------------------------------------

    /**
     * 返回可用性 Future。如果队列非空，该 Future 已完成；否则在下次队列变非空时或调用
     * {@link #notifyAvailable()} 时被完成。
     *
     * <p>需要注意：即便 Future 已完成，也不保证下次 {@code poll()} 一定能返回非空元素，
     * 因为可能存在并发消费或“虚假唤醒”等情况。因此若 {@code poll()} 返回空，应再次调用此方法。
     */
    public CompletableFuture<Void> getAvailabilityFuture() {
        return currentFuture;
    }

    /**
     * 若当前可用性 Future 未完成，则将其标记为完成；所有之前通过 {@link #getAvailabilityFuture()} 获取的
     * Future 均会被完成。
     *
     * <p>当队列通过 {@link #poll()} 再次变空后，可用性将被重置为一个新的 Future。
     */
    public void notifyAvailable() {
        lock.lock();
        try {
            moveToAvailable();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将当前 future 标记为 {@code AVAILABLE}，若其尚未处于该状态，则完成并替换。
     */
    @GuardedBy("lock")
    private void moveToAvailable() {
        final CompletableFuture<Void> current = currentFuture;
        if (current != AVAILABLE) {
            currentFuture = AVAILABLE;
            current.complete(null);
        }
    }

    /**
     * 将可用性重置为一个新的未完成 Future，以便下次队列变空或消费完后，消费者需再次等待。
     */
    @GuardedBy("lock")
    private void moveToUnAvailable() {
        if (currentFuture == AVAILABLE) {
            currentFuture = new CompletableFuture<>();
        }
    }

    // ------------------------------------------------------------------------
    //  Blocking Queue Logic （阻塞队列核心逻辑）
    // ------------------------------------------------------------------------

    /**
     * 向队列放入一个元素。当队列已满时，调用线程会被阻塞。
     *
     * @param threadIndex 唯一标识线程的索引，用于管理唤醒标志。
     * @param element 要放入的元素，不能为 null。
     * @return 若元素成功放入返回 true；若被唤醒标志中断此操作返回 false。
     * @throws InterruptedException 当线程被中断时抛出。
     */
    public boolean put(int threadIndex, T element) throws InterruptedException {
        if (element == null) {
            throw new NullPointerException();
        }
        lock.lockInterruptibly();
        try {
            // 当队列容量已满，等待可用空间或唤醒标志
            while (queue.size() >= capacity) {
                // 如果有唤醒标志则直接返回
                if (getAndResetWakeUpFlag(threadIndex)) {
                    return false;
                }
                waitOnPut(threadIndex);
            }
            // 若未被唤醒且有空间，将元素入队
            enqueue(element);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * <b>Warning:</b> This is a dangerous method and should only be used for testing convenience. A
     * method that blocks until availability does not go together well with the concept of
     * asynchronous notifications and non-blocking polling.
     *
     * <p>Get and remove the first element from the queue. The call blocks if the queue is empty.
     * The problem with this method is that it may loop internally until an element is available and
     * that way eagerly reset the availability future. If a consumer thread is blocked in taking an
     * element, it will receive availability notifications from {@link #notifyAvailable()} and
     * immediately reset them by calling {@link #poll()} and finding the queue empty.
     *
     * <p>中文说明：该方法会一直阻塞直到队列中出现可用数据后再返回，通常只建议在测试场景使用。
     * 因为它在获取元素的过程中可能不断地重新设置队列的可用性（availability）并导致“虚假唤醒”或过早重置。
     * 若消费者线程长期阻塞在此，将会主动消费可用性通知，并在 `poll()` 依旧为空时重置可用性，影响异步逻辑。
     *
     * @return the first element in the queue.
     * @throws InterruptedException when the thread is interrupted.
     */
    @VisibleForTesting
    public T take() throws InterruptedException {
        T next;
        while ((next = poll()) == null) {
            // use the future to wait for availability to avoid busy waiting
            try {
                getAvailabilityFuture().get();
            } catch (ExecutionException | CompletionException e) {
                // this should never happen, but we propagate just in case
                throw new FlinkRuntimeException("exception in queue future completion", e);
            }
        }
        return next;
    }

    /**
     * Get and remove the first element from the queue. Null is returned if the queue is empty. If
     * this makes the queue empty (takes the last element) or finds the queue already empty, then
     * this resets the availability notifications. The next call to {@link #getAvailabilityFuture()}
     * will then return a non-complete future that completes only the next time that the queue
     * becomes non-empty or the {@link #notifyAvailable()} method is called.
     *
     * <p>中文说明：移除并返回队列首元素。若队列为空则返回 Null。如果取完后队列变空或最初即为空，
     * 会重置队列的可用性（availability）状态，并导致下一次再调用 `getAvailabilityFuture()` 返回一个新的
     * 未完成 Future，直到队列下一次变非空或手动调用 `notifyAvailable()`。
     *
     * @return the first element from the queue, or Null if the queue is empty.
     */
    public T poll() {
        lock.lock();
        try {
            if (queue.size() == 0) {
                moveToUnAvailable();
                return null;
            }
            return dequeue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the first element from the queue without removing it.
     *
     * <p>中文说明：仅查看队列首元素但不删除，若队列为空则返回 Null。
     *
     * @return the first element in the queue, or Null if the queue is empty.
     */
    public T peek() {
        lock.lock();
        try {
            return queue.peek();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the size of the queue.
     *
     * <p>中文说明：获取当前队列中元素的数量。
     */
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks whether the queue is empty.
     *
     * <p>中文说明：判断队列是否为空。
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks the remaining capacity in the queue. That is the difference between the maximum
     * capacity and the current number of elements in the queue.
     *
     * <p>中文说明：计算队列剩余容量 = 最大容量 - 当前已用容量。
     */
    public int remainingCapacity() {
        lock.lock();
        try {
            return capacity - queue.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gracefully wakes up the thread with the given {@code threadIndex} if it is blocked in adding
     * an element to the queue. If the thread is blocked in {@link #put(int, Object)}, it will
     * immediately return from the method with a return value of false.
     *
     * <p>If this method is called, the next time the thread with the given index is about to be
     * blocked in adding an element, it may immediately wake up and return.
     *
     * <p>中文说明：当某个生产者线程（使用 threadIndex 标识）因队列已满而被阻塞时，
     * 可调用此方法进行优雅唤醒。若该线程正阻塞于 {@link #put(int, Object)}，则会
     * 立刻结束阻塞并返回 false。这种方式无需使用中断线程的手段。
     *
     * @param threadIndex The number identifying the thread.
     */
    public void wakeUpPuttingThread(int threadIndex) {
        lock.lock();
        try {
            maybeCreateCondition(threadIndex);
            ConditionAndFlag caf = putConditionAndFlags[threadIndex];
            if (caf != null) {
                // 将标志设置为唤醒，并通知阻塞线程的 Condition
                caf.setWakeUp(true);
                caf.condition().signal();
            }
        } finally {
            lock.unlock();
        }
    }

// --------------- private helpers （私有辅助方法）-------------------------

    /**
     * Insert an element into the queue. If the queue was empty, mark it as available.
     * Also, if there's space left, notify any waiting producer threads.
     *
     * <p>中文说明：将元素加入队列。若队列先前为空，则更新可用性；若仍有空间，则唤醒阻塞中的其他生产者线程。
     */
    @GuardedBy("lock")
    private void enqueue(T element) {
        final int sizeBefore = queue.size();
        queue.add(element);
        if (sizeBefore == 0) {
            moveToAvailable();
        }
        // 如果原先的队列大小 < capacity - 1 且存在等待中的生产者，唤醒其中一个
        if (sizeBefore < capacity - 1 && !notFull.isEmpty()) {
            signalNextPutter();
        }
    }

    /**
     * Remove and return the first element in the queue. If the queue becomes empty after removal,
     * mark it as unavailable. If the queue was full, notify waiting producer threads.
     *
     * <p>中文说明：移除并返回队列首元素。若移除后队列为空，则标记不可用；若队列先前已满，则唤醒等待中的生产者。
     */
    @GuardedBy("lock")
    private T dequeue() {
        final int sizeBefore = queue.size();
        final T element = queue.poll();
        if (sizeBefore == capacity && !notFull.isEmpty()) {
            signalNextPutter();
        }
        // 移除后若队列为空，则标记不可用
        if (queue.isEmpty()) {
            moveToUnAvailable();
        }
        return element;
    }

    /**
     * Wait for space to become available when the queue is full.
     *
     * <p>中文说明：当队列满时，当前线程在其 Condition 上阻塞，直到被唤醒后继续尝试放入元素。
     */
    @GuardedBy("lock")
    private void waitOnPut(int fetcherIndex) throws InterruptedException {
        maybeCreateCondition(fetcherIndex);
        Condition cond = putConditionAndFlags[fetcherIndex].condition();
        notFull.add(cond);
        cond.await();
    }

    /**
     * Notify the next producer thread (if any) that space is available.
     *
     * <p>中文说明：从队列中弹出并唤醒一个等待可用空间的生产者。
     */
    @GuardedBy("lock")
    private void signalNextPutter() {
        if (!notFull.isEmpty()) {
            notFull.poll().signal();
        }
    }

    /**
     * Make sure there's a ConditionAndFlag for the given thread index.
     * If the array isn't large enough, expand it.
     *
     * <p>中文说明：为给定的 threadIndex 初始化对应的 ConditionAndFlag 对象。
     * 若当前数组容量不足，则对其进行扩容。
     */
    @GuardedBy("lock")
    private void maybeCreateCondition(int threadIndex) {
        if (putConditionAndFlags.length < threadIndex + 1) {
            putConditionAndFlags = Arrays.copyOf(putConditionAndFlags, threadIndex + 1);
        }

        if (putConditionAndFlags[threadIndex] == null) {
            putConditionAndFlags[threadIndex] = new ConditionAndFlag(lock.newCondition());
        }
    }

    /**
     * Check and reset the wakeUp flag for the given thread. If set, unset it and return true.
     * Otherwise return false.
     *
     * <p>中文说明：判断并重置指定线程的唤醒标志。若已设置唤醒标志，则清除并返回 true；否则返回 false。
     */
    @GuardedBy("lock")
    private boolean getAndResetWakeUpFlag(int threadIndex) {
        maybeCreateCondition(threadIndex);
        if (putConditionAndFlags[threadIndex].getWakeUp()) {
            putConditionAndFlags[threadIndex].setWakeUp(false);
            return true;
        }
        return false;
    }

// --------------- private per thread state （每个线程的状态管理）------------

    /**
     * Each thread that puts data has its own Condition and a wakeUp flag.
     * The flag indicates if the thread should be woken up gracefully from a blocking put operation.
     *
     * <p>中文说明：封装了某个生产者线程的 Condition 以及唤醒标志 wakeUp。
     * 当队列满了阻塞线程时，可通过 wakeUp 标志来优雅退出阻塞。
     */
    private static class ConditionAndFlag {
        private final Condition cond;
        private boolean wakeUp;

        private ConditionAndFlag(Condition cond) {
            this.cond = cond;
            this.wakeUp = false;
        }

        private Condition condition() {
            return cond;
        }

        private boolean getWakeUp() {
            return wakeUp;
        }

        private void setWakeUp(boolean value) {
            wakeUp = value;
        }
    }

// ------------------------------------------------------------------------
//  utilities
// ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> getAvailableFuture() {
        // this is a way to obtain the AvailabilityProvider.AVAILABLE future until we decide to
        // move the class from the runtime module to the core module
        try {
            final Class<?> clazz =
                    Class.forName("org.apache.flink.runtime.io.AvailabilityProvider");
            final Field field = clazz.getDeclaredField("AVAILABLE");
            return (CompletableFuture<Void>) field.get(null);
        } catch (Throwable t) {
            return CompletableFuture.completedFuture(null);
        }
    }

}
