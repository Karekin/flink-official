/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * 这个类用于在两个组件之间进行协调，其中一个组件有一个基于Mailbox模型的执行器，
 * 而另一个组件在需要时通知它。
 */
public class ExecutorNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorNotifier.class);
    private final ScheduledExecutorService workerExecutor; // 负责执行任务的调度线程池
    private final Executor executorToNotify; // 被通知执行任务的执行器

    /**
     * 构造方法。
     *
     * @param workerExecutor 用于执行后台任务的调度线程池
     * @param executorToNotify 用于接收通知并执行结果处理的执行器
     */
    public ExecutorNotifier(ScheduledExecutorService workerExecutor, Executor executorToNotify) {
        this.executorToNotify = executorToNotify;
        this.workerExecutor = workerExecutor;
    }

    /**
     * 执行给定的 Callable 一次，并通知 {@link #executorToNotify} 来执行结果处理。
     *
     * <p>注意：当此方法被多次调用时，可能会导致多个 Callable 同时执行，
     * 处理程序（handler）也可能会并发执行。例如，假设 workerExecutor 和
     * executorToNotify 都是单线程的，以下代码可能仍会抛出
     * <code>ConcurrentModificationException</code>。
     *
     * <pre>{@code
     * final List<Integer> list = new ArrayList<>();
     *
     * // Callable 向列表添加整数 1，表面上看没问题，
     * // 但可能会因为 Callable 和 handler 同时修改列表而抛出异常。
     * notifier.notifyReadyAsync(
     *     () -> list.add(1),
     *     (ignoredValue, ignoredThrowable) -> list.add(2));
     * }</pre>
     *
     * <p>正确的实现方式应该是：
     *
     * <pre>{@code
     * // 在 handler 中修改状态。
     * notifier.notifyReadyAsync(() -> 1, (v, ignoredThrowable) -> {
     *     list.add(v);
     *     list.add(2);
     * });
     * }</pre>
     *
     * @param callable 要在通知执行器之前调用的 Callable
     * @param handler 处理 Callable 返回结果的处理程序
     * @param <T> Callable 返回值的类型
     */
    public <T> void notifyReadyAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        workerExecutor.execute(
                () -> {
                    try {
                        // 调用 Callable 并获取结果
                        T result = callable.call();
                        // 通知 executorToNotify 执行处理程序
                        executorToNotify.execute(() -> handler.accept(result, null));
                    } catch (Throwable t) {
                        // 如果发生异常，将异常传递给处理程序
                        executorToNotify.execute(() -> handler.accept(null, t));
                    }
                });
    }

    /**
     * 周期性地调用给定的 Callable，并通知 {@link #executorToNotify} 来执行结果处理。
     *
     * <p>注意：当此方法被多次调用时，可能会导致多个 Callable 和处理程序并发执行。
     * 例如，与前一个方法类似，如果调度线程池和执行器都是单线程的，仍可能出现并发问题。
     *
     * @param callable 要定期调用的 Callable
     * @param handler 处理 Callable 返回结果的处理程序
     * @param initialDelayMs 调用 Callable 前的初始延迟时间（毫秒）
     * @param periodMs 调用 Callable 的周期时间（毫秒）
     * @param <T> Callable 返回值的类型
     */
    public <T> void notifyReadyAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelayMs,
            long periodMs) {
        workerExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        // 调用 Callable 并获取结果
                        T result = callable.call();
                        // 通知 executorToNotify 执行处理程序
                        executorToNotify.execute(() -> handler.accept(result, null));
                    } catch (Throwable t) {
                        // 如果发生异常，将异常传递给处理程序
                        executorToNotify.execute(() -> handler.accept(null, t));
                    }
                },
                initialDelayMs, // 初始延迟时间
                periodMs,       // 调用周期
                TimeUnit.MILLISECONDS); // 时间单位为毫秒
    }
}

