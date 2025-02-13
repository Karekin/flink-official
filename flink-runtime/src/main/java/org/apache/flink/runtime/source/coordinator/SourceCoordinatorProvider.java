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

import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SourceCoordinator} 的提供者（Provider），用于创建 SourceCoordinator 实例。
 *
 * <p>SourceCoordinatorProvider 继承自 {@link RecreateOnResetOperatorCoordinator.Provider}，
 * 允许在作业恢复或重置时重新创建 SourceCoordinator。
 *
 * @param <SplitT> Source 拆分（Split）的类型
 */
public class SourceCoordinatorProvider<SplitT extends SourceSplit>
        extends RecreateOnResetOperatorCoordinator.Provider {

    private static final long serialVersionUID = -1921681440009738462L;

    // 算子名称
    private final String operatorName;
    // 关联的数据源（Source）
    private final Source<?, SplitT, ?> source;
    // 工作线程数（用于 SplitEnumerator 的异步调用）
    private final int numWorkerThreads;
    // 水位线对齐参数
    private final WatermarkAlignmentParams alignmentParams;
    // 协调器监听 ID（可选）
    @Nullable private final String coordinatorListeningID;

    /**
     * 构造 SourceCoordinatorProvider 实例。
     *
     * @param operatorName 算子名称
     * @param operatorID 算子的唯一标识符
     * @param source 关联的数据源（Source）
     * @param numWorkerThreads 分配给 SplitEnumerator 进行异步调用的线程数
     * @param alignmentParams 水位线对齐参数
     * @param coordinatorListeningID 协调器的监听 ID（可选）
     */
    public SourceCoordinatorProvider(
            String operatorName,
            OperatorID operatorID,
            Source<?, SplitT, ?> source,
            int numWorkerThreads,
            WatermarkAlignmentParams alignmentParams,
            @Nullable String coordinatorListeningID) {
        super(operatorID);
        this.operatorName = operatorName;
        this.source = source;
        this.numWorkerThreads = numWorkerThreads;
        this.alignmentParams = alignmentParams;
        this.coordinatorListeningID = coordinatorListeningID;
    }

    /**
     * 创建并返回 SourceCoordinator 实例。
     *
     * @param context 算子协调器的上下文信息
     * @return 新创建的 SourceCoordinator
     */
    @Override
    public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
        // 为协调器创建一个专用线程
        final String coordinatorThreadName = "SourceCoordinator-" + operatorName;
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(coordinatorThreadName, context);

        // 获取 Source 拆分的序列化器
        SimpleVersionedSerializer<SplitT> splitSerializer = source.getSplitSerializer();

        // 创建 SourceCoordinatorContext，上下文管理线程执行、拆分序列化等
        SourceCoordinatorContext<SplitT> sourceCoordinatorContext =
                new SourceCoordinatorContext<>(
                        coordinatorThreadFactory,
                        numWorkerThreads,
                        context,
                        splitSerializer,
                        context.isConcurrentExecutionAttemptsSupported());

        // 返回新的 SourceCoordinator 实例
        return new SourceCoordinator<>(
                operatorName,
                source,
                sourceCoordinatorContext,
                context.getCoordinatorStore(),
                alignmentParams,
                coordinatorListeningID);
    }

    // ----------------- 协调器线程工厂 -----------------

    /**
     * 用于创建 SourceCoordinator 线程的工厂类。
     *
     * <p>该类提供了一些辅助方法，确保 SourceCoordinator 线程唯一且可以检测当前线程是否合法。
     * 由于该工厂仅允许创建一个线程，因此不要用它来创建多个线程。
     */
    public static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinatorProvider.class);

        // 协调器线程名称
        private final String coordinatorThreadName;
        // 类加载器（用于确保任务在正确的 ClassLoader 上下文中运行）
        private final ClassLoader cl;
        // 线程异常处理器
        private final Thread.UncaughtExceptionHandler errorHandler;
        // 线程实例（仅允许创建一个线程）
        @Nullable private Thread t;

        /**
         * 通过 OperatorCoordinator.Context 创建协调器线程工厂。
         *
         * @param coordinatorThreadName 协调器线程名称
         * @param context OperatorCoordinator 上下文
         */
        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName, final OperatorCoordinator.Context context) {
            this(
                    coordinatorThreadName,
                    context.getUserCodeClassloader(),
                    (t, e) -> {
                        // 当 SourceCoordinator 线程发生未捕获异常时，记录日志并使作业失败
                        LOG.error(
                                "线程 '{}' 发生未捕获异常，导致作业失败。",
                                t.getName(),
                                e);
                        context.failJob(e);
                    });
        }

        /**
         * 创建协调器线程工厂，允许自定义错误处理。
         *
         * @param coordinatorThreadName 线程名称
         * @param contextClassLoader 类加载器
         * @param errorHandler 线程异常处理器
         */
        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName,
                final ClassLoader contextClassLoader,
                final Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.cl = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        /**
         * 创建新的协调器线程。
         *
         * @param r 线程执行的任务
         * @return 创建的线程
         */
        @Override
        public synchronized Thread newThread(Runnable r) {
            // 确保该工厂只创建一个线程
            checkState(
                    t == null,
                    "请使用新的 CoordinatorExecutorThreadFactory，该工厂无法创建多个线程。");
            t = new Thread(r, coordinatorThreadName);
            t.setContextClassLoader(cl); // 设置类加载器
            t.setUncaughtExceptionHandler(this); // 设置异常处理器
            return t;
        }

        /**
         * 处理线程中的未捕获异常。
         *
         * @param t 发生异常的线程
         * @param e 发生的异常
         */
        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorHandler.uncaughtException(t, e);
        }

        /**
         * 获取协调器线程的名称。
         *
         * @return 线程名称
         */
        String getCoordinatorThreadName() {
            return coordinatorThreadName;
        }

        /**
         * 检查当前线程是否为协调器线程。
         *
         * @return 如果是协调器线程，则返回 true；否则返回 false
         */
        boolean isCurrentThreadCoordinatorThread() {
            return Thread.currentThread() == t;
        }
    }
}

