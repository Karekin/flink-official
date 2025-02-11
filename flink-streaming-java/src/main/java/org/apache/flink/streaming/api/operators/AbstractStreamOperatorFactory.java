/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 所有流算子工厂的基础抽象类。该类实现了一些通用方法，并实现了 {@link ProcessingTimeServiceAware} 接口，
 * 使得其所创建的流算子可以访问 {@link ProcessingTimeService}。
 *
 * <p>被标记为 {@link org.apache.flink.annotation.Experimental}，说明此接口或功能还在实验阶段，
 * 未来版本中可能会发生变化。</p>
 *
 * @param <OUT> 算子输出的数据类型
 */
@Experimental
public abstract class AbstractStreamOperatorFactory<OUT>
        implements StreamOperatorFactory<OUT>, ProcessingTimeServiceAware {

    /**
     * 决定算子链合并方式的策略，默认为 {@link ChainingStrategy#DEFAULT_CHAINING_STRATEGY}，
     * 用于控制算子之间是否可以在同一个 Task 中链式执行，从而减少线程间切换、提高吞吐。
     */
    protected ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

    /**
     * 处理时间服务，用于注册定时器或获取处理时间戳等。由于其在实际运行时由 Flink 注入，因此被声明为 transient。
     */
    protected transient ProcessingTimeService processingTimeService;

    /**
     * 邮箱执行器，用于在算子内部提交异步任务或延迟执行逻辑。仅在实现了 {@link YieldingOperatorFactory} 的情况下使用，
     * 同样在运行时由 Flink 注入，因此使用 transient。
     */
    @Nullable
    private transient MailboxExecutor mailboxExecutor;

    /**
     * 设置算子的链合并策略。由外部（例如 StreamGraph、StreamExecutionEnvironment 等）来控制算子链接行为。
     *
     * @param strategy 链合并策略
     */
    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        this.chainingStrategy = strategy;
    }

    /**
     * 获取当前设定的链合并策略。
     *
     * @return 算子链合并策略
     */
    @Override
    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    /**
     * 注入处理时间服务，以便算子能够使用定时器功能（如注册处理时间定时器）。
     *
     * @param processingTimeService Flink 运行时注入的处理时间服务
     */
    @Override
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
    }

    /**
     * 注入邮箱执行器，用于在算子中安全地提交异步任务或进行协作式多线程调度。
     * 仅当该工厂实现了 {@link YieldingOperatorFactory} 时，Flink 才会使用此方法进行设置。
     *
     * @param mailboxExecutor 提供给算子的邮箱执行器
     */
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
        this.mailboxExecutor = mailboxExecutor;
    }

    /**
     * 当工厂实现了 {@link YieldingOperatorFactory} 时，提供对邮箱执行器的访问。
     * 如果当前工厂并未实现 {@code YieldingOperatorFactory}，则会抛出空指针异常提示。
     *
     * @return 邮箱执行器对象
     */
    protected MailboxExecutor getMailboxExecutor() {
        return checkNotNull(
                mailboxExecutor, "Factory does not implement %s", YieldingOperatorFactory.class);
    }
}

