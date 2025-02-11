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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * 操作符协调器接口定义，负责在JobMaster中协调运行时操作符的全局行为
 * 核心职责包括：
 * 1. 管理操作符子任务的生命周期（启动/重置/失败处理）
 * 2. 协调检查点机制与状态恢复
 * 3. 处理操作符事件通信
 *
 * @see org.apache.flink.runtime.jobgraph.OperatorID 关联的操作符唯一标识
 * @see CheckpointListener 检查点监听接口
 */
@Internal
public interface OperatorCoordinator extends CheckpointListener, AutoCloseable {

    /**
     * 表示无有效检查点的特殊标识，用于恢复初始状态
     */
    long NO_CHECKPOINT = -1L;

    // --------------------------- 生命周期管理 ---------------------------

    /**
     * 启动协调器，在作业启动时由JobManager调用
     * @throws Exception 启动失败将导致整个作业失败
     *
     * 实现建议：
     * - 初始化内部状态
     * - 建立与外部系统的连接
     * - 启动后台线程（如有需要）
     */
    void start() throws Exception;

    /**
     * 关闭协调器并释放资源
     * @throws Exception 关闭异常将被记录但不会导致作业失败
     *
     * 注意：
     * - 必须确保线程安全
     * - 需要处理中断场景
     */
    @Override
    void close() throws Exception;

    // ------------------------- 事件处理机制 -------------------------

    /**
     * 处理来自操作符子任务的事件
     * @param subtask       源子任务索引（0 ~ parallelism-1）
     * @param attemptNumber 子任务执行尝试次数（用于处理失败重试）
     * @param event         操作符事件负载
     * @throws Exception 处理失败将触发作业全局恢复
     *
     * 典型应用场景：
     * - 接收子任务状态汇报
     * - 处理子任务请求的协调操作
     */
    void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception;

    // ------------------------ 检查点协调机制 ------------------------

    /**
     * 执行协调器检查点
     * @param checkpointId  唯一检查点ID
     * @param resultFuture  用于返回检查点状态的Future
     * @throws Exception 检查点失败将触发作业恢复
     *
     * 实现要求：
     * - 必须严格保证事件发送与检查点完成的顺序性
     * - 建议使用同步机制（如锁）协调事件发送与状态保存
     *
     * 精确一次语义保证：
     * 1. 检查点Future完成前发送的事件 → 属于该检查点之前
     * 2. 检查点Future完成后发送的事件 → 属于该检查点之后
     */
    void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception;

    /**
     * 通知检查点完成
     * @param checkpointId 已完成的检查点ID
     *
     * 注意事项：
     * - 此方法调用不保证顺序性
     * - 需处理重复通知的情况
     */
    @Override
    void notifyCheckpointComplete(long checkpointId);

    /**
     * 通知检查点中止
     * @param checkpointId 被中止的检查点ID
     */
    @Override
    default void notifyCheckpointAborted(long checkpointId) {}

    // ------------------------ 状态恢复机制 ------------------------

    /**
     * 全局恢复协调器状态（JobManager故障恢复时调用）
     * @param checkpointId 要恢复到的检查点ID
     * @param checkpointData 检查点数据（可能为null）
     * @throws Exception 恢复失败将导致作业无法启动
     *
     * 特殊场景处理：
     * - checkpointData为null时表示恢复到初始状态
     * - 必须确保恢复后状态与子任务状态一致
     */
    void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception;

    /**
     * 子任务级重置（部分故障恢复时调用）
     * @param subtask      需要重置的子任务索引
     * @param checkpointId 目标检查点ID
     *
     * 与全局恢复的区别：
     * - 仅影响指定子任务
     * - 通常由调度器的局部恢复策略触发
     */
    void subtaskReset(int subtask, long checkpointId);

    // ---------------------- 子任务生命周期监控 ----------------------

    /**
     * 处理子任务执行失败事件
     * @param subtask       失败子任务索引
     * @param attemptNumber 失败尝试次数
     * @param reason        失败原因（可能为null）
     *
     * 后续处理：
     * - 停止向该子任务发送事件
     * - 记录错误指标
     */
    void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason);

    /**
     * 通知子任务准备就绪
     * @param subtask       子任务索引
     * @param attemptNumber 执行尝试次数
     * @param gateway       子任务通信网关
     *
     * 典型操作：
     * - 发送初始化事件
     * - 同步状态信息
     */
    void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway);

    // -------------------------- 上下文接口 --------------------------

    /**
     * 协调器上下文接口，提供环境信息访问入口
     */
    interface Context {
        /**
         * 获取操作符唯一标识
         * @return OperatorID实例
         */
        OperatorID getOperatorId();

        /**
         * 获取指标收集器
         * @return 预绑定的度量组
         */
        OperatorCoordinatorMetricGroup metricGroup();

        /**
         * 触发作业级故障恢复
         * @param cause 故障原因
         *
         * 使用场景：
         * - 检测到不可恢复的协调状态不一致
         * - 关键外部系统连接丢失
         */
        void failJob(Throwable cause);

        /**
         * 获取当前操作符并行度
         * @return 并行度数值
         */
        int currentParallelism();

        /**
         * 获取用户代码类加载器
         * @return 隔离的类加载器实例
         */
        ClassLoader getUserCodeClassloader();

        /**
         * 获取协调器共享存储
         * @return 线程安全的存储容器
         */
        CoordinatorStore getCoordinatorStore();

        /**
         * 是否支持并发执行尝试
         * @return true表示允许同一子任务多个执行实例
         */
        boolean isConcurrentExecutionAttemptsSupported();

        /**
         * 获取检查点协调器
         * @return 可能为null（检查点禁用时）
         */
        @Nullable
        CheckpointCoordinator getCheckpointCoordinator();
    }

    // ------------------------ 子任务通信网关 ------------------------

    /**
     * 子任务通信网关接口
     */
    interface SubtaskGateway {
        /**
         * 发送操作符事件到对应子任务
         * @param evt 事件对象
         * @return 异步确认Future
         *
         * 注意事项：
         * - 事件发送不保证可靠性
         * - 需处理子任务已终止的情况
         */
        CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt);

        /**
         * 获取目标执行尝试ID
         * @return 唯一执行标识
         */
        ExecutionAttemptID getExecution();

        /**
         * 获取目标子任务索引
         * @return 0到并行度-1之间的整数
         */
        int getSubtask();
    }

    // ------------------------- 协调器工厂接口 ------------------------

    /**
     * 协调器工厂接口（需支持序列化）
     */
    interface Provider extends Serializable {
        /**
         * 获取关联操作符ID
         * @return 操作符唯一标识
         */
        OperatorID getOperatorId();

        /**
         * 创建协调器实例
         * @param context 上下文对象
         * @return 初始化后的协调器
         * @throws Exception 实例化失败将导致作业提交失败
         */
        OperatorCoordinator create(Context context) throws Exception;
    }
}

