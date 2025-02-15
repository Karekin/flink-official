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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * 该接口提供对并行子任务（subtask）的访问，并作为目标，允许 {@link OperatorCoordinator} 发送
 * {@link OperatorEvent} 事件到特定的子任务执行尝试（Execution Attempt）。
 *
 * <p><b>重要：</b> 一个 {@code SubtaskAccess} 实例必须绑定到某个特定的子任务执行尝试（Execution Attempt）。
 * 如果该执行尝试失败，则该实例不能再绑定到其他执行尝试，而是需要通过 {@link SubtaskAccess.SubtaskAccessFactory}
 * 创建新的实例。
 */
interface SubtaskAccess {

    /**
     * 创建一个 {@link Callable} 对象，该对象在调用时，会将事件发送到该 {@code SubtaskAccess} 绑定的
     * 子任务执行尝试，并返回一个异步执行结果（{@link CompletableFuture}）。
     *
     * <p>该方法允许调用者指定一个特定的子任务作为目标，但不一定立即发送事件。例如，可以通过
     * {@link SubtaskGatewayImpl} 进行对齐检查点（checkpoint alignment）后再发送事件。
     *
     * @param event 需要发送的 {@link OperatorEvent} 事件（已序列化）
     * @return 发送事件的操作，返回一个 {@link Callable}，调用该操作后会返回一个表示发送状态的 {@link CompletableFuture}
     */
    Callable<CompletableFuture<Acknowledge>> createEventSendAction(
            SerializedValue<OperatorEvent> event);

    /**
     * 获取目标子任务的并行索引（parallel subtask index）。
     *
     * @return 该子任务的索引
     */
    int getSubtaskIndex();

    /**
     * 获取该实例绑定的子任务执行尝试的执行 ID。
     *
     * @return 该执行尝试的 {@link ExecutionAttemptID}
     */
    ExecutionAttemptID currentAttempt();

    /**
     * 获取该操作符子任务的描述性名称，包括：
     * - 操作符名称
     * - 子任务 ID
     * - 并行度
     * - 执行尝试 ID
     *
     * @return 该子任务的描述性名称
     */
    String subtaskName();

    /**
     * 该方法返回的 {@link CompletableFuture} 会在目标子任务进入运行状态（running state）时完成。
     *
     * <p>子任务的运行状态包括：
     * - {@link ExecutionState#RUNNING}
     * - {@link ExecutionState#INITIALIZING}
     *
     * @return 一个 {@link CompletableFuture}，表示该子任务是否切换到运行状态
     */
    CompletableFuture<?> hasSwitchedToRunning();

    /**
     * 检查当前子任务的执行尝试是否仍处于运行状态。
     * 该方法的详细状态可参考 {@link #hasSwitchedToRunning()}。
     *
     * @return 如果该子任务的执行仍然在运行状态，则返回 true，否则返回 false
     */
    boolean isStillRunning();

    /**
     * 触发该实例绑定的子任务执行尝试的失败恢复（failover）。
     * 该方法通常用于在某些异常情况下强制触发任务失败，以便调度器执行故障恢复机制。
     *
     * @param cause 触发失败恢复的原因
     */
    void triggerTaskFailover(Throwable cause);

    // ------------------------------------------------------------------------

    /**
     * 当 {@link SubtaskAccess} 绑定到某个子任务的执行尝试时（例如 {@link
     * org.apache.flink.runtime.executiongraph.Execution}），该工厂类 {@code SubtaskAccessFactory}
     * 绑定到整个操作符（Operator）的范围，例如 {@link
     * org.apache.flink.runtime.executiongraph.ExecutionJobVertex}。
     *
     * <p>该工厂类负责创建和管理多个 {@link SubtaskAccess} 实例，确保 OperatorCoordinator 可以正确地
     * 访问所有子任务执行尝试。
     */
    interface SubtaskAccessFactory {

        /**
         * 获取指定子任务索引的所有当前执行尝试（execution attempt）。
         *
         * @param subtaskIndex 目标子任务的索引
         * @return 该子任务当前所有执行尝试的集合
         */
        Collection<SubtaskAccess> getAccessesForSubtask(int subtaskIndex);

        /**
         * 获取指定子任务索引和执行尝试编号的 {@link SubtaskAccess} 实例。
         *
         * @param subtaskIndex 目标子任务的索引
         * @param attemptNumber 该子任务的执行尝试编号
         * @return 该执行尝试对应的 {@link SubtaskAccess} 实例
         */
        SubtaskAccess getAccessForAttempt(int subtaskIndex, int attemptNumber);
    }
}

