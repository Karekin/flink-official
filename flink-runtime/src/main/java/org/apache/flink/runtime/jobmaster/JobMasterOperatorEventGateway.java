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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/**
 * 该接口（JobMasterOperatorEventGateway）用于 **在 Flink 任务执行过程中**，
 * 负责 **从 TaskManager 向 JobManager 发送** {@link OperatorEvent} 或 {@link CoordinationRequest}，
 * 并最终由 **JobManager 端的 {@link OperatorCoordinator} 处理**。
 *
 * <p>在 Flink 任务执行过程中，算子（Operator）和协调器（Coordinator）之间需要通信：
 * <ul>
 *     <li>TaskManager 运行的算子可能需要向 JobManager 上的协调器发送 **事件**（OperatorEvent）。</li>
 *     <li>TaskManager 运行的算子可能需要向 JobManager 上的协调器发送 **请求**（CoordinationRequest），
 *         并期待协调器返回一个 **异步响应**（CoordinationResponse）。</li>
 * </ul>
 *
 * <h2>事件传输链路</h2>
 * <p>事件在 Flink 集群的 **TaskManager** 和 **JobManager** 之间流转，具体如下：
 * <pre>
 *     1. 任务管理器（TaskManager）上的算子（Operator）希望向协调器（Coordinator）发送事件，
 *        于是它调用 {@link OperatorEventGateway#sendEventToCoordinator(OperatorEvent)}。
 *     2. 事件传递给 {@link TaskOperatorEventGateway}，该层会添加 **ExecutionAttemptID**（执行尝试 ID），
 *        然后转发给：
 *     3. {@link JobMasterOperatorEventGateway}，它是一个 **RPC 接口**，用于将事件从 TaskManager 发送到
 *        JobManager 上的 **OperatorCoordinator** 进行处理。
 *     4. 最终，JobManager 上的 **OperatorCoordinator** 通过
 *        {@link OperatorCoordinator#handleEventFromOperator(int, int, OperatorEvent)}
 *        方法处理该事件。
 * </pre>
 *
 * <h2>作用</h2>
 * <p>该接口主要提供两个功能：</p>
 * <ul>
 *     <li>**算子 → 协调器 事件传输（sendOperatorEventToCoordinator）**：
 *         允许算子向协调器发送事件（OperatorEvent），用于状态同步、算子间协作等场景。</li>
 *     <li>**算子 → 协调器 请求-响应机制（sendRequestToCoordinator）**：
 *         允许算子向协调器发送请求（CoordinationRequest），并等待协调器返回的响应（CoordinationResponse）。</li>
 * </ul>
 */
public interface JobMasterOperatorEventGateway {

    /**
     * 发送一个 **Operator 事件**（OperatorEvent）到 **JobManager** 端的 **OperatorCoordinator**，
     * 事件的目标 **OperatorCoordinator** 由 **OperatorID** 进行唯一标识。
     *
     * <p>该方法适用于算子需要通知协调器某些状态变化，或者在算子和协调器之间建立事件驱动的交互方式。</p>
     *
     * <h3>调用流程：</h3>
     * <ol>
     *     <li>算子调用 {@code sendOperatorEventToCoordinator} 发送事件到 TaskManager。</li>
     *     <li>TaskManager 端的 {@link TaskOperatorEventGateway} 进一步封装事件信息，并调用 RPC 远程过程调用。</li>
     *     <li>JobManager 端的 {@link JobMasterOperatorEventGateway} 接收到事件，并调用
     *         {@link OperatorCoordinator#handleEventFromOperator(int, int, OperatorEvent)} 进行处理。</li>
     * </ol>
     *
     * <h3>应用场景：</h3>
     * <ul>
     *     <li>算子需要通知协调器某个关键状态发生变化。</li>
     *     <li>算子希望请求协调器提供额外的计算资源或调整任务策略。</li>
     *     <li>算子在事件驱动的任务场景下，希望与协调器进行事件交互。</li>
     * </ul>
     *
     * @param task        任务的执行尝试 ID（ExecutionAttemptID），用于唯一标识算子的当前执行实例。
     * @param operatorID  目标协调器的 {@link OperatorID}，用于唯一标识目标 OperatorCoordinator。
     * @param event       要发送的事件，必须先进行序列化（{@link SerializedValue}）。
     * @return 一个 {@link CompletableFuture}，当 JobManager 成功接收事件后会返回 {@link Acknowledge} 确认消息。
     */
    CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            ExecutionAttemptID task, OperatorID operatorID, SerializedValue<OperatorEvent> event);

    /**
     * 发送一个 **请求**（CoordinationRequest）到 **JobManager** 端的 **OperatorCoordinator**，
     * 并返回一个异步的 **响应**（CoordinationResponse）。
     *
     * <p>该方法适用于算子需要向协调器请求信息，并 **等待协调器返回数据**，可用于动态调整算子参数、查询全局状态等场景。</p>
     *
     * <h3>调用流程：</h3>
     * <ol>
     *     <li>算子调用 {@code sendRequestToCoordinator} 发送请求到 TaskManager。</li>
     *     <li>TaskManager 端的 {@link TaskOperatorEventGateway} 进一步封装请求信息，并调用 RPC 远程过程调用。</li>
     *     <li>JobManager 端的 {@link JobMasterOperatorEventGateway} 接收到请求，并调用
     *         {@link OperatorCoordinator#handleEventFromOperator(int, int, OperatorEvent)} 进行处理。</li>
     *     <li>OperatorCoordinator 处理请求，并返回一个 **异步响应（CompletableFuture<CoordinationResponse>）**。</li>
     *     <li>最终，TaskManager 端接收该响应，并返回给请求方（算子）。</li>
     * </ol>
     *
     * <h3>应用场景：</h3>
     * <ul>
     *     <li>算子需要向协调器请求 **配置信息**，例如动态调整算子的行为。</li>
     *     <li>算子需要查询 **全局共享状态**，例如协调器维护的一些元数据。</li>
     *     <li>算子希望在 **协调器端触发某些计算逻辑**，并获得结果。</li>
     * </ul>
     *
     * @param operatorID  目标协调器的 {@link OperatorID}，用于唯一标识目标 OperatorCoordinator。
     * @param request     要发送的请求，必须先进行序列化（{@link SerializedValue}）。
     * @return 一个 {@link CompletableFuture}，当协调器处理完成后，将返回一个 {@link CoordinationResponse} 作为结果。
     */
    CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> request);
}

