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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/**
 * 该接口（TaskOperatorEventGateway）用于 **在 Flink 任务执行过程中**，
 * 负责从 **TaskManager** 发送 {@link OperatorEvent} 或 {@link CoordinationRequest} 到
 * **JobManager** 端的 {@link OperatorCoordinator}。
 *
 * <p>这是从 **算子（Operator）到协调器（Coordinator）** 发送事件和请求的 **第一步**。
 * 事件或请求在流转过程中，每一层都会增加更多的上下文信息，以保持底层组件的独立性，
 * 降低耦合性，使得系统更易于维护和测试。
 *
 * <h2>事件传输链路</h2>
 * <p>事件在 Flink 集群内部的 **TaskManager** 和 **JobManager** 之间传递：
 * <pre>
 *     1. 任务管理器（TaskManager）上的算子（Operator）调用
 *        {@code TaskOperatorEventGateway.sendOperatorEventToCoordinator(...)} 发送事件，
 *        该方法会附加 {@link OperatorID} 并将事件转发给：
 *     2. {@link TaskOperatorEventGateway} 进一步为事件附加 {@link ExecutionAttemptID}，
 *        然后将事件转发给：
 *     3. {@link JobMasterOperatorEventGateway}，它是一个 RPC 接口，
 *        负责将事件从 TaskManager 发送到 JobManager 上的 **OperatorCoordinator** 进行处理。
 * </pre>
 *
 * <h2>用途</h2>
 * <ul>
 *     <li>算子向协调器发送事件（Operator Event），例如：通知状态变化。</li>
 *     <li>算子向协调器发送请求（Coordination Request），并期待一个异步响应。</li>
 *     <li>在算子与协调器之间进行双向通信，以实现更复杂的控制逻辑，如动态调整算子参数。</li>
 * </ul>
 */
public interface TaskOperatorEventGateway {

    /**
     * 发送一个 **Operator 事件**（OperatorEvent）到 JobManager 端的 **OperatorCoordinator**，
     * 事件的目标 **OperatorCoordinator** 由 **OperatorID** 进行唯一标识。
     *
     * <p>算子可以通过此方法向协调器发送特定的事件，例如：
     * <ul>
     *     <li>算子需要通知协调器某个关键状态变化。</li>
     *     <li>算子希望请求协调器提供额外的计算资源或调整任务策略。</li>
     * </ul>
     *
     * <h3>调用流程：</h3>
     * <ol>
     *     <li>调用 {@code sendOperatorEventToCoordinator} 发送事件到 TaskManager 层的 TaskOperatorEventGateway。</li>
     *     <li>TaskOperatorEventGateway 进一步封装事件，并转发给 JobManager 层的 JobMasterOperatorEventGateway。</li>
     *     <li>最终，JobMasterOperatorEventGateway 通过 RPC 调用，将事件交给 OperatorCoordinator 处理。</li>
     * </ol>
     *
     * @param operator 目标协调器的 {@link OperatorID}，用于唯一标识目标 OperatorCoordinator。
     * @param event    要发送的事件，必须先进行序列化（{@link SerializedValue}）。
     */
    void sendOperatorEventToCoordinator(OperatorID operator, SerializedValue<OperatorEvent> event);

    /**
     * 发送一个 **请求**（CoordinationRequest）到 JobManager 端的 **OperatorCoordinator**，
     * 并返回一个异步的响应（CoordinationResponse）。
     *
     * <p>该方法允许算子向协调器发送特定的请求，并 **等待响应**，适用于需要请求-响应交互的场景。
     *
     * <h3>应用场景：</h3>
     * <ul>
     *     <li>算子需要从协调器获取配置信息，例如动态调整算子的行为。</li>
     *     <li>算子需要查询全局共享状态，例如协调器维护的一些元数据信息。</li>
     *     <li>算子希望在协调器端触发某些计算逻辑，并获得结果。</li>
     * </ul>
     *
     * <h3>调用流程：</h3>
     * <ol>
     *     <li>调用 {@code sendRequestToCoordinator} 发送请求到 TaskOperatorEventGateway。</li>
     *     <li>TaskOperatorEventGateway 进一步封装请求，并转发给 JobMasterOperatorEventGateway。</li>
     *     <li>JobMasterOperatorEventGateway 通过 RPC 将请求转发给 OperatorCoordinator。</li>
     *     <li>OperatorCoordinator 处理请求，并返回一个异步响应（CompletableFuture<CoordinationResponse>）。</li>
     *     <li>最终，TaskManager 端接收该响应，并返回给请求方（算子）。</li>
     * </ol>
     *
     * @param operator 目标协调器的 {@link OperatorID}，用于唯一标识目标 OperatorCoordinator。
     * @param request  要发送的请求，必须先进行序列化（{@link SerializedValue}）。
     * @return 一个 {@link CompletableFuture}，当协调器处理完成后，将返回一个 {@link CoordinationResponse}。
     */
    CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operator, SerializedValue<CoordinationRequest> request);
}

