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


/**
 * {@code OperatorEventHandler} 是算子端（Operator 端）的 **事件处理接口**，
 * 负责处理来自 **OperatorCoordinator** 的事件。
 *
 * <p>在 Flink 分布式计算框架中，**算子（Operator）** 和 **协调器（Coordinator）** 之间
 * 需要进行事件交互，以便在作业执行过程中进行调度、控制、数据同步等操作。
 *
 * <p>该接口的实现类通常是运行在 **TaskManager** 端的算子（Operator）。
 * **当 JobManager 端的 `OperatorCoordinator` 发送事件时，该算子会通过此接口处理该事件。**
 *
 * <h2>事件传输链路</h2>
 * <p>事件在 **JobManager** 和 **TaskManager** 之间流转，完整路径如下：
 * <pre>
 *     1. JobManager 端的 `OperatorCoordinator` 需要通知算子某些信息，
 *        于是调用 `OperatorCoordinator.Context#sendEvent(OperatorEvent)` 发送事件。
 *     2. 事件经过 `JobMasterOperatorEventGateway` 传输至 TaskManager。
 *     3. TaskManager 通过 `TaskOperatorEventGateway` 接收事件，并传递给目标算子。
 *     4. 目标算子内部的 `OperatorEventHandler` 处理该事件，
 *        具体实现 `handleOperatorEvent(OperatorEvent evt)` 方法。
 * </pre>
 *
 * <h2>用途</h2>
 * <ul>
 *     <li>协调器（Coordinator）可以通过事件通知算子执行某些操作，例如调整算子参数。</li>
 *     <li>算子可以接收协调器发送的全局配置信息，例如广播变量或调优参数。</li>
 *     <li>算子在恢复或故障恢复时，可以通过事件获取协调器存储的最新状态。</li>
 * </ul>
 *
 * <h2>示例：如何在算子中处理 OperatorEvent</h2>
 * <p>假设我们有一个 `OperatorCoordinator` 需要向 `KeyedProcessOperator` 发送控制信号：
 *
 * 1. 在 `OperatorCoordinator` 端发送事件：
 * </p>
 * <pre>{@code
 * OperatorEvent myEvent = new MyCustomEvent("Update processing threshold");
 * operatorCoordinatorContext.sendEvent(myEvent);
 * }</pre>
 *
 * <p>2. 在 `OperatorEventHandler` 端（即算子端）接收并处理该事件：</p>
 * <pre>{@code
 * @Override
 * public void handleOperatorEvent(OperatorEvent evt) {
 *     if (evt instanceof MyCustomEvent) {
 *         MyCustomEvent event = (MyCustomEvent) evt;
 *         LOG.info("Received control message from Coordinator: {}", event.getMessage());
 *     }
 * }
 * }</pre>
 *
 * <p>这样，算子可以接收来自 `OperatorCoordinator` 的事件，并执行相应的逻辑。</p>
 *
 * @see OperatorCoordinator#handleEventFromOperator(int, int, OperatorEvent)
 */
public interface OperatorEventHandler {

    /**
     * 处理来自 **OperatorCoordinator** 的事件（OperatorEvent）。
     *
     * <p>当 `OperatorCoordinator` 调用 `sendEvent()` 发送事件后，
     * 事件最终会通过 `JobMasterOperatorEventGateway` 和 `TaskOperatorEventGateway` 传输到
     * 运行在 **TaskManager** 上的算子（Operator），并由该方法进行处理。
     *
     * <p>通常，该方法的实现需要根据事件类型执行相应的操作，例如：
     * <ul>
     *     <li>更新算子的运行参数，例如修改计算阈值。</li>
     *     <li>触发特定的业务逻辑，例如重启某些数据流。</li>
     *     <li>同步协调器的状态信息，例如获取 Checkpoint 状态。</li>
     * </ul>
     *
     * @param evt 收到的事件，通常是 `OperatorCoordinator` 发送的控制信号或状态更新。
     */
    void handleOperatorEvent(OperatorEvent evt);
}

