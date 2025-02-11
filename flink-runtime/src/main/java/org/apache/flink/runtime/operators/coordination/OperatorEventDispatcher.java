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

import org.apache.flink.runtime.jobgraph.OperatorID;


/**
 * {@code OperatorEventDispatcher} 是 **Operator 事件的分发器（Dispatcher）**，
 * 负责在 **TaskManager** 端的 **算子（Operator）** 与 **JobManager** 端的 **OperatorCoordinator** 之间
 * 进行事件传输。
 *
 * <p>主要功能：
 * <ul>
 *     <li>接收并处理从 **OperatorCoordinator** 发送到算子的 {@link OperatorEvent}。</li>
 *     <li>提供 **OperatorEventGateway**，允许算子向 **OperatorCoordinator** 发送事件。</li>
 * </ul>
 *
 * <h2>Flink 事件传输流程</h2>
 * <p>事件在 **TaskManager** 和 **JobManager** 之间流转，完整路径如下：</p>
 * <pre>
 * 1. JobManager 端的 `OperatorCoordinator` 需要通知算子某些信息，
 *    于是调用 `OperatorCoordinator.Context#sendEvent(OperatorEvent)` 发送事件。
 * 2. 事件经过 `JobMasterOperatorEventGateway` 传输至 TaskManager。
 * 3. TaskManager 通过 `TaskOperatorEventGateway` 传递事件给 `OperatorEventDispatcher`。
 * 4. `OperatorEventDispatcher` 负责将事件 **分发** 给对应的算子（Operator）。
 * 5. 算子内部的 `OperatorEventHandler` 处理该事件。
 * </pre>
 *
 * <h2>应用场景</h2>
 * <p>`OperatorEventDispatcher` 主要用于算子（Operator）和协调器（Coordinator）之间的 **事件交互**，例如：</p>
 * <ul>
 *     <li>动态调整算子参数，例如算子可以从 `OperatorCoordinator` 接收新的处理规则。</li>
 *     <li>算子可以向 `OperatorCoordinator` 发送统计信息，用于全局调度优化。</li>
 *     <li>算子可以接收 `OperatorCoordinator` 的全局控制指令，例如暂停/恢复计算。</li>
 * </ul>
 *
 * <h2>示例代码</h2>
 * <p>假设我们有一个自定义的 `MyKeyedProcessOperator` 需要注册 `OperatorEventHandler`：</p>
 *
 * <pre>{@code
 * public class MyKeyedProcessOperator<KEY, IN, OUT> extends AbstractStreamOperator<OUT>
 *         implements OneInputStreamOperator<IN, OUT>, OperatorEventHandler {
 *
 *     private final OperatorEventGateway operatorEventGateway;
 *
 *     public MyKeyedProcessOperator(OperatorEventGateway operatorEventGateway) {
 *         this.operatorEventGateway = operatorEventGateway;
 *     }
 *
 *     @Override
 *     public void open() {
 *         getContainingTask()
 *                 .getEnvironment()
 *                 .getOperatorEventDispatcher()
 *                 .registerEventHandler(getOperatorID(), this);
 *     }
 *
 *     @Override
 *     public void handleOperatorEvent(OperatorEvent evt) {
 *         LOG.info("Received Operator Event: {}", evt);
 *     }
 * }
 * }</pre>
 *
 * <p>在 `open()` 方法中，我们通过 `OperatorEventDispatcher` **注册算子事件处理器**，
 * 这样 `OperatorEventHandler` 就可以监听来自 `OperatorCoordinator` 的事件。</p>
 */
public interface OperatorEventDispatcher {

    /**
     * 注册事件处理器（EventHandler），用于接收 **OperatorCoordinator** 发送到算子的事件。
     *
     * <p>当 `OperatorCoordinator` 调用 `sendEvent()` 发送事件后，事件会经过
     * `JobMasterOperatorEventGateway` 和 `TaskOperatorEventGateway`，最终由
     * `OperatorEventDispatcher` 分发到 **对应算子的 `OperatorEventHandler`** 进行处理。</p>
     *
     * <h3>示例：在算子中注册事件处理器</h3>
     * <pre>{@code
     * public void open() {
     *     getContainingTask()
     *             .getEnvironment()
     *             .getOperatorEventDispatcher()
     *             .registerEventHandler(getOperatorID(), this);
     * }
     * }</pre>
     *
     * <p>这样，当 `OperatorCoordinator` 发送事件时，该算子就能接收并处理。</p>
     *
     * @param operator 目标算子的 {@link OperatorID}，用于唯一标识算子。
     * @param handler  事件处理器（算子），实现 {@link OperatorEventHandler} 接口。
     */
    void registerEventHandler(OperatorID operator, OperatorEventHandler handler);

    /**
     * 获取 **OperatorEventGateway**，用于将事件从 **算子** 发送到 **OperatorCoordinator**。
     *
     * <p>算子可以通过 `OperatorEventGateway.sendEventToCoordinator()` 方法主动向 `OperatorCoordinator`
     * 发送事件。例如，算子可以向协调器汇报统计数据，或者请求某些配置参数。</p>
     *
     * <h3>示例：算子向 `OperatorCoordinator` 发送事件</h3>
     * <pre>{@code
     * OperatorEventGateway eventGateway =
     *         getContainingTask()
     *                 .getEnvironment()
     *                 .getOperatorEventDispatcher()
     *                 .getOperatorEventGateway(getOperatorID());
     *
     * eventGateway.sendEventToCoordinator(new MyCustomEvent("Task completed"));
     * }</pre>
     *
     * <p>这样，事件就会通过 `OperatorEventGateway` 传输到 `OperatorCoordinator`。</p>
     *
     * @param operatorId 目标 `OperatorCoordinator` 的 {@link OperatorID}。
     * @return 事件网关 {@link OperatorEventGateway}，用于算子向 `OperatorCoordinator` 发送事件。
     */
    OperatorEventGateway getOperatorEventGateway(OperatorID operatorId);
}

