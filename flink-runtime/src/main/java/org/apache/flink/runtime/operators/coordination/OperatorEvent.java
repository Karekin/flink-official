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

import java.io.Serializable;

/**
 * {@code OperatorEvent} 是所有在 {@link OperatorCoordinator}（算子协调器）
 * 和 {@link OperatorEventHandler}（算子事件处理器）之间传输的事件的根接口（Root Interface）。
 *
 * <p>在 Flink 的 **TaskManager**（任务管理器）和 **JobManager**（作业管理器）之间，
 * 任务执行过程中，算子（Operator）和协调器（Coordinator）可能需要进行通信，主要体现在：
 * <ul>
 *     <li>算子（Operator）向协调器（Coordinator）发送自定义事件，以通知状态变化或请求协调器执行某些逻辑。</li>
 *     <li>协调器（Coordinator）向算子（Operator）发送事件，以控制算子的行为或分发全局任务信息。</li>
 * </ul>
 *
 * <h2>事件传输链路</h2>
 * <p>事件在 Flink 集群内部的 **TaskManager** 和 **JobManager** 之间传递，具体链路如下：
 * <pre>
 *     1. 任务管理器（TaskManager）上的算子（Operator）希望通知协调器（Coordinator），
 *        于是它调用 {@link OperatorEventGateway#sendEventToCoordinator(OperatorEvent)} 发送事件。
 *     2. 事件经过 {@link TaskOperatorEventGateway} 处理，进一步封装信息并转发到：
 *     3. 任务管理器的 {@link JobMasterOperatorEventGateway} 通过 **RPC** 远程过程调用，
 *        将事件从 TaskManager 传输到 JobManager。
 *     4. JobManager 上的 **OperatorCoordinator** 通过
 *        {@link OperatorCoordinator#handleEventFromOperator(int, int, OperatorEvent)}
 *        方法处理该事件，并作出相应操作。
 *     5. 如果协调器需要向算子发送事件，它调用
 *        {@link OperatorCoordinator.Context#sendEvent(OperatorEvent)}。
 *     6. 事件再度经过 **JobMasterOperatorEventGateway** 和 **TaskOperatorEventGateway**，
 *        最终到达 TaskManager 的 **OperatorEventHandler**，由
 *        {@link OperatorEventHandler#handleOperatorEvent(OperatorEvent)} 进行处理。
 * </pre>
 *
 * <h2>实现要求</h2>
 * <p>所有的自定义事件都必须实现此接口，并且 **必须是可序列化的**，即实现 {@link Serializable} 接口，
 * 这样 Flink 才能正确地在不同组件（JobManager/TaskManager）之间传输该事件。</p>
 *
 * <h2>示例：如何自定义 OperatorEvent</h2>
 * <p>假设我们有一个自定义算子和协调器，希望算子可以向协调器汇报某些信息，我们可以定义如下事件：</p>
 *
 * <pre>{@code
 * public class MyCustomEvent implements OperatorEvent {
 *     private final String message;
 *
 *     public MyCustomEvent(String message) {
 *         this.message = message;
 *     }
 *
 *     public String getMessage() {
 *         return message;
 *     }
 * }
 * }</pre>
 *
 * <p>然后在算子内部，我们可以发送该事件：</p>
 *
 * <pre>{@code
 * operatorEventGateway.sendEventToCoordinator(new MyCustomEvent("Hello from operator!"));
 * }</pre>
 *
 * <p>而在协调器内部，我们可以处理该事件：</p>
 *
 * <pre>{@code
 * @Override
 * public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
 *     if (event instanceof MyCustomEvent) {
 *         String message = ((MyCustomEvent) event).getMessage();
 *         LOG.info("Received event from operator {}: {}", subtask, message);
 *     }
 * }
 * }</pre>
 *
 * <h2>事件流转总结</h2>
 * <p>{@code OperatorEvent} 的传输路径可以总结如下：</p>
 * <ul>
 *     <li>算子（Operator） → 事件网关（OperatorEventGateway） → TaskManager</li>
 *     <li>TaskManager → JobManager（通过 RPC 机制）</li>
 *     <li>JobManager → {@link OperatorCoordinator}（最终处理事件）</li>
 *     <li>如果有需要，{@code OperatorCoordinator} 也可以发送事件回 TaskManager。</li>
 * </ul>
 */
public interface OperatorEvent extends Serializable {}

