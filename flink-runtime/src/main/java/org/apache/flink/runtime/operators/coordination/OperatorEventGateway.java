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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;

/**
 * 该接口（OperatorEventGateway）是算子（Operator）向作业管理器（JobManager）上的
 * {@link OperatorCoordinator} 发送 {@link OperatorEvent} 事件的网关，是 Flink 任务中 算子与协调器交互的桥梁。
 *
 * <p>这个接口是从算子向协调器发送 Operator 事件的 **第一步**。
 * 在这个事件传递链条中，每一层都会添加更多的上下文信息，使得内部层无需知道完整的执行环境，
 * 从而降低耦合性，提高测试的可行性和模块化。
 *
 * <p>事件的传输链路如下：
 *
 * <pre>
 *     1. {@code OperatorEventGateway} 接口：接收算子发送的事件，并为事件附加 {@link OperatorID}，
 *        ，确保事件是从哪个算子发出的，然后将事件转发给 TaskOperatorEventGateway；
 *     2. {@link TaskOperatorEventGateway}：进一步为事件附加 {@link ExecutionAttemptID}，
 *        ，用于唯一标识当前任务执行实例（ExecutionAttempt），然后转发给 JobMasterOperatorEventGateway；
 *     3. {@link JobMasterOperatorEventGateway}：最终通过 RPC（远程过程调用）接口，将事件从
 *        任务管理器（TaskManager）发送到作业管理器（JobManager）。
 * </pre>
 *
 * <p>通过这种方式，事件的上下文逐层丰富，而底层组件（如 `OperatorCoordinator`）只需要关注
 * 处理事件的逻辑，而不需要知道事件是如何从 TaskManager 传输过来的。
 */
public interface OperatorEventGateway {

    /**
     * 向作业管理器（JobManager）上的协调器（OperatorCoordinator）发送一个事件（OperatorEvent）。
     *
     * <p>该事件最终会由 {@link OperatorCoordinator#handleEventFromOperator(int, int, OperatorEvent)}
     * 方法进行处理。
     *
     * @param event 要发送的 {@link OperatorEvent} 事件对象
     */
    void sendEventToCoordinator(OperatorEvent event);
}

