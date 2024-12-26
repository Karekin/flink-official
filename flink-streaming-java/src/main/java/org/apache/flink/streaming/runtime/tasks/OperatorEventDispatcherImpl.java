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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link OperatorEventDispatcher} 的实现类。
 *
 * <p>该类设计用于单线程使用，通过流任务的 mailbox 来调度。
 */
@Internal
public final class OperatorEventDispatcherImpl implements OperatorEventDispatcher {

    // 用于存储 OperatorID 和对应事件处理器的映射关系
    private final Map<OperatorID, OperatorEventHandler> handlers;

    // 类加载器，用于反序列化 OperatorEvent
    private final ClassLoader classLoader;

    // 用于与 JobMaster 通信的任务事件网关
    private final TaskOperatorEventGateway toCoordinator;

    /**
     * 构造函数。
     *
     * @param classLoader 用于反序列化 OperatorEvent 的类加载器
     * @param toCoordinator 与 JobMaster 通信的事件网关
     */
    public OperatorEventDispatcherImpl(
            ClassLoader classLoader, TaskOperatorEventGateway toCoordinator) {
        // 检查传入的参数是否为空，避免空指针异常
        this.classLoader = checkNotNull(classLoader);
        this.toCoordinator = checkNotNull(toCoordinator);
        // 初始化事件处理器的映射
        this.handlers = new HashMap<>();
    }

    /**
     * 分发事件给对应的事件处理器。
     *
     * @param operatorID 操作符的唯一标识符
     * @param serializedEvent 序列化的事件对象
     * @throws FlinkException 如果事件反序列化或分发失败
     */
    void dispatchEventToHandlers(
            OperatorID operatorID, SerializedValue<OperatorEvent> serializedEvent)
            throws FlinkException {
        // 定义 OperatorEvent 实例
        final OperatorEvent evt;
        try {
            // 使用类加载器反序列化事件对象
            evt = serializedEvent.deserializeValue(classLoader);
        } catch (IOException | ClassNotFoundException e) {
            // 抛出反序列化异常
            throw new FlinkException("Could not deserialize operator event", e);
        }

        // 根据 OperatorID 获取对应的事件处理器
        final OperatorEventHandler handler = handlers.get(operatorID);
        if (handler != null) {
            // 调用处理器的方法处理事件
            handler.handleOperatorEvent(evt);
        } else {
            // 抛出异常，表示未找到对应的处理器
            throw new FlinkException("Operator not registered for operator events");
        }
    }

    /**
     * 注册一个事件处理器。
     *
     * @param operator 操作符的唯一标识符
     * @param handler 对应的事件处理器
     */
    @Override
    public void registerEventHandler(OperatorID operator, OperatorEventHandler handler) {
        // 将处理器与操作符绑定，如果之前已存在处理器则抛出异常
        final OperatorEventHandler prior = handlers.putIfAbsent(operator, handler);
        if (prior != null) {
            throw new IllegalStateException("already a handler registered for this operatorId");
        }
    }

    /**
     * 获取所有已注册的操作符 ID 集合。
     *
     * @return 已注册操作符的集合
     */
    Set<OperatorID> getRegisteredOperators() {
        return handlers.keySet();
    }

    /**
     * 获取指定操作符的事件网关。
     *
     * @param operatorId 操作符的唯一标识符
     * @return 操作符事件网关实例
     */
    @Override
    public OperatorEventGateway getOperatorEventGateway(OperatorID operatorId) {
        // 返回网关实现类的实例
        return new OperatorEventGatewayImpl(toCoordinator, operatorId);
    }

    // ------------------------------------------------------------------------

    /**
     * 操作符事件网关的实现类，用于与 JobMaster 通信。
     */
    private static final class OperatorEventGatewayImpl implements OperatorEventGateway {

        // 用于与 JobMaster 通信的任务事件网关
        private final TaskOperatorEventGateway toCoordinator;

        // 关联的操作符 ID
        private final OperatorID operatorId;

        /**
         * 构造函数。
         *
         * @param toCoordinator 用于与 JobMaster 通信的事件网关
         * @param operatorId 操作符的唯一标识符
         */
        private OperatorEventGatewayImpl(
                TaskOperatorEventGateway toCoordinator, OperatorID operatorId) {
            this.toCoordinator = toCoordinator;
            this.operatorId = operatorId;
        }

        /**
         * 向 JobMaster 发送操作符事件。
         *
         * @param event 要发送的事件对象
         */
        @Override
        public void sendEventToCoordinator(OperatorEvent event) {
            // 定义序列化后的事件对象
            final SerializedValue<OperatorEvent> serializedEvent;
            try {
                // 将事件对象序列化
                serializedEvent = new SerializedValue<>(event);
            } catch (IOException e) {
                // 抛出运行时异常，表示序列化失败
                throw new FlinkRuntimeException("Cannot serialize operator event", e);
            }
            // 调用网关的方法将事件发送到 JobMaster
            toCoordinator.sendOperatorEventToCoordinator(operatorId, serializedEvent);
        }
    }
}
