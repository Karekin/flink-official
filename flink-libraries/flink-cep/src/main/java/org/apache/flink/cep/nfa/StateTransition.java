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

package org.apache.flink.cep.nfa;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.io.Serializable;
import java.util.Objects;

/**
 * 表示从一个 {@link State} 到另一个 {@link State} 的状态转换。
 *
 * @param <T> 事件的类型，事件由 {@link IterativeCondition} 处理
 */
public class StateTransition<T> implements Serializable {
    private static final long serialVersionUID = -4825345749997891838L;

    private final StateTransitionAction action; // 状态转换的动作（例如，取走、继续等）
    private final State<T> sourceState; // 源状态
    private final State<T> targetState; // 目标状态
    private IterativeCondition<T> condition; // 状态转换的条件

    /**
     * 构造函数，用于创建一个状态转换。
     *
     * @param sourceState 源状态
     * @param action 状态转换的动作
     * @param targetState 目标状态
     * @param condition 状态转换的条件
     */
    public StateTransition(
            final State<T> sourceState,
            final StateTransitionAction action,
            final State<T> targetState,
            final IterativeCondition<T> condition) {
        this.action = action; // 设置动作
        this.targetState = targetState; // 设置目标状态
        this.sourceState = sourceState; // 设置源状态
        this.condition = condition; // 设置状态转换的条件
    }

    /**
     * 获取状态转换的动作。
     *
     * @return 状态转换的动作
     */
    public StateTransitionAction getAction() {
        return action;
    }

    /**
     * 获取目标状态。
     *
     * @return 目标状态
     */
    public State<T> getTargetState() {
        return targetState;
    }

    /**
     * 获取源状态。
     *
     * @return 源状态
     */
    public State<T> getSourceState() {
        return sourceState;
    }

    /**
     * 获取状态转换的条件。
     *
     * @return 状态转换的条件
     */
    public IterativeCondition<T> getCondition() {
        return condition;
    }

    /**
     * 设置新的状态转换条件。
     *
     * @param condition 新的状态转换条件
     */
    public void setCondition(IterativeCondition<T> condition) {
        this.condition = condition;
    }

    /**
     * 重写 equals 方法，用于比较两个状态转换是否相等。
     *
     * @param obj 另一个对象
     * @return 如果两个状态转换相等，则返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StateTransition) {
            @SuppressWarnings("unchecked")
            StateTransition<T> other = (StateTransition<T>) obj;

            // 比较动作、源状态和目标状态的名称是否相同
            return action == other.action
                    && sourceState.getName().equals(other.sourceState.getName())
                    && targetState.getName().equals(other.targetState.getName());
        } else {
            return false;
        }
    }

    /**
     * 重写 hashCode 方法，用于计算状态转换的哈希值。
     *
     * @return 状态转换的哈希值
     */
    @Override
    public int hashCode() {
        // 我们必须使用目标状态的名称，因为状态转换可能是自反的（源状态和目标状态相同）
        return Objects.hash(action, targetState.getName(), sourceState.getName());
    }

    /**
     * 重写 toString 方法，返回状态转换的字符串表示。
     *
     * @return 状态转换的字符串表示
     */
    @Override
    public String toString() {
        return new StringBuilder()
                .append("StateTransition(")
                .append(action) // 添加动作
                .append(", ")
                .append("from ")
                .append(sourceState.getName()) // 添加源状态名称
                .append(" to ")
                .append(targetState.getName()) // 添加目标状态名称
                .append(condition != null ? ", with condition)" : ")") // 如果有条件，添加条件信息
                .toString();
    }
}

