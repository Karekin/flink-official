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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * 表示 {@link NFA} 的一个状态。
 *
 * <p>每个状态由一个名称和状态类型标识。此外，它还包含一组状态转换。状态转换描述了在什么条件下可以进入新的状态。
 *
 * @param <T> 输入事件的类型
 */
public class State<T> implements Serializable {
    private static final long serialVersionUID = 6658700025989097781L;

    private final String name; // 状态名称
    private StateType stateType; // 状态类型
    private final Collection<StateTransition<T>> stateTransitions; // 状态转换集合

    /**
     * 构造函数，初始化状态名称和状态类型，并创建一个空的状态转换集合。
     *
     * @param name 状态的名称
     * @param stateType 状态的类型
     */
    public State(final String name, final StateType stateType) {
        this.name = name;
        this.stateType = stateType;
        stateTransitions = new ArrayList<>(); // 初始化状态转换集合为空
    }

    /**
     * 获取当前状态的状态类型。
     *
     * @return 当前状态的状态类型
     */
    public StateType getStateType() {
        return stateType;
    }

    /**
     * 判断当前状态是否为最终状态。
     *
     * @return 如果当前状态是最终状态，则返回 true；否则返回 false
     */
    public boolean isFinal() {
        return stateType == StateType.Final; // 判断状态是否为最终状态
    }

    /**
     * 判断当前状态是否为起始状态。
     *
     * @return 如果当前状态是起始状态，则返回 true；否则返回 false
     */
    public boolean isStart() {
        return stateType == StateType.Start; // 判断状态是否为起始状态
    }

    /**
     * 获取当前状态的名称。
     *
     * @return 当前状态的名称
     */
    public String getName() {
        return name;
    }

    /**
     * 获取当前状态的所有状态转换。
     *
     * @return 当前状态的状态转换集合
     */
    public Collection<StateTransition<T>> getStateTransitions() {
        return stateTransitions;
    }

    /**
     * 将当前状态设置为起始状态。
     */
    public void makeStart() {
        this.stateType = StateType.Start; // 设置当前状态为起始状态
    }

    /**
     * 添加一个状态转换，指定动作、目标状态和条件。
     *
     * @param action 状态转换的动作
     * @param targetState 转换到的目标状态
     * @param condition 触发状态转换的条件
     */
    public void addStateTransition(
            final StateTransitionAction action,
            final State<T> targetState,
            final IterativeCondition<T> condition) {
        stateTransitions.add(new StateTransition<T>(this, action, targetState, condition)); // 添加新的状态转换
    }

    /**
     * 添加一个忽略动作的状态转换，目标状态为当前状态，条件为指定条件。
     *
     * @param condition 触发忽略动作的条件
     */
    public void addIgnore(final IterativeCondition<T> condition) {
        addStateTransition(StateTransitionAction.IGNORE, this, condition); // 添加忽略动作的转换
    }

    /**
     * 添加一个忽略动作的状态转换，目标状态为指定状态，条件为指定条件。
     *
     * @param targetState 目标状态
     * @param condition 触发忽略动作的条件
     */
    public void addIgnore(final State<T> targetState, final IterativeCondition<T> condition) {
        addStateTransition(StateTransitionAction.IGNORE, targetState, condition); // 添加忽略动作的转换
    }

    /**
     * 添加一个取走动作的状态转换，目标状态为指定状态，条件为指定条件。
     *
     * @param targetState 目标状态
     * @param condition 触发取走动作的条件
     */
    public void addTake(final State<T> targetState, final IterativeCondition<T> condition) {
        addStateTransition(StateTransitionAction.TAKE, targetState, condition); // 添加取走动作的转换
    }

    /**
     * 添加一个继续执行动作的状态转换，目标状态为指定状态，条件为指定条件。
     *
     * @param targetState 目标状态
     * @param condition 触发继续执行动作的条件
     */
    public void addProceed(final State<T> targetState, final IterativeCondition<T> condition) {
        addStateTransition(StateTransitionAction.PROCEED, targetState, condition); // 添加继续执行动作的转换
    }

    /**
     * 添加一个取走动作的状态转换，目标状态为当前状态，条件为指定条件。
     *
     * @param condition 触发取走动作的条件
     */
    public void addTake(final IterativeCondition<T> condition) {
        addStateTransition(StateTransitionAction.TAKE, this, condition); // 添加取走动作的转换
    }

    /**
     * 重写 equals 方法，用于比较两个状态是否相等。
     *
     * @param obj 另一个对象
     * @return 如果两个状态相等，则返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof State) {
            @SuppressWarnings("unchecked")
            State<T> other = (State<T>) obj;

            // 比较状态名称、类型和状态转换集合
            return name.equals(other.name)
                    && stateType == other.stateType
                    && stateTransitions.equals(other.stateTransitions);
        } else {
            return false;
        }
    }

    /**
     * 重写 toString 方法，返回状态的字符串表示。
     *
     * @return 当前状态的字符串表示
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(stateType).append(" State ").append(name).append(" [\n");
        for (StateTransition<T> stateTransition : stateTransitions) {
            builder.append("\t").append(stateTransition).append(",\n");
        }
        builder.append("])");

        return builder.toString();
    }

    /**
     * 重写 hashCode 方法，用于计算状态的哈希值。
     *
     * @return 当前状态的哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, stateType, stateTransitions);
    }

    /**
     * 判断当前状态是否为停止状态。
     *
     * @return 如果当前状态是停止状态，则返回 true；否则返回 false
     */
    public boolean isStop() {
        return stateType == StateType.Stop; // 判断是否为停止状态
    }

    /**
     * 判断当前状态是否为挂起状态。
     *
     * @return 如果当前状态是挂起状态，则返回 true；否则返回 false
     */
    public boolean isPending() {
        return stateType == StateType.Pending; // 判断是否为挂起状态
    }

    /**
     * 状态类型的枚举集合。包含各种状态类型。
     */
    public enum StateType {
        Start, // 起始状态
        Final, // 最终状态
        Normal, // 普通状态，既不是起始状态也不是最终状态
        Pending, // 挂起状态，等待超时处理
        Stop // 停止状态
    }
}

