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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * 用于存储 NFA（非确定性有限自动机）状态的类。
 *
 * 该类用于维护 NFA 的当前匹配状态，包括正在匹配的中间状态 (`partialMatches`) 和已完成匹配的状态 (`completedMatches`)。
 * 它提供了方法来检查状态是否发生变化，并允许 NFA 进行状态更新。
 */
public class NFAState {

    /**
     * 维护 NFA 计算过程中的 "部分匹配状态"（partial matches）。
     *
     * 这些状态表示已经匹配了部分模式的事件，但仍需等待新的事件来完成匹配。
     * partialMatches 存储的是可以继续匹配的状态，而不是简单的“未完成匹配”状态。
     * 该队列必须包含至少一个起始 ComputationState，以确保模式匹配能够从起始状态开始。
     *
     * 当 NFAState 初始化时，会扫描所有 State，将可以作为起点的 State 转换成 ComputationState 并存入 partialMatches。
     * 未来新的事件到来时，会在 partialMatches 中寻找匹配状态，如果匹配成功，则表明当前事件属于同一模式匹配的序列。
     */

    private Queue<ComputationState> partialMatches;

    /**
     * 维护 NFA 计算过程中"已完成的匹配状态"（completed matches）。
     *
     * 这些状态表示已经完成匹配的事件序列，可以输出到下游算子。
     */
    private Queue<ComputationState> completedMatches;

    /**
     * 标记 NFA 状态是否发生变化。
     *
     * 该标志用于优化计算，避免不必要的状态更新，提高性能。
     */
    private boolean stateChanged;

    /**
     * 标记是否出现新的部分匹配。
     *
     * 该标志用于记录是否有新的部分匹配被创建，在某些情况下用于触发额外逻辑处理。
     */
    private boolean isNewStartPartialMatch;

    /**
     * 计算状态 (`ComputationState`) 的比较器，按照匹配开始时间和事件 ID 进行排序。
     *
     * 主要用于优先队列（PriorityQueue），以确保优先处理最早匹配的事件序列。
     */
    public static final Comparator<ComputationState> COMPUTATION_STATE_COMPARATOR =
            Comparator.<ComputationState>comparingLong(
                            c ->
                                    c.getStartEventID() != null
                                            ? c.getStartEventID().getTimestamp()
                                            : Long.MAX_VALUE)
                    .thenComparingInt(
                            c ->
                                    c.getStartEventID() != null
                                            ? c.getStartEventID().getId()
                                            : Integer.MAX_VALUE);

    /**
     * 构造函数，初始化 `partialMatches` 并存入初始状态集合。
     *
     * @param states 初始状态集合
     */
    public NFAState(Iterable<ComputationState> states) {
        this.partialMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
        for (ComputationState startingState : states) {
            partialMatches.add(startingState);
        }
        this.completedMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
    }

    /**
     * 构造函数，直接使用提供的 `partialMatches` 和 `completedMatches` 队列初始化 NFA 状态。
     *
     * @param partialMatches  部分匹配的状态队列
     * @param completedMatches 已完成匹配的状态队列
     */
    public NFAState(
            Queue<ComputationState> partialMatches, Queue<ComputationState> completedMatches) {
        this.partialMatches = partialMatches;
        this.completedMatches = completedMatches;
    }

    /**
     * 检查 NFA 的匹配状态是否发生变化。
     *
     * @return 如果匹配状态发生变化，则返回 `true`，否则返回 `false`
     */
    public boolean isStateChanged() {
        return stateChanged;
    }

    /**
     * 重置 `stateChanged` 标志位，表示状态未发生变化。
     */
    public void resetStateChanged() {
        this.stateChanged = false;
    }

    /**
     * 设置 `stateChanged` 标志位，表示状态发生了变化。
     */
    public void setStateChanged() {
        this.stateChanged = true;
    }

    /**
     * 获取部分匹配的状态队列。
     *
     * @return `partialMatches` 队列
     */
    public Queue<ComputationState> getPartialMatches() {
        return partialMatches;
    }

    /**
     * 获取已完成匹配的状态队列。
     *
     * @return `completedMatches` 队列
     */
    public Queue<ComputationState> getCompletedMatches() {
        return completedMatches;
    }

    /**
     * 设置新的部分匹配状态队列，替换现有的 `partialMatches`。
     *
     * @param newPartialMatches 新的部分匹配状态队列
     */
    public void setNewPartialMatches(PriorityQueue<ComputationState> newPartialMatches) {
        this.partialMatches = newPartialMatches;
    }

    /**
     * 检查是否有新的部分匹配状态。
     *
     * @return 如果存在新的部分匹配状态，则返回 `true`，否则返回 `false`
     */
    public boolean isNewStartPartialMatch() {
        return isNewStartPartialMatch;
    }

    /**
     * 重置 `isNewStartPartialMatch` 标志位，表示没有新的部分匹配状态。
     */
    public void resetNewStartPartialMatch() {
        this.isNewStartPartialMatch = false;
    }

    /**
     * 设置 `isNewStartPartialMatch` 标志位，表示有新的部分匹配状态。
     */
    public void setNewStartPartiailMatch() {
        this.isNewStartPartialMatch = true;
    }

    /**
     * 重写 `equals` 方法，比较两个 `NFAState` 是否相等。
     *
     * @param o 另一个 `NFAState` 实例
     * @return 如果 `partialMatches` 和 `completedMatches` 内容完全相同，则返回 `true`，否则返回 `false`
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NFAState nfaState = (NFAState) o;
        return Arrays.equals(partialMatches.toArray(), nfaState.partialMatches.toArray())
                && Arrays.equals(completedMatches.toArray(), nfaState.completedMatches.toArray());
    }

    /**
     * 计算 `NFAState` 的哈希值，基于 `partialMatches` 和 `completedMatches` 计算。
     *
     * @return 计算得到的哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(partialMatches, completedMatches);
    }

    /**
     * 返回 `NFAState` 的字符串表示，包含 `partialMatches`、`completedMatches` 和 `stateChanged` 信息。
     *
     * @return `NFAState` 的字符串表示
     */
    @Override
    public String toString() {
        return "NFAState{"
                + "partialMatches=" + partialMatches
                + ", completedMatches=" + completedMatches
                + ", stateChanged=" + stateChanged
                + '}';
    }
}

