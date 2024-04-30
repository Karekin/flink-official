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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Component to retrieve the inputs locations of an {@link ExecutionVertex}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于检索 ExecutionVertex 的输入位置的组件
*/
public interface InputsLocationsRetriever {

    /**
     * Get the consumed result partition groups of an execution vertex.
     *
     * @param executionVertexId identifies the execution vertex
     * @return the consumed result partition groups
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取执行顶点的消耗结果分区组。
    */
    Collection<ConsumedPartitionGroup> getConsumedPartitionGroups(
            ExecutionVertexID executionVertexId);

    /**
     * Get the producer execution vertices of a consumed result partition group.
     *
     * @param consumedPartitionGroup the consumed result partition group
     * @return the ids of producer execution vertices
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取已消耗结果分区组的生产者执行顶点。
    */
    Collection<ExecutionVertexID> getProducersOfConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup);

    /**
     * Get the task manager location future for an execution vertex.
     *
     * @param executionVertexId identifying the execution vertex
     * @return the task manager location future
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取执行顶点的任务管理器位置future。
    */
    Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
            ExecutionVertexID executionVertexId);
}
