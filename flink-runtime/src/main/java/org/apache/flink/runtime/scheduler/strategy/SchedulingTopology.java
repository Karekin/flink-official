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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.runtime.topology.Topology;

import java.util.List;

/** Topology of {@link SchedulingExecutionVertex}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * SchedulingExecutionVertex 的拓步
*/
public interface SchedulingTopology
        extends Topology<
                ExecutionVertexID,
                IntermediateResultPartitionID,
                SchedulingExecutionVertex,
                SchedulingResultPartition,
                SchedulingPipelinedRegion> {

    /**
     * Looks up the {@link SchedulingExecutionVertex} for the given {@link ExecutionVertexID}.
     *
     * @param executionVertexId identifying the respective scheduling vertex
     * @return The respective scheduling vertex
     * @throws IllegalArgumentException If the vertex does not exist
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通过executionVertexId获取SchedulingExecutionVertex
    */
    SchedulingExecutionVertex getVertex(ExecutionVertexID executionVertexId);

    /**
     * Looks up the {@link SchedulingResultPartition} for the given {@link
     * IntermediateResultPartitionID}.
     *
     * @param intermediateResultPartitionId identifying the respective scheduling result partition
     * @return The respective scheduling result partition
     * @throws IllegalArgumentException If the partition does not exist
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通过intermediateResultPartitionId获得SchedulingResultPartition
    */
    SchedulingResultPartition getResultPartition(
            IntermediateResultPartitionID intermediateResultPartitionId);

    /**
     * Register a scheduling topology listener. The listener will be notified by {@link
     * SchedulingTopologyListener#notifySchedulingTopologyUpdated(SchedulingTopology, List)} when
     * the scheduling topology is updated.
     *
     * @param listener the registered listener.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 注册一个调度拓扑侦听器。
     * 每当更新调度拓扑时，都会通知此侦听器。
    */
    void registerSchedulingTopologyListener(SchedulingTopologyListener listener);
}
