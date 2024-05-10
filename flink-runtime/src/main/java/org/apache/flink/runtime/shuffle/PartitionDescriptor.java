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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Partition descriptor for {@link ShuffleMaster} to obtain {@link ShuffleDescriptor}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
*/
public class PartitionDescriptor implements Serializable {

    private static final long serialVersionUID = 6343547936086963705L;

    /** The ID of the result this partition belongs to. */
    //此分区所属结果的ID
    private final IntermediateDataSetID resultId;

    /** The total number of partitions for the result. */
    //结果的分区总数。
    private final int totalNumberOfPartitions;

    /** The ID of the partition. */
    //分区的ID。
    private final IntermediateResultPartitionID partitionId;

    /** The type of the partition. */
    //分区的类型
    private final ResultPartitionType partitionType;

    /** The number of subpartitions. */
    //子分区的数量。
    private final int numberOfSubpartitions;

    /** Connection index to identify this partition of intermediate result. */
    //连接索引以标识此分区的中间结果。
    private final int connectionIndex;

    /** Whether the intermediate result is a broadcast result. */
    //中间结果是否为广播结果。
    private final boolean isBroadcast;

    /**
     * Whether the distribution pattern of the intermediate result is {@link
     * DistributionPattern.ALL_TO_ALL}.
     */
    //中间结果的分布模式是否为DistributionPattern.ALL_TO_ALL
    private final boolean isAllToAllDistribution;

    private final boolean isNumberOfPartitionConsumerUndefined;

    @VisibleForTesting
    public PartitionDescriptor(
            IntermediateDataSetID resultId,
            int totalNumberOfPartitions,
            IntermediateResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numberOfSubpartitions,
            int connectionIndex,
            boolean isBroadcast,
            boolean isAllToAllDistribution,
            boolean isNumberOfPartitionConsumerUndefined) {
        this.resultId = checkNotNull(resultId);
        checkArgument(totalNumberOfPartitions >= 1);
        this.totalNumberOfPartitions = totalNumberOfPartitions;
        this.partitionId = checkNotNull(partitionId);
        this.partitionType = checkNotNull(partitionType);
        checkArgument(numberOfSubpartitions >= 1);
        this.numberOfSubpartitions = numberOfSubpartitions;
        this.connectionIndex = connectionIndex;
        this.isBroadcast = isBroadcast;
        this.isAllToAllDistribution = isAllToAllDistribution;
        this.isNumberOfPartitionConsumerUndefined = isNumberOfPartitionConsumerUndefined;
    }

    public IntermediateDataSetID getResultId() {
        return resultId;
    }

    public int getTotalNumberOfPartitions() {
        return totalNumberOfPartitions;
    }

    public IntermediateResultPartitionID getPartitionId() {
        return partitionId;
    }

    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    public int getNumberOfSubpartitions() {
        return numberOfSubpartitions;
    }

    public boolean isNumberOfPartitionConsumerUndefined() {
        return isNumberOfPartitionConsumerUndefined;
    }

    int getConnectionIndex() {
        return connectionIndex;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public boolean isAllToAllDistribution() {
        return isAllToAllDistribution;
    }

    @Override
    public String toString() {
        return String.format(
                "PartitionDescriptor [result id: %s, partition id: %s, partition type: %s, "
                        + "subpartitions: %d, connection index: %d, is broadcast: %s, "
                        + "is all-to-all distribution: %s]",
                resultId,
                partitionId,
                partitionType,
                numberOfSubpartitions,
                connectionIndex,
                isBroadcast,
                isAllToAllDistribution);
    }

    public static PartitionDescriptor from(IntermediateResultPartition partition) {
        checkNotNull(partition);

        IntermediateResult result = partition.getIntermediateResult();
        return new PartitionDescriptor(
                result.getId(),
                partition.getIntermediateResult().getNumberOfAssignedPartitions(),
                partition.getPartitionId(),
                result.getResultType(),
                partition.getNumberOfSubpartitions(),
                result.getConnectionIndex(),
                result.isBroadcast(),
                result.getConsumingDistributionPattern() == DistributionPattern.ALL_TO_ALL,
                partition.isNumberOfPartitionConsumersUndefined());
    }
}
