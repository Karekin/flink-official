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

package org.apache.flink.table.planner.plan.metadata;

import org.apache.flink.table.planner.plan.stats.ValueInterval;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.RelModifiedMonotonicity;
import org.apache.flink.table.planner.plan.trait.RelWindowProperties;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Arrays;
import java.util.Set;

/**
 * FlinkRelMetadataQuery 是 Flink 扩展的 RelMetadataQuery 类，
 * 主要用于获取 Flink 运行时 SQL 优化所需的元信息，例如：
 * - 列值范围（ColumnInterval）
 * - 过滤后的列值范围（FilteredColumnInterval）
 * - 列的空值统计（ColumnNullCount）
 * - 列的原始空值统计（ColumnOriginNullCount）
 * - 唯一键分组（UniqueGroups）
 * - 分布信息（FlinkDistribution）
 * - 修改的单调性（ModifiedMonotonicity）
 * - 窗口属性（WindowProperties）
 * - Upsert 关键字（UpsertKeys）
 */
public class FlinkRelMetadataQuery extends RelMetadataQuery {

    // 静态 Handlers 实例，所有 FlinkRelMetadataQuery 实例都会共享
    private static final Handlers HANDLERS = new Handlers();

    // 各种元信息查询处理器（Handler），用于计算对应的元数据信息
    private FlinkMetadata.ColumnInterval.Handler columnIntervalHandler;
    private FlinkMetadata.FilteredColumnInterval.Handler filteredColumnInterval;
    private FlinkMetadata.ColumnNullCount.Handler columnNullCountHandler;
    private FlinkMetadata.ColumnOriginNullCount.Handler columnOriginNullCountHandler;
    private FlinkMetadata.UniqueGroups.Handler uniqueGroupsHandler;
    private FlinkMetadata.FlinkDistribution.Handler distributionHandler;
    private FlinkMetadata.ModifiedMonotonicity.Handler modifiedMonotonicityHandler;
    private FlinkMetadata.WindowProperties.Handler windowPropertiesHandler;
    private FlinkMetadata.UpsertKeys.Handler upsertKeysHandler;

    /**
     * 获取 FlinkRelMetadataQuery 实例。
     * 该方法确保元数据计算过程中不会出现循环调用问题。
     *
     * @return FlinkRelMetadataQuery 的新实例
     */
    public static FlinkRelMetadataQuery instance() {
        return new FlinkRelMetadataQuery();
    }

    /**
     * 复用已有的 RelMetadataQuery 实例，如果 mq 本身是 FlinkRelMetadataQuery 类型，则直接返回；
     * 否则，创建一个新的 FlinkRelMetadataQuery 实例。
     *
     * @param mq 需要复用的元数据查询实例
     * @return FlinkRelMetadataQuery 实例
     */
    public static FlinkRelMetadataQuery reuseOrCreate(RelMetadataQuery mq) {
        if (mq instanceof FlinkRelMetadataQuery) {
            return (FlinkRelMetadataQuery) mq;
        } else {
            return instance();
        }
    }

    /**
     * 私有构造函数，初始化所有元数据查询处理器（Handler）。
     * 由于 FlinkRelMetadataQuery 只能通过静态方法创建，因此构造函数被设置为 private。
     */
    private FlinkRelMetadataQuery() {
        this.columnIntervalHandler = HANDLERS.columnIntervalHandler;
        this.filteredColumnInterval = HANDLERS.filteredColumnInterval;
        this.columnNullCountHandler = HANDLERS.columnNullCountHandler;
        this.columnOriginNullCountHandler = HANDLERS.columnOriginNullCountHandler;
        this.uniqueGroupsHandler = HANDLERS.uniqueGroupsHandler;
        this.distributionHandler = HANDLERS.distributionHandler;
        this.modifiedMonotonicityHandler = HANDLERS.modifiedMonotonicityHandler;
        this.windowPropertiesHandler = HANDLERS.windowPropertiesHandler;
        this.upsertKeysHandler = HANDLERS.upsertKeysHandler;
    }

    /**
     * 内部静态类 Handlers，包含所有元数据查询处理器的初始实例。
     * 这些处理器会在 FlinkRelMetadataQuery 创建时进行赋值，避免重复创建对象，提高性能。
     */
    private static class Handlers {
        private FlinkMetadata.ColumnInterval.Handler columnIntervalHandler =
                initialHandler(FlinkMetadata.ColumnInterval.Handler.class);
        private FlinkMetadata.FilteredColumnInterval.Handler filteredColumnInterval =
                initialHandler(FlinkMetadata.FilteredColumnInterval.Handler.class);
        private FlinkMetadata.ColumnNullCount.Handler columnNullCountHandler =
                initialHandler(FlinkMetadata.ColumnNullCount.Handler.class);
        private FlinkMetadata.ColumnOriginNullCount.Handler columnOriginNullCountHandler =
                initialHandler(FlinkMetadata.ColumnOriginNullCount.Handler.class);
        private FlinkMetadata.UniqueGroups.Handler uniqueGroupsHandler =
                initialHandler(FlinkMetadata.UniqueGroups.Handler.class);
        private FlinkMetadata.FlinkDistribution.Handler distributionHandler =
                initialHandler(FlinkMetadata.FlinkDistribution.Handler.class);
        private FlinkMetadata.ModifiedMonotonicity.Handler modifiedMonotonicityHandler =
                initialHandler(FlinkMetadata.ModifiedMonotonicity.Handler.class);
        private FlinkMetadata.WindowProperties.Handler windowPropertiesHandler =
                initialHandler(FlinkMetadata.WindowProperties.Handler.class);
        private FlinkMetadata.UpsertKeys.Handler upsertKeysHandler =
                initialHandler(FlinkMetadata.UpsertKeys.Handler.class);
    }

    /**
     * 获取指定列的值区间（Column Interval）。
     *
     * @param rel 关系表达式（RelNode）
     * @param index 目标列的索引
     * @return 该列的值区间，可能返回 null 或空值区间（EmptyValueInterval）
     */
    public ValueInterval getColumnInterval(RelNode rel, int index) {
        for (; ; ) {
            try {
                return columnIntervalHandler.getColumnInterval(rel, this, index);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 如果没有处理器，重新生成并绑定
                columnIntervalHandler = revise(e.relClass, FlinkMetadata.ColumnInterval.DEF);
            }
        }
    }

    /**
     * 获取应用了过滤条件的列值区间（Filtered Column Interval）。
     *
     * @param rel 关系表达式（RelNode）
     * @param columnIndex 目标列的索引
     * @param filterArg 过滤条件参数索引
     * @return 该列的值区间，可能返回 null 或空值区间（EmptyValueInterval）
     */
    public ValueInterval getFilteredColumnInterval(RelNode rel, int columnIndex, int filterArg) {
        for (; ; ) {
            try {
                return filteredColumnInterval.getFilteredColumnInterval(
                        rel, this, columnIndex, filterArg);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 重新生成处理器
                filteredColumnInterval = revise(e.relClass, FlinkMetadata.FilteredColumnInterval.DEF);
            }
        }
    }


    /**
     * 获取指定列的空值数量 (Null Count)。
     *
     * @param rel 关系表达式（RelNode）
     * @param index 目标列的索引
     * @return 该列的空值数量，如果无法估算，则返回 null。
     */
    public Double getColumnNullCount(RelNode rel, int index) {
        for (; ; ) {
            try {
                return columnNullCountHandler.getColumnNullCount(rel, this, index);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 如果当前 Handler 为空，则重新生成一个 Handler
                columnNullCountHandler = revise(e.relClass, FlinkMetadata.ColumnNullCount.DEF);
            }
        }
    }

    /**
     * 获取原始数据中的指定列的空值数量 (Origin Null Count)。
     *
     * @param rel 关系表达式（RelNode）
     * @param index 目标列的索引
     * @return 该列的原始空值数量，如果无法估算，则返回 null。
     */
    public Double getColumnOriginNullCount(RelNode rel, int index) {
        for (; ; ) {
            try {
                return columnOriginNullCountHandler.getColumnOriginNullCount(rel, this, index);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 重新生成 ColumnOriginNullCount Handler
                columnOriginNullCountHandler = revise(e.relClass, FlinkMetadata.ColumnOriginNullCount.DEF);
            }
        }
    }

    /**
     * 获取指定列的唯一分组 (Minimum Unique Groups)。
     * 该方法用于计算最小唯一列集合，即使部分列组合可以唯一标识数据，也会返回该最小子集。
     *
     * @param rel 关系表达式（RelNode）
     * @param columns 需要计算唯一性的列集合，不能为 null。
     * @return 该列集合中的最小唯一分组，如果找不到唯一列，则返回原始列集合。
     */
    public ImmutableBitSet getUniqueGroups(RelNode rel, ImmutableBitSet columns) {
        for (; ; ) {
            try {
                // 确保输入列集合不为空
                Preconditions.checkArgument(columns != null);
                if (columns.isEmpty()) {
                    return columns;
                }
                ImmutableBitSet uniqueGroups = uniqueGroupsHandler.getUniqueGroups(rel, this, columns);
                // 确保唯一列不为空，且是输入列集合的子集
                Preconditions.checkArgument(uniqueGroups != null && !uniqueGroups.isEmpty());
                Preconditions.checkArgument(columns.contains(uniqueGroups));
                return uniqueGroups;
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 重新生成 UniqueGroups Handler
                uniqueGroupsHandler = revise(e.relClass, FlinkMetadata.UniqueGroups.DEF);
            }
        }
    }

    /**
     * 获取关系表达式的 Flink 物理分布信息 (FlinkRelDistribution)。
     *
     * @param rel 关系表达式（RelNode）
     * @return 物理分布信息，例如 Hash 分区、广播、单一分区等。
     */
    public FlinkRelDistribution flinkDistribution(RelNode rel) {
        for (; ; ) {
            try {
                return distributionHandler.flinkDistribution(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 重新生成 FlinkDistribution Handler
                distributionHandler = revise(e.relClass, FlinkMetadata.FlinkDistribution.DEF);
            }
        }
    }

    /**
     * 获取关系表达式的单调性 (Modified Monotonicity)。
     * 单调性描述了数据如何变化，例如是否是严格递增、递减或无序等。
     *
     * @param rel 关系表达式（RelNode）
     * @return 该 `RelNode` 的单调性信息。
     */
    public RelModifiedMonotonicity getRelModifiedMonotonicity(RelNode rel) {
        for (; ; ) {
            try {
                return modifiedMonotonicityHandler.getRelModifiedMonotonicity(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 重新生成 ModifiedMonotonicity Handler
                modifiedMonotonicityHandler = revise(e.relClass, FlinkMetadata.ModifiedMonotonicity.DEF);
            }
        }
    }

    /**
     * 获取关系表达式的窗口属性 (RelWindowProperties)。
     * 该方法用于查询窗口计算时的窗口边界、分区键等信息。
     *
     * @param rel 关系表达式（RelNode）
     * @return 窗口属性信息，例如窗口大小、滑动步长等。
     */
    public RelWindowProperties getRelWindowProperties(RelNode rel) {
        for (; ; ) {
            try {
                return windowPropertiesHandler.getWindowProperties(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 重新生成 WindowProperties Handler
                windowPropertiesHandler = revise(e.relClass, FlinkMetadata.WindowProperties.DEF);
            }
        }
    }

    /**
     * 获取该关系表达式的 Upsert 关键列集合。
     * Upsert 键是用于唯一标识数据的键集合，类似于主键，但在流式环境中可能随数据变化。
     *
     * @param rel 关系表达式（RelNode）
     * @return 该 `RelNode` 的 Upsert 关键列集合，如果无法确定，则返回 null。
     */
    public Set<ImmutableBitSet> getUpsertKeys(RelNode rel) {
        for (; ; ) {
            try {
                return upsertKeysHandler.getUpsertKeys(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                // 重新生成 UpsertKeys Handler
                upsertKeysHandler = revise(e.relClass, FlinkMetadata.UpsertKeys.DEF);
            }
        }
    }

    /**
     * 获取在单个 Key Group 范围内的 Upsert 关键列集合。
     * 该方法可以忽略分区键 (Partition Keys) 所造成的影响，仅在本地范围内计算 Upsert Key。
     *
     * <p> 该方法主要用于优化器在分布式环境下进行 Upsert 操作时，确定 Key Group 内唯一标识数据的最小列集合。
     *
     * @param rel 关系表达式（RelNode）
     * @param partitionKeys 参与分区的列索引数组
     * @return 该 `RelNode` 在指定 Key Group 内的 Upsert 关键列集合。
     */
    public Set<ImmutableBitSet> getUpsertKeysInKeyGroupRange(RelNode rel, int[] partitionKeys) {
        // 检查当前 RelNode 是否是 Exchange 类型（表示分区交换）
        if (rel instanceof Exchange) {
            Exchange exchange = (Exchange) rel;
            // 如果 Exchange 分区键与指定 partitionKeys 相同，则忽略 Exchange 层，直接获取其输入的 Upsert Keys
            if (Arrays.equals(
                    exchange.getDistribution().getKeys().stream()
                            .mapToInt(Integer::intValue)
                            .toArray(),
                    partitionKeys)) {
                rel = exchange.getInput();
            }
        }
        return getUpsertKeys(rel);
    }
}
