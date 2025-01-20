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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/**
 * 用于 group by（无窗口）聚合的函数。
 * 实现了基于 Key 的聚合功能，支持增量计算和撤销消息处理。
 */
public class GroupAggFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final long serialVersionUID = -4767158666069797704L;

    /**
     * 用于处理聚合逻辑的代码生成函数。
     * 通过动态代码生成（CodeGen）实现高效的聚合计算。
     */
    private final GeneratedAggsHandleFunction genAggsHandler;

    /**
     * 用于比较 RowData 是否相等的代码生成函数。
     * 在判断聚合结果是否变化时使用。
     */
    private final GeneratedRecordEqualiser genRecordEqualiser;

    /**
     * 累加器的类型（Accumulator Types）。
     * 累加器用于存储中间聚合结果。
     */
    private final LogicalType[] accTypes;

    /**
     * 用于记录输入记录的新增和撤销计数器。
     * 该计数器用于跟踪当前 Key 下的记录数。
     */
    private final RecordCounter recordCounter;

    /**
     * 是否生成 UPDATE_BEFORE 消息。
     * UPDATE_BEFORE 消息表示在更新一条记录之前输出旧值。
     */
    private final boolean generateUpdateBefore;

    /**
     * 状态空闲保留时间（单位：毫秒）。
     * 用于控制状态的生命周期，避免状态无限制地增长。
     */
    private final long stateRetentionTime;

    /**
     * 用于重用的输出行对象。
     * 避免在每次输出时创建新对象，提高性能。
     */
    private transient JoinedRowData resultRow = null;

    /**
     * 用于处理聚合逻辑的函数实例。
     * 在运行时由代码生成函数动态实例化。
     */
    private transient AggsHandleFunction function = null;

    /**
     * 用于比较 RowData 的函数实例。
     * 判断两个 RowData 是否相等。
     */
    private transient RecordEqualiser equaliser = null;

    /**
     * 用于存储累加器状态的 ValueState。
     * 每个 Key 对应一个状态，存储其聚合结果的累加器。
     */
    private transient ValueState<RowData> accState = null;

    /**
     * 构造函数，创建一个 {@link GroupAggFunction}。
     *
     * @param genAggsHandler 用于处理聚合逻辑的代码生成函数。
     *                       该函数通过动态代码生成高效处理聚合操作。
     * @param genRecordEqualiser 用于比较 RowData 是否相等的代码生成函数。
     *                           用于判断新旧聚合结果是否发生变化。
     * @param accTypes 累加器（Accumulator）的数据类型。
     *                 累加器用于存储中间的聚合结果。
     * @param indexOfCountStar 聚合中 COUNT(*) 的索引位置。
     *                         如果输入数据中不包含 COUNT(*) 操作，值为 -1。
     *                         当输入流包含撤销消息（Retraction Messages）时，确保存在 COUNT(*)。
     * @param generateUpdateBefore 是否生成 UPDATE_BEFORE 消息。
     *                              UPDATE_BEFORE 消息表示在更新记录之前输出旧值。
     * @param stateRetentionTime 状态的空闲保留时间（单位：毫秒）。
     *                           用于控制状态的生命周期，避免状态无限制增长。
     */
    public GroupAggFunction(
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            LogicalType[] accTypes,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long stateRetentionTime) {
        this.genAggsHandler = genAggsHandler;
        this.genRecordEqualiser = genRecordEqualiser;
        this.accTypes = accTypes;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateRetentionTime = stateRetentionTime;
    }

    /**
     * 初始化方法，Flink 框架在算子启动时调用。
     *
     * @param openContext 上下文对象，提供运行时信息。
     * @throws Exception 如果初始化失败抛出异常。
     */
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        // 创建状态 TTL 配置
        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);

        // 实例化聚合处理函数（AggsHandleFunction）
        // 动态生成的代码实例化时会使用运行时的类加载器
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        // 初始化聚合处理函数并绑定每个 Key 的状态存储
        function.open(new PerKeyStateDataViewStore(getRuntimeContext(), ttlConfig));

        // 实例化比较函数（RecordEqualiser）
        equaliser = genRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // 定义累加器的状态描述符
        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);

        // 如果 TTL 配置启用，为状态启用空闲过期（Time-to-Live）
        if (ttlConfig.isEnabled()) {
            accDesc.enableTimeToLive(ttlConfig);
        }

        // 初始化 ValueState，用于存储每个 Key 的累加器状态
        accState = getRuntimeContext().getState(accDesc);

        // 初始化重用的输出行对象
        resultRow = new JoinedRowData();
    }


    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        // 获取当前 Key
        RowData currentKey = ctx.getCurrentKey();
        boolean firstRow; // 标记是否为第一条记录
        // 获取当前 Key 的累加器状态
        RowData accumulators = accState.value();

        if (null == accumulators) {
            // 如果累加器为空，且输入为撤销消息（Retraction Message），直接返回
            // 这种情况可能发生在撤销消息是此 Key 的第一条消息，或状态已被清理后收到撤销消息
            if (isRetractMsg(input)) {
                return;
            }
            // 初始化累加器
            firstRow = true;
            accumulators = function.createAccumulators();
        } else {
            // 累加器存在，说明不是第一条记录
            firstRow = false;
        }

        // 将累加器设置到聚合函数中
        function.setAccumulators(accumulators);
        // 获取之前的聚合结果
        RowData prevAggValue = function.getValue();

        // 根据输入类型更新聚合结果
        if (isAccumulateMsg(input)) {
            // 如果是累加消息，调用累加逻辑
            function.accumulate(input);
        } else {
            // 如果是撤销消息，调用撤销逻辑
            function.retract(input);
        }
        // 获取更新后的聚合结果
        RowData newAggValue = function.getValue();

        // 获取最新的累加器
        accumulators = function.getAccumulators();

        // 如果累加器中还有有效记录
        if (!recordCounter.recordCountIsZero(accumulators)) {
            // 更新累加器状态
            accState.update(accumulators);

            // 如果不是第一条记录，且需要输出撤销消息
            if (!firstRow) {
                if (stateRetentionTime <= 0 && equaliser.equals(prevAggValue, newAggValue)) {
                    // 如果新旧聚合结果相等，且状态清理未启用，则不输出任何消息
                    // 如果启用了状态清理，为避免下游过早清理状态，需要输出消息
                    return;
                } else {
                    // 输出撤销旧结果的消息
                    if (generateUpdateBefore) {
                        // 准备 UPDATE_BEFORE 消息，表示撤销上一条记录
                        resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.UPDATE_BEFORE);
                        out.collect(resultRow);
                    }
                    // 准备 UPDATE_AFTER 消息，表示插入新结果
                    resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
                }
            } else {
                // 如果是第一条记录，直接输出 INSERT 消息
                resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
            }

            // 输出最终结果
            out.collect(resultRow);

        } else {
            // 如果累加器中没有剩余记录（所有记录都被撤销）
            // 输出 DELETE 消息
            if (!firstRow) {
                // 准备 DELETE 消息，表示删除上一条记录
                resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.DELETE);
                out.collect(resultRow);
            }
            // 清空累加器状态
            accState.clear();
            // 清理与当前 Key 相关的数据视图
            function.cleanup();
        }
    }


    @Override
    public void close() throws Exception {
        if (function != null) {
            function.close();
        }
    }
}
