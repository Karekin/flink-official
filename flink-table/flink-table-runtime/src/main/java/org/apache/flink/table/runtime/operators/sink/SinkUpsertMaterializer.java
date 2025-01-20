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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;

/**
 * 一个操作符，维护与 upsert 键相关的状态，并为下游操作符生成 upsert 视图。
 *
 * <ul>
 *   <li>将插入记录添加到状态中，并更新 {@link RowKind} 后发射。</li>
 *   <li>将删除操作应用于状态。</li>
 *   <li>如果删除操作影响到最后一条记录，或者删除后状态为空，则发射带有更新的 {@link RowKind} 的删除操作。
 *       对于已经更新的记录的删除操作将被吞掉。</li>
 * </ul>
 */
public class SinkUpsertMaterializer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SinkUpsertMaterializer.class);

    // 状态清除的警告信息，当状态因为 TTL 被清除时会显示该信息。
    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state ttl. This will result in incorrect result. "
                    + "You can increase the state ttl to avoid this.";

    private final StateTtlConfig ttlConfig; // 状态的TTL配置
    private final GeneratedRecordEqualiser generatedRecordEqualiser; // 生成的记录比较器
    private final GeneratedRecordEqualiser generatedUpsertKeyEqualiser; // 生成的upsert键比较器
    private final TypeSerializer<RowData> serializer; // RowData 的序列化器
    private final int[] inputUpsertKey; // 用于 upsert 操作的键字段索引
    private final boolean hasUpsertKey; // 是否存在 upsert 键字段

    // 比较器，用于比较记录是否相等：
    // 如果 hasUpsertKey 为 true，则比较仅基于 upsertKey（投影后的行数据）
    // 如果 hasUpsertKey 为 false，则比较整个行数据
    private transient RecordEqualiser equaliser;

    // 用于缓冲已发射的插入记录，删除操作将在这些记录上优先应用。
    // 行的类型可能是 +I 或 +U，应用删除操作时将忽略这些类型。
    private transient ValueState<List<RowData>> state;
    private transient TimestampedCollector<RowData> collector;

    // 如果有 upsertKey，重用的投影行数据，用于比较 upsert 键。
    private transient ProjectedRowData upsertKeyProjectedRow1;
    private transient ProjectedRowData upsertKeyProjectedRow2;

    public SinkUpsertMaterializer(
            StateTtlConfig ttlConfig,
            TypeSerializer<RowData> serializer,
            GeneratedRecordEqualiser generatedRecordEqualiser,
            @Nullable GeneratedRecordEqualiser generatedUpsertKeyEqualiser,
            @Nullable int[] inputUpsertKey) {
        this.ttlConfig = ttlConfig;
        this.serializer = serializer;
        this.generatedRecordEqualiser = generatedRecordEqualiser;
        this.generatedUpsertKeyEqualiser = generatedUpsertKeyEqualiser;
        this.inputUpsertKey = inputUpsertKey;
        this.hasUpsertKey = null != inputUpsertKey && inputUpsertKey.length > 0;
        if (hasUpsertKey) {
            // 如果有 upsertKey，确保 generatedUpsertKeyEqualiser 不为 null
            Preconditions.checkNotNull(
                    generatedUpsertKeyEqualiser,
                    "GeneratedUpsertKeyEqualiser cannot be null when inputUpsertKey is not empty!");
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        // 根据是否有 upsert 键，选择相应的比较器
        if (hasUpsertKey) {
            this.equaliser =
                    generatedUpsertKeyEqualiser.newInstance(
                            getRuntimeContext().getUserCodeClassLoader());
            // 创建 upsert 键的投影行数据实例
            upsertKeyProjectedRow1 = ProjectedRowData.from(inputUpsertKey);
            upsertKeyProjectedRow2 = ProjectedRowData.from(inputUpsertKey);
        } else {
            this.equaliser =
                    generatedRecordEqualiser.newInstance(
                            getRuntimeContext().getUserCodeClassLoader());
        }
        // 初始化状态存储器，定义存储的 List 类型的状态
        ValueStateDescriptor<List<RowData>> descriptor =
                new ValueStateDescriptor<>("values", new ListSerializer<>(serializer));
        // 如果启用了 TTL，设置状态的 TTL 配置
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        this.state = getRuntimeContext().getState(descriptor);
        this.collector = new TimestampedCollector<>(output); // 设置时间戳收集器
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData row = element.getValue(); // 获取当前行数据
        List<RowData> values = state.value(); // 获取当前状态值
        if (values == null) {
            values = new ArrayList<>(2); // 如果状态为空，初始化为空的列表
        }

        switch (row.getRowKind()) { // 根据行的类型进行处理
            case INSERT:
            case UPDATE_AFTER:
                addRow(values, row); // 插入或更新操作，添加行到状态
                break;

            case UPDATE_BEFORE:
            case DELETE:
                retractRow(values, row); // 更新前或删除操作，从状态中撤销
                break;
        }
    }

    // 将一条新行添加到状态中，并发射更新后的行数据
    private void addRow(List<RowData> values, RowData add) throws IOException {
        // 确定发射的行类型，如果状态为空，使用 INSERT，否则使用 UPDATE_AFTER
        RowKind outRowKind = values.isEmpty() ? INSERT : UPDATE_AFTER;
        if (hasUpsertKey) {
            // 如果有 upsert 键，查找该键对应的记录
            int index = findFirst(values, add);
            if (index == -1) {
                values.add(add); // 如果没有找到相同记录，添加新记录
            } else {
                values.set(index, add); // 如果找到了相同记录，替换该记录
            }
        } else {
            values.add(add); // 如果没有 upsert 键，直接添加
        }
        add.setRowKind(outRowKind); // 设置行类型
        collector.collect(add); // 发射该行

        // 更新状态
        state.update(values);
    }

    // 撤销某条记录，并根据需要发射删除或更新后的记录
    private void retractRow(List<RowData> values, RowData retract) throws IOException {
        final int lastIndex = values.size() - 1;
        final int index = findFirst(values, retract); // 查找要撤销的记录
        if (index == -1) {
            LOG.info(STATE_CLEARED_WARN_MSG); // 如果找不到记录，输出警告
            return;
        } else {
            // 找到记录后，从状态中移除
            values.remove(index);
        }
        if (values.isEmpty()) {
            // 如果状态为空，发射删除操作
            retract.setRowKind(DELETE);
            collector.collect(retract);
        } else if (index == lastIndex) {
            // 如果移除的是最后一条记录，更新倒数第二条记录
            final RowData latestRow = values.get(values.size() - 1);
            latestRow.setRowKind(UPDATE_AFTER);
            collector.collect(latestRow);
        }

        // 如果状态为空，清空状态
        if (values.isEmpty()) {
            state.clear();
        } else {
            state.update(values); // 更新状态
        }
    }

    // 查找与目标记录相等的第一条记录
    private int findFirst(List<RowData> values, RowData target) {
        final Iterator<RowData> iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            if (equalsIgnoreRowKind(target, iterator.next())) { // 比较行数据是否相等
                return i;
            }
            i++;
        }
        return -1;
    }

    // 忽略行类型进行行比较
    private boolean equalsIgnoreRowKind(RowData newRow, RowData oldRow) {
        newRow.setRowKind(oldRow.getRowKind()); // 设置相同的行类型
        if (hasUpsertKey) {
            // 如果有 upsert 键，仅比较 upsert 键部分
            return equaliser.equals(
                    upsertKeyProjectedRow1.replaceRow(newRow),
                    upsertKeyProjectedRow2.replaceRow(oldRow));
        }
        return equaliser.equals(newRow, oldRow); // 否则，比较整个行数据
    }
}

