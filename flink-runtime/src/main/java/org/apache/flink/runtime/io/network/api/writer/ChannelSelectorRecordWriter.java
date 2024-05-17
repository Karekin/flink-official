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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.IOReadableWritable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A regular record-oriented runtime result writer.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link RecordWriter} and emits records to the
 * channel selected by the {@link ChannelSelector} for regular {@link #emit(IOReadableWritable)}.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 一个面向记录的常规运行时结果写入器。
*/
public final class ChannelSelectorRecordWriter<T extends IOReadableWritable>
        extends RecordWriter<T> {

    private final ChannelSelector<T> channelSelector;

    ChannelSelectorRecordWriter(
            ResultPartitionWriter writer,
            ChannelSelector<T> channelSelector,
            long timeout,
            String taskName) {
        super(writer, timeout, taskName);

        this.channelSelector = checkNotNull(channelSelector);
        this.channelSelector.setup(numberOfSubpartitions);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将数据发送到下游的InputChannel中
    */
    @Override
    public void emit(T record) throws IOException {
        //channelSelector.selectChannel选择下游的InputChannel
        emit(record, channelSelector.selectChannel(record));
    }

    @Override
    public void broadcastEmit(T record) throws IOException {
        checkErroneous();

        // Emitting to all subpartitions in a for loop can be better than calling
        // ResultPartitionWriter#broadcastRecord because the broadcastRecord
        // method incurs extra overhead.
        ByteBuffer serializedRecord = serializeRecord(serializer, record);
        for (int subpartitionIndex = 0;
                subpartitionIndex < numberOfSubpartitions;
                subpartitionIndex++) {
            serializedRecord.rewind();
            emit(serializedRecord, subpartitionIndex);
        }

        if (flushAlways) {
            flushAll();
        }
    }

    @VisibleForTesting
    public ChannelSelector<T> getChannelSelector() {
        return channelSelector;
    }
}
