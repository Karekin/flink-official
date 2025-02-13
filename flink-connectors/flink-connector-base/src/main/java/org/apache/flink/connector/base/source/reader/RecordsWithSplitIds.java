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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * 一个接口，表示从 fetchers 传递到 source reader 的元素。
 */
@PublicEvolving
public interface RecordsWithSplitIds<E> {

    /**
     * 切换到下一个数据分片（split）。
     *
     * 该方法在初始化时也会被调用，以移动到第一个数据分片。
     * 如果没有剩余的分片，则返回 null。
     *
     * @return 下一个数据分片的 ID，如果没有剩余分片，则返回 null。
     */
    @Nullable
    String nextSplit();

    /**
     * 从当前数据分片（split）获取下一条记录。
     *
     * 如果当前分片中没有剩余记录，则返回 null。
     *
     * @return 当前分片中的下一条记录，如果没有剩余记录，则返回 null。
     */
    @Nullable
    E nextRecordFromSplit();

    /**
     * 获取已完成处理的数据分片集合。
     *
     * @return 该 RecordsWithSplitIds 返回后，已完成的分片集合。
     */
    Set<String> finishedSplits();

    /**
     * 当此批次中的所有记录都已被处理完毕时调用该方法。
     *
     * <p>重写此方法可以使实现类有机会回收或复用该对象，这是一种性能优化策略。
     * 在记录对象较大或分配成本较高的情况下，这种优化尤为重要。
     */
    default void recycle() {}
}

