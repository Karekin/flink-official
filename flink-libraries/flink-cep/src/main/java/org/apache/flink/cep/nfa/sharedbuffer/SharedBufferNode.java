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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge.SharedBufferEdgeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyedStateBackend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SharedBuffer} 中的一个条目，允许存储不同条目之间的关系。
 */
public class SharedBufferNode {

    private final List<Lockable<SharedBufferEdge>> edges; // 存储共享缓冲区节点的边

    /**
     * 默认构造函数，初始化一个空的边列表。
     */
    public SharedBufferNode() {
        edges = new ArrayList<>(); // 初始化为空的边列表
    }

    /**
     * 带有边列表的构造函数，用于初始化共享缓冲区节点。
     *
     * @param edges 共享缓冲区节点的边列表
     */
    SharedBufferNode(List<Lockable<SharedBufferEdge>> edges) {
        this.edges = edges; // 使用提供的边列表初始化
    }

    /**
     * 获取当前节点的所有边。
     *
     * @return 当前节点的边列表
     */
    public List<Lockable<SharedBufferEdge>> getEdges() {
        return edges;
    }

    /**
     * 向当前节点添加一条边。
     *
     * @param edge 需要添加的边
     */
    public void addEdge(SharedBufferEdge edge) {
        edges.add(new Lockable<>(edge, 0)); // 将边添加到列表中，并将其包装为 Lockable 对象
    }

    /**
     * 重写 toString 方法，返回共享缓冲区节点的字符串表示。
     *
     * @return 当前节点的字符串表示
     */
    @Override
    public String toString() {
        return "SharedBufferNode{" + "edges=" + edges + '}'; // 返回包含边列表的字符串表示
    }

    /**
     * 重写 equals 方法，比较两个共享缓冲区节点是否相等。
     *
     * @param o 另一个对象
     * @return 如果两个节点相等，返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SharedBufferNode that = (SharedBufferNode) o;
        return Objects.equals(edges, that.edges); // 比较边列表是否相等
    }

    /**
     * 重写 hashCode 方法，根据边列表生成节点的哈希值。
     *
     * @return 当前节点的哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(edges); // 使用边列表计算哈希值
    }

    /**
     * {@link SharedBufferNode} 的序列化器。
     *
     * <p>此序列化器已经被弃用，不能直接迁移到新版本。新结构需要从其他节点获取附加信息。迁移在 {@link SharedBuffer#migrateOldState(KeyedStateBackend, ValueState)} 中发生。
     *
     * @deprecated 该序列化器在 <= 1.12 版本中使用，新的版本请使用 {@link org.apache.flink.cep.nfa.sharedbuffer.SharedBufferNodeSerializer}。
     */
    @Deprecated
    public static class SharedBufferNodeSerializer
            extends TypeSerializerSingleton<SharedBufferNode> {

        private static final long serialVersionUID = -6687780732295439832L;

        private final ListSerializer<SharedBufferEdge> edgesSerializer; // 用于序列化边列表的序列化器

        /**
         * 构造函数，初始化边列表序列化器。
         */
        public SharedBufferNodeSerializer() {
            this.edgesSerializer = new ListSerializer<>(new SharedBufferEdgeSerializer()); // 初始化边列表的序列化器
        }

        private SharedBufferNodeSerializer(ListSerializer<SharedBufferEdge> edgesSerializer) {
            this.edgesSerializer = checkNotNull(edgesSerializer); // 使用提供的序列化器初始化
        }

        @Override
        public boolean isImmutableType() {
            return false; // 该类型不是不可变的
        }

        @Override
        public SharedBufferNode createInstance() {
            return new SharedBufferNode(new ArrayList<>()); // 创建一个新的空节点实例
        }

        @Override
        public SharedBufferNode copy(SharedBufferNode from) {
            throw new UnsupportedOperationException("Should not be used"); // 不支持复制方法
        }

        @Override
        public SharedBufferNode copy(SharedBufferNode from, SharedBufferNode reuse) {
            return copy(from); // 返回复制的方法（不建议使用）
        }

        @Override
        public int getLength() {
            return -1; // 不支持计算长度
        }

        /**
         * 序列化方法，将 {@link SharedBufferNode} 对象序列化到目标输出视图。
         *
         * @param record 要序列化的共享缓冲区节点
         * @param target 序列化的目标输出视图
         * @throws IOException 序列化过程中可能抛出的异常
         */
        @Override
        public void serialize(SharedBufferNode record, DataOutputView target) throws IOException {
            throw new UnsupportedOperationException("We should no longer use it for serialization"); // 不再使用此序列化方法
        }

        /**
         * 反序列化方法，从输入视图中反序列化一个 {@link SharedBufferNode} 对象。
         *
         * @param source 输入数据视图
         * @return 反序列化后的共享缓冲区节点
         * @throws IOException 反序列化过程中可能抛出的异常
         */
        @Override
        public SharedBufferNode deserialize(DataInputView source) throws IOException {
            // 反序列化边列表
            List<SharedBufferEdge> edges = edgesSerializer.deserialize(source);
            SharedBufferNode node = new SharedBufferNode();

            // 将反序列化的边添加到节点中
            for (SharedBufferEdge edge : edges) {
                node.addEdge(edge);
            }
            return node; // 返回反序列化的节点
        }

        @Override
        public SharedBufferNode deserialize(SharedBufferNode reuse, DataInputView source)
                throws IOException {
            return deserialize(source); // 使用反序列化方法
        }

        /**
         * 从源数据视图复制数据到目标数据视图。
         *
         * @param source 源数据视图
         * @param target 目标数据视图
         * @throws IOException 复制过程中可能抛出的异常
         */
        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            edgesSerializer.copy(source, target); // 复制边列表
        }

        // -----------------------------------------------------------------------------------

        /**
         * 获取序列化器的配置快照，用于兼容性和格式演变。
         *
         * @return 配置快照
         */
        @Override
        public TypeSerializerSnapshot<SharedBufferNode> snapshotConfiguration() {
            return new SharedBufferNodeSerializerSnapshot(this); // 返回配置快照
        }

        /** 用于兼容性和格式演变的序列化器配置快照 */
        @SuppressWarnings("WeakerAccess")
        public static final class SharedBufferNodeSerializerSnapshot
                extends CompositeTypeSerializerSnapshot<
                SharedBufferNode, SharedBufferNodeSerializer> {

            private static final int VERSION = 1; // 配置快照的版本

            public SharedBufferNodeSerializerSnapshot() {}

            public SharedBufferNodeSerializerSnapshot(
                    SharedBufferNodeSerializer sharedBufferNodeSerializer) {
                super(sharedBufferNodeSerializer); // 使用提供的序列化器创建快照
            }

            @Override
            protected int getCurrentOuterSnapshotVersion() {
                return VERSION; // 返回当前版本号
            }

            @Override
            @SuppressWarnings("unchecked")
            protected SharedBufferNodeSerializer createOuterSerializerWithNestedSerializers(
                    TypeSerializer<?>[] nestedSerializers) {
                return new SharedBufferNodeSerializer(
                        (ListSerializer<SharedBufferEdge>) nestedSerializers[0]); // 创建新的序列化器
            }

            @Override
            protected TypeSerializer<?>[] getNestedSerializers(
                    SharedBufferNodeSerializer outerSerializer) {
                return new TypeSerializer<?>[] {outerSerializer.edgesSerializer}; // 返回嵌套的序列化器
            }
        }
    }
}

