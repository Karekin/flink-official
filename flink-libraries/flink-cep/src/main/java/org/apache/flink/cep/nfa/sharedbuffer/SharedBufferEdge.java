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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 在 {@link SharedBuffer} 中的版本化边，允许检索前驱元素。
 */
public class SharedBufferEdge {

    private final NodeId target; // 目标节点的ID
    private final DeweyNumber deweyNumber; // 边的版本号（Dewey编号）

    /**
     * 创建一个带有版本信息（使用 {@link DeweyNumber}）的边，指向目标条目。
     *
     * @param target 目标条目的ID
     * @param deweyNumber 该边的版本号
     */
    public SharedBufferEdge(NodeId target, DeweyNumber deweyNumber) {
        this.target = target; // 设置目标节点ID
        this.deweyNumber = deweyNumber; // 设置版本号
    }

    /**
     * 获取目标节点ID。
     *
     * @return 目标节点ID
     */
    NodeId getTarget() {
        return target;
    }

    /**
     * 获取该边的 Dewey 编号（版本号）。
     *
     * @return 该边的 Dewey 编号
     */
    DeweyNumber getDeweyNumber() {
        return deweyNumber;
    }

    /**
     * 重写 toString 方法，返回共享缓冲区边的字符串表示。
     *
     * @return 返回边的字符串表示
     */
    @Override
    public String toString() {
        return "SharedBufferEdge{" + "target=" + target + ", deweyNumber=" + deweyNumber + '}';
    }

    /**
     * 重写 equals 方法，比较两个共享缓冲区边是否相等。
     *
     * @param o 另一个对象
     * @return 如果两个边相等，返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true; // 如果是同一个对象，返回 true
        }
        if (o == null || getClass() != o.getClass()) {
            return false; // 如果对象为空或者类型不一致，返回 false
        }
        SharedBufferEdge that = (SharedBufferEdge) o;
        return Objects.equals(target, that.target) && Objects.equals(deweyNumber, that.deweyNumber); // 比较目标节点和版本号是否相等
    }

    /**
     * 重写 hashCode 方法，根据目标节点和版本号生成边的哈希值。
     *
     * @return 边的哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(target, deweyNumber); // 计算哈希值
    }

    /**
     * {@link SharedBufferEdge} 的序列化器。
     */
    public static class SharedBufferEdgeSerializer
            extends TypeSerializerSingleton<SharedBufferEdge> {

        private static final long serialVersionUID = -5122474955050663979L;

        /**
         * 注意：这些序列化器字段实际上应该是 final 的。它们之所以不是 final，是因为为了兼容旧版本的反序列化路径。
         * 参见 {@link #readObject(ObjectInputStream)} 方法。
         */
        private TypeSerializer<NodeId> nodeIdSerializer; // 用于序列化 NodeId 的序列化器
        private TypeSerializer<DeweyNumber> deweyNumberSerializer; // 用于序列化 DeweyNumber 的序列化器

        /**
         * 构造函数，初始化序列化器。
         */
        public SharedBufferEdgeSerializer() {
            this(new NodeId.NodeIdSerializer(), DeweyNumber.DeweyNumberSerializer.INSTANCE);
        }

        private SharedBufferEdgeSerializer(
                TypeSerializer<NodeId> nodeIdSerializer,
                TypeSerializer<DeweyNumber> deweyNumberSerializer) {
            this.nodeIdSerializer = checkNotNull(nodeIdSerializer); // 确保 NodeId 序列化器不为空
            this.deweyNumberSerializer = checkNotNull(deweyNumberSerializer); // 确保 DeweyNumber 序列化器不为空
        }

        @Override
        public boolean isImmutableType() {
            return true; // 该类型是不可变的
        }

        @Override
        public SharedBufferEdge createInstance() {
            return null; // 创建实例的操作不需要，因为该类型是不可变的
        }

        @Override
        public SharedBufferEdge copy(SharedBufferEdge from) {
            return new SharedBufferEdge(from.target, from.deweyNumber); // 复制边，创建新的实例
        }

        @Override
        public SharedBufferEdge copy(SharedBufferEdge from, SharedBufferEdge reuse) {
            return copy(from); // 直接调用复制方法
        }

        @Override
        public int getLength() {
            return -1; // 无法计算长度
        }

        /**
         * 序列化方法，将 {@link SharedBufferEdge} 对象序列化到目标输出视图。
         *
         * @param record 要序列化的共享缓冲区边
         * @param target 序列化的目标输出视图
         * @throws IOException 序列化过程中可能抛出的异常
         */
        @Override
        public void serialize(SharedBufferEdge record, DataOutputView target) throws IOException {
            nodeIdSerializer.serialize(record.target, target); // 序列化目标节点ID
            deweyNumberSerializer.serialize(record.deweyNumber, target); // 序列化 Dewey 编号
        }

        /**
         * 反序列化方法，从输入视图中反序列化一个 {@link SharedBufferEdge} 对象。
         *
         * @param source 输入数据视图
         * @return 反序列化后的共享缓冲区边
         * @throws IOException 反序列化过程中可能抛出的异常
         */
        @Override
        public SharedBufferEdge deserialize(DataInputView source) throws IOException {
            NodeId target = nodeIdSerializer.deserialize(source); // 反序列化目标节点ID
            DeweyNumber deweyNumber = deweyNumberSerializer.deserialize(source); // 反序列化 Dewey 编号
            return new SharedBufferEdge(target, deweyNumber); // 返回反序列化的边
        }

        @Override
        public SharedBufferEdge deserialize(SharedBufferEdge reuse, DataInputView source)
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
            nodeIdSerializer.copy(source, target); // 复制目标节点ID
            deweyNumberSerializer.copy(source, target); // 复制 Dewey 编号
        }

        // -----------------------------------------------------------------------------------

        /**
         * 获取序列化器的配置快照，用于兼容性和格式演变。
         *
         * @return 配置快照
         */
        @Override
        public TypeSerializerSnapshot<SharedBufferEdge> snapshotConfiguration() {
            return new SharedBufferEdgeSerializerSnapshot(this); // 返回配置快照
        }

        /** 用于兼容性和格式演变的序列化器配置快照 */
        @SuppressWarnings("WeakerAccess")
        public static final class SharedBufferEdgeSerializerSnapshot
                extends CompositeTypeSerializerSnapshot<
                SharedBufferEdge, SharedBufferEdgeSerializer> {

            private static final int VERSION = 1; // 配置快照的版本

            public SharedBufferEdgeSerializerSnapshot() {}

            public SharedBufferEdgeSerializerSnapshot(
                    SharedBufferEdgeSerializer sharedBufferEdgeSerializer) {
                super(sharedBufferEdgeSerializer); // 使用提供的序列化器创建快照
            }

            @Override
            protected int getCurrentOuterSnapshotVersion() {
                return VERSION; // 返回当前版本号
            }

            @Override
            protected SharedBufferEdgeSerializer createOuterSerializerWithNestedSerializers(
                    TypeSerializer<?>[] nestedSerializers) {
                return new SharedBufferEdgeSerializer(
                        (NodeId.NodeIdSerializer) nestedSerializers[0], // 创建新的序列化器
                        (DeweyNumber.DeweyNumberSerializer) nestedSerializers[1]);
            }

            @Override
            protected TypeSerializer<?>[] getNestedSerializers(
                    SharedBufferEdgeSerializer outerSerializer) {
                return new TypeSerializer<?>[] {
                        outerSerializer.nodeIdSerializer, outerSerializer.deweyNumberSerializer
                };
            }
        }

        // ------------------------------------------------------------------------

        /**
         * 反序列化读取方法，在某些情况下需要重新创建序列化器。
         *
         * @param in 输入流
         * @throws IOException 如果发生IO异常
         * @throws ClassNotFoundException 如果发生类找不到异常
         */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject(); // 执行默认反序列化操作

            if (nodeIdSerializer == null) {
                // 如果序列化器为空，则为旧版本的反序列化路径创建新的序列化器实例
                this.nodeIdSerializer = new NodeId.NodeIdSerializer();
                this.deweyNumberSerializer = DeweyNumber.DeweyNumberSerializer.INSTANCE;
            }
        }
    }
}

