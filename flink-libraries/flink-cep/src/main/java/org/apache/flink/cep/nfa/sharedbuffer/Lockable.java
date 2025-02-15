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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.LegacySerializerSnapshotTransformer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * 实现了对传入事件和 {@link SharedBufferNode} 的锁定逻辑，使用锁引用计数器。
 *
 * <p>该类允许通过一个引用计数器来控制对元素的锁定，保证并发访问时的线程安全。
 */
public final class Lockable<T> {

    private int refCounter; // 锁引用计数器，记录当前有多少个锁定操作

    private final T element; // 锁定的元素，通常是事件或节点

    /**
     * 构造函数，初始化锁定元素及其引用计数器。
     *
     * @param element 锁定的元素
     * @param refCounter 锁引用计数器的初始值
     */
    public Lockable(T element, int refCounter) {
        this.refCounter = refCounter; // 初始化引用计数器
        this.element = element; // 初始化锁定的元素
    }

    /**
     * 锁定该元素，增加引用计数器。
     */
    public void lock() {
        refCounter += 1; // 增加锁引用计数
    }

    /**
     * 释放该元素的锁。如果不再有其他锁定操作，这个方法将返回 true。
     *
     * @return 如果没有其他锁定操作，返回 true
     */
    boolean release() {
        if (refCounter <= 0) {
            return true; // 如果引用计数小于等于0，表示已无锁定
        }

        refCounter -= 1; // 减少锁引用计数
        return refCounter == 0; // 如果引用计数变为0，表示所有锁已释放
    }

    /**
     * 获取锁定的元素。
     *
     * @return 锁定的元素
     */
    public T getElement() {
        return element;
    }

    /**
     * 获取当前的引用计数器值。
     *
     * @return 当前的引用计数器值
     */
    int getRefCounter() {
        return refCounter;
    }

    /**
     * 重写 toString 方法，返回锁定对象的字符串表示。
     *
     * @return 锁定对象的字符串表示
     */
    @Override
    public String toString() {
        return "Lock{" + "refCounter=" + refCounter + '}'; // 返回包含引用计数器的字符串表示
    }

    /**
     * 重写 equals 方法，比较两个 Lockable 对象是否相等。
     *
     * @param o 另一个对象
     * @return 如果两个 Lockable 对象相等，返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true; // 如果是同一个对象，返回 true
        }
        if (o == null || getClass() != o.getClass()) {
            return false; // 如果对象为空或类型不同，返回 false
        }
        Lockable<?> lockable = (Lockable<?>) o;
        return refCounter == lockable.refCounter && Objects.equals(element, lockable.element); // 比较引用计数器和元素是否相同
    }

    /**
     * 重写 hashCode 方法，生成 Lockable 对象的哈希值。
     *
     * @return Lockable 对象的哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(refCounter, element); // 根据引用计数器和元素计算哈希值
    }

    /** {@link Lockable} 类型的序列化器。 */
    public static class LockableTypeSerializer<E> extends TypeSerializer<Lockable<E>>
            implements LegacySerializerSnapshotTransformer<Lockable<E>> {

        private static final long serialVersionUID = 3298801058463337340L;
        private final TypeSerializer<E> elementSerializer; // 元素的序列化器

        /**
         * 构造函数，初始化元素的序列化器。
         *
         * @param elementSerializer 元素的序列化器
         */
        public LockableTypeSerializer(TypeSerializer<E> elementSerializer) {
            this.elementSerializer = elementSerializer; // 初始化元素序列化器
        }

        @Override
        public boolean isImmutableType() {
            return false; // 该类型是可变的
        }

        @Override
        public LockableTypeSerializer<E> duplicate() {
            TypeSerializer<E> elementSerializerCopy = elementSerializer.duplicate(); // 复制元素序列化器
            return elementSerializerCopy == elementSerializer
                    ? this
                    : new LockableTypeSerializer<>(elementSerializerCopy); // 返回新的 Lockable 类型序列化器
        }

        @Override
        public Lockable<E> createInstance() {
            return null; // 不创建实例
        }

        /**
         * 复制 Lockable 对象，返回一个新的对象。
         *
         * @param from 原始 Lockable 对象
         * @return 复制后的 Lockable 对象
         */
        @Override
        public Lockable<E> copy(Lockable<E> from) {
            return new Lockable<>(elementSerializer.copy(from.element), from.refCounter); // 复制元素和引用计数器
        }

        @Override
        public Lockable<E> copy(Lockable<E> from, Lockable<E> reuse) {
            return copy(from); // 直接调用复制方法
        }

        @Override
        public int getLength() {
            return -1; // 不支持固定长度
        }

        /**
         * 序列化 Lockable 对象到目标输出视图。
         *
         * @param record 要序列化的 Lockable 对象
         * @param target 序列化的目标输出视图
         * @throws IOException 序列化过程中可能抛出的异常
         */
        @Override
        public void serialize(Lockable<E> record, DataOutputView target) throws IOException {
            IntSerializer.INSTANCE.serialize(record.refCounter, target); // 序列化引用计数器
            elementSerializer.serialize(record.element, target); // 序列化元素
        }

        /**
         * 反序列化 Lockable 对象。
         *
         * @param source 输入数据视图
         * @return 反序列化后的 Lockable 对象
         * @throws IOException 反序列化过程中可能抛出的异常
         */
        @Override
        public Lockable<E> deserialize(DataInputView source) throws IOException {
            int refCount = IntSerializer.INSTANCE.deserialize(source); // 反序列化引用计数器
            E record = elementSerializer.deserialize(source); // 反序列化元素
            return new Lockable<>(record, refCount); // 返回新的 Lockable 对象
        }

        @Override
        public Lockable<E> deserialize(Lockable<E> reuse, DataInputView source) throws IOException {
            return deserialize(source); // 直接调用反序列化方法
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
            IntSerializer.INSTANCE.copy(source, target); // 复制引用计数器

            E element = elementSerializer.deserialize(source); // 反序列化元素
            elementSerializer.serialize(element, target); // 序列化元素
        }

        /**
         * 重写 equals 方法，比较两个 Lockable 类型的序列化器是否相等。
         *
         * @param o 另一个对象
         * @return 如果两个序列化器相等，返回 true；否则返回 false
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true; // 如果是同一个对象，返回 true
            }
            if (o == null || getClass() != o.getClass()) {
                return false; // 如果对象为空或类型不同，返回 false
            }
            LockableTypeSerializer<?> that = (LockableTypeSerializer<?>) o;
            return Objects.equals(elementSerializer, that.elementSerializer); // 比较元素序列化器是否相同
        }

        /**
         * 重写 hashCode 方法，生成 Lockable 类型序列化器的哈希值。
         *
         * @return Lockable 类型序列化器的哈希值
         */
        @Override
        public int hashCode() {
            return Objects.hash(elementSerializer); // 根据元素序列化器计算哈希值
        }

        /**
         * 获取序列化器配置的快照，用于兼容性和格式演变。
         *
         * @return 配置快照
         */
        @Override
        public TypeSerializerSnapshot<Lockable<E>> snapshotConfiguration() {
            return new LockableTypeSerializerSnapshot<>(this); // 返回配置快照
        }

        @VisibleForTesting
        TypeSerializer<E> getElementSerializer() {
            return elementSerializer; // 返回元素序列化器
        }

        @Override
        @SuppressWarnings("unchecked")
        public <U> TypeSerializerSnapshot<Lockable<E>> transformLegacySerializerSnapshot(
                TypeSerializerSnapshot<U> legacySnapshot) {
            if (legacySnapshot instanceof LockableTypeSerializerSnapshot) {
                return (TypeSerializerSnapshot<Lockable<E>>) legacySnapshot; // 转换为旧版序列化器快照
            }

            LockableTypeSerializerSnapshot<E> lockableSnapshot =
                    new LockableTypeSerializerSnapshot<>();
            CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
                    lockableSnapshot, legacySnapshot); // 设置嵌套的序列化器快照
            return lockableSnapshot;
        }
    }
}

