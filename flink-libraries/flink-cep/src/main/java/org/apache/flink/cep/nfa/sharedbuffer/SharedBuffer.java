/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOVICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Vhe ASF licenses this file
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
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.cep.configuration.SharedBufferCacheConfig;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalCause;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 一个共享缓冲区实现，它根据状态存储值。此外，这些值可以版本化，从而可以在缓冲区中检索其前驱元素。
 *
 * <p>该实现的思想是为传入的事件创建一个缓冲区，并为每个事件分配唯一的ID。这样，我们在处理过程中不需要反序列化事件，并且只存储事件的一个副本。
 *
 * <p>{@link SharedBuffer} 中的条目是 {@link SharedBufferNode}。共享缓冲区节点允许存储不同条目之间的关系。Dewey版本控制方案允许区分不同的关系（例如，前一个元素）。
 *
 * <p>该实现强烈基于论文 "Efficient Pattern Matching over Event Streams"。
 *
 * @param <V> 值的类型
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 *     https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class SharedBuffer<V> {

    private static final Logger LOG = LoggerFactory.getLogger(SharedBuffer.class);

    private static final String LEGACY_ENTRIES_STATE_NAME = "sharedBuffer-entries"; // 旧的条目状态名称
    private static final String ENTRIES_STATE_NAME = "sharedBuffer-entries-with-lockable-edges"; // 新的条目状态名称
    private static final String EVENTS_STATE_NAME = "sharedBuffer-events"; // 事件状态名称
    private static final String EVENTS_COUNT_STATE_NAME = "sharedBuffer-events-count"; // 事件计数状态名称

    private final MapState<EventId, Lockable<V>> eventsBuffer; // 存储事件的缓冲区
    /** 存储每个时间戳看到的事件数量 */
    private final MapState<Long, Integer> eventsCount;

    private final MapState<NodeId, Lockable<SharedBufferNode>> entries; // 存储共享缓冲区节点的条目

    /** 事件缓冲区状态的缓存 */
    private final Cache<EventId, Lockable<V>> eventsBufferCache;

    /** 共享缓冲区节点的缓存 */
    private final Cache<NodeId, Lockable<SharedBufferNode>> entryCache;

    private final Timer cacheStatisticsTimer; // 用于定期记录缓存统计信息的定时器

    /**
     * 构造函数，用于初始化共享缓冲区。
     *
     * @param stateStore 状态存储
     * @param valueSerializer 值的序列化器
     */
    @VisibleForTesting
    public SharedBuffer(KeyedStateStore stateStore, TypeSerializer<V> valueSerializer) {
        this(stateStore, valueSerializer, new SharedBufferCacheConfig());
    }

    /**
     * 构造函数，初始化共享缓冲区，带有缓存配置。
     *
     * @param stateStore 状态存储
     * @param valueSerializer 值的序列化器
     * @param cacheConfig 缓存配置
     */
    public SharedBuffer(
            KeyedStateStore stateStore,
            TypeSerializer<V> valueSerializer,
            SharedBufferCacheConfig cacheConfig) {
        // 初始化事件缓冲区的状态
        this.eventsBuffer =
                stateStore.getMapState(
                        new MapStateDescriptor<>(
                                EVENTS_STATE_NAME,
                                EventId.EventIdSerializer.INSTANCE,
                                new Lockable.LockableTypeSerializer<>(valueSerializer)));

        // 初始化共享缓冲区节点的状态
        this.entries =
                stateStore.getMapState(
                        new MapStateDescriptor<>(
                                ENTRIES_STATE_NAME,
                                new NodeId.NodeIdSerializer(),
                                new Lockable.LockableTypeSerializer<>(
                                        new SharedBufferNodeSerializer())));

        // 初始化事件计数的状态
        this.eventsCount =
                stateStore.getMapState(
                        new MapStateDescriptor<>(
                                EVENTS_COUNT_STATE_NAME,
                                LongSerializer.INSTANCE,
                                IntSerializer.INSTANCE));

        // 设置事件缓冲区的缓存
        this.eventsBufferCache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheConfig.getEventsBufferCacheSlots()) // 设置缓存最大大小
                        .removalListener(
                                (RemovalListener<EventId, Lockable<V>>)
                                        removalNotification -> {
                                            if (RemovalCause.SIZE
                                                    == removalNotification.getCause()) {
                                                try {
                                                    eventsBuffer.put(
                                                            removalNotification.getKey(),
                                                            removalNotification.getValue());
                                                } catch (Exception e) {
                                                    LOG.error(
                                                            "Error in putting value into eventsBuffer.",
                                                            e);
                                                }
                                            }
                                        })
                        .build();

        // 设置共享缓冲区节点的缓存
        this.entryCache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheConfig.getEntryCacheSlots()) // 设置缓存最大大小
                        .removalListener(
                                (RemovalListener<NodeId, Lockable<SharedBufferNode>>)
                                        removalNotification -> {
                                            if (RemovalCause.SIZE
                                                    == removalNotification.getCause()) {
                                                try {
                                                    entries.put(
                                                            removalNotification.getKey(),
                                                            removalNotification.getValue());
                                                } catch (Exception e) {
                                                    LOG.error(
                                                            "Error in putting value into entries.",
                                                            e);
                                                }
                                            }
                                        })
                        .build();

        // 初始化缓存统计定时器，用于定期输出缓存的统计信息
        cacheStatisticsTimer = new Timer();
        cacheStatisticsTimer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        LOG.info(
                                "Statistics details of eventsBufferCache: {}, statistics details of entryCache: {}.",
                                eventsBufferCache.stats(),
                                entryCache.stats());
                    }
                },
                cacheConfig.getCacheStatisticsInterval().toMillis(),
                cacheConfig.getCacheStatisticsInterval().toMillis());
    }

    /**
     * 将旧的状态迁移到新的状态存储中。
     *
     * @param stateBackend 状态后端
     * @param computationStates 计算状态
     */
    public void migrateOldState(
            KeyedStateBackend<?> stateBackend, ValueState<NFAState> computationStates)
            throws Exception {
        // 遍历所有的键，并应用状态迁移
        stateBackend.applyToAllKeys(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>(
                        LEGACY_ENTRIES_STATE_NAME,
                        new NodeId.NodeIdSerializer(),
                        new Lockable.LockableTypeSerializer<>(
                                new SharedBufferNode.SharedBufferNodeSerializer())),
                (key, state) -> {
                    copyEntries(state); // 复制旧的条目
                    state.entries().forEach(this::lockPredecessorEdges); // 锁定前驱边
                    state.clear(); // 清空旧的状态

                    NFAState nfaState = computationStates.value(); // 获取计算状态
                    // 锁定部分匹配的前驱元素
                    nfaState.getPartialMatches()
                            .forEach(
                                    computationState ->
                                            lockEdges(
                                                    computationState.getPreviousBufferEntry(),
                                                    computationState.getVersion()));
                    // 锁定已完成匹配的前驱元素
                    nfaState.getCompletedMatches()
                            .forEach(
                                    computationState ->
                                            lockEdges(
                                                    computationState.getPreviousBufferEntry(),
                                                    computationState.getVersion()));
                });
    }

    private void copyEntries(MapState<NodeId, Lockable<SharedBufferNode>> state) throws Exception {
        // 将旧状态条目复制到当前条目中
        state.entries()
                .forEach(
                        e -> {
                            try {
                                entries.put(e.getKey(), e.getValue());
                            } catch (Exception exception) {
                                throw new RuntimeException(exception);
                            }
                        });
    }

    private void lockPredecessorEdges(Map.Entry<NodeId, Lockable<SharedBufferNode>> e) {
        // 锁定前驱节点的边
        SharedBufferNode oldNode = e.getValue().getElement();
        oldNode.getEdges()
                .forEach(
                        edge -> {
                            SharedBufferEdge oldEdge = edge.getElement();
                            lockEdges(oldEdge.getTarget(), oldEdge.getDeweyNumber());
                        });
    }
    /**
     * 锁定指定节点的边，确保在处理过程中，版本兼容的边被锁定。
     *
     * @param nodeId 要锁定的节点ID
     * @param version 版本号，用于检查边是否兼容
     */
    private void lockEdges(NodeId nodeId, DeweyNumber version) {

        // 如果节点ID为空，则直接返回
        if (nodeId == null) {
            return;
        }

        try {
            // 从 entries 状态获取共享缓冲区节点
            SharedBufferNode newNode = entries.get(nodeId).getElement();

            // 遍历节点的所有边，检查边的版本号是否与指定版本兼容
            newNode.getEdges()
                    .forEach(
                            newEdge -> {
                                // 如果版本兼容，则锁定该边
                                if (version.isCompatibleWith(
                                        newEdge.getElement().getDeweyNumber())) {
                                    newEdge.lock(); // 锁定边
                                }
                            });
        } catch (Exception exception) {
            // 捕获异常并包装为运行时异常抛出
            throw new RuntimeException(exception);
        }
    }

    /**
     * 构建一个访问器，用于处理这个共享缓冲区。
     *
     * @return 返回一个用于处理共享缓冲区的访问器
     */
    public SharedBufferAccessor<V> getAccessor() {
        return new SharedBufferAccessor<>(this); // 创建并返回共享缓冲区访问器
    }

    /**
     * 处理时间推进操作，清理小于当前时间戳的事件计数。
     *
     * @param timestamp 当前时间戳
     * @throws Exception 如果在清理过程中发生异常，则抛出
     */
    void advanceTime(long timestamp) throws Exception {
        // 获取事件计数器的迭代器
        Iterator<Long> iterator = eventsCount.keys().iterator();

        // 遍历事件计数器，移除时间戳小于当前时间戳的事件
        while (iterator.hasNext()) {
            Long next = iterator.next();
            if (next < timestamp) {
                iterator.remove(); // 移除过时事件
            }
        }
    }

    /**
     * 注册一个新事件，并返回事件ID。
     *
     * @param value 事件值
     * @param timestamp 事件时间戳
     * @return 返回注册的事件ID
     * @throws Exception 如果在注册过程中发生异常，则抛出
     */
    EventId registerEvent(V value, long timestamp) throws Exception {
        // 获取该时间戳下已注册事件的数量
        Integer id = eventsCount.get(timestamp);
        if (id == null) {
            id = 0; // 如果没有事件，则初始化为0
        }

        // 创建事件ID
        EventId eventId = new EventId(id, timestamp);

        // 创建 Lockable 对象，锁定事件
        Lockable<V> lockableValue = new Lockable<>(value, 1);

        // 更新事件计数器
        eventsCount.put(timestamp, id + 1);

        // 将事件缓存到事件缓冲区缓存中
        eventsBufferCache.put(eventId, lockableValue);

        return eventId; // 返回事件ID
    }

    /**
     * 检查缓冲区中是否没有任何元素。
     *
     * @return 如果缓冲区为空，返回 true；否则返回 false
     * @throws Exception 如果无法访问系统状态，则抛出异常
     */
    public boolean isEmpty() throws Exception {
        // 检查缓存中的事件ID和状态中的事件是否为空
        return Iterables.isEmpty(eventsBufferCache.asMap().keySet())
                && Iterables.isEmpty(eventsBuffer.keys());
    }

    /**
     * 释放缓存统计定时器。
     */
    public void releaseCacheStatisticsTimer() {
        // 如果定时器不为空，则取消定时器
        if (cacheStatisticsTimer != null) {
            cacheStatisticsTimer.cancel();
        }
    }

    /**
     * 将事件插入或更新到缓存中。
     *
     * @param eventId 事件ID
     * @param event 事件内容（值）
     */
    void upsertEvent(EventId eventId, Lockable<V> event) {
        // 更新事件缓存
        this.eventsBufferCache.put(eventId, event);
    }

    /**
     * 将共享缓冲区节点插入或更新到缓存中。
     *
     * @param nodeId 节点ID
     * @param entry 共享缓冲区节点
     */
    void upsertEntry(NodeId nodeId, Lockable<SharedBufferNode> entry) {
        // 更新共享缓冲区节点缓存
        this.entryCache.put(nodeId, entry);
    }

    /**
     * 从缓存和状态中移除事件。
     *
     * @param eventId 事件ID
     * @throws Exception 如果无法访问状态，则抛出异常
     */
    void removeEvent(EventId eventId) throws Exception {
        // 从事件缓存和状态中移除事件
        this.eventsBufferCache.invalidate(eventId);
        this.eventsBuffer.remove(eventId);
    }

    /**
     * 从缓存和状态中移除共享缓冲区节点。
     *
     * @param nodeId 节点ID
     * @throws Exception 如果无法访问状态，则抛出异常
     */
    void removeEntry(NodeId nodeId) throws Exception {
        // 从节点缓存和状态中移除共享缓冲区节点
        this.entryCache.invalidate(nodeId);
        this.entries.remove(nodeId);
    }

    /**
     * 总是返回共享缓冲区节点，优先从缓存中获取，如果缓存中没有，则从状态中获取。
     *
     * @param nodeId 节点ID
     * @return 返回共享缓冲区节点
     */
    Lockable<SharedBufferNode> getEntry(NodeId nodeId) {
        try {
            // 尝试从缓存中获取共享缓冲区节点
            Lockable<SharedBufferNode> lockableFromCache = entryCache.getIfPresent(nodeId);

            if (Objects.nonNull(lockableFromCache)) {
                return lockableFromCache; // 如果缓存中有节点，直接返回
            } else {
                // 如果缓存中没有节点，则从状态中获取
                Lockable<SharedBufferNode> lockableFromState = entries.get(nodeId);
                if (Objects.nonNull(lockableFromState)) {
                    // 如果状态中有节点，缓存到缓存中
                    entryCache.put(nodeId, lockableFromState);
                }
                return lockableFromState; // 返回从状态中获取的节点
            }
        } catch (Exception ex) {
            // 如果获取失败，包装异常并抛出
            throw new WrappingRuntimeException(ex);
        }
    }

    /**
     * 总是返回事件，优先从缓存中获取，如果缓存中没有，则从状态中获取。
     *
     * @param eventId 事件ID
     * @return 返回事件内容
     */
    Lockable<V> getEvent(EventId eventId) {
        try {
            // 尝试从缓存中获取事件
            Lockable<V> lockableFromCache = eventsBufferCache.getIfPresent(eventId);

            if (Objects.nonNull(lockableFromCache)) {
                return lockableFromCache; // 如果缓存中有事件，直接返回
            } else {
                // 如果缓存中没有事件，则从状态中获取
                Lockable<V> lockableFromState = eventsBuffer.get(eventId);
                if (Objects.nonNull(lockableFromState)) {
                    // 如果状态中有事件，缓存到缓存中
                    eventsBufferCache.put(eventId, lockableFromState);
                }
                return lockableFromState; // 返回从状态中获取的事件
            }
        } catch (Exception ex) {
            // 如果获取失败，包装异常并抛出
            throw new WrappingRuntimeException(ex);
        }
    }

    /**
     * 将事件和节点从缓存刷新到状态中。
     *
     * @throws Exception 如果系统无法访问状态，则抛出异常
     */
    void flushCache() throws Exception {
        // 如果缓存中有条目，则将它们刷新到状态中
        if (!entryCache.asMap().isEmpty()) {
            entries.putAll(entryCache.asMap()); // 将缓存中的条目放回状态
            entryCache.invalidateAll(); // 清空缓存
        }

        // 如果缓存中有事件，则将它们刷新到状态中
        if (!eventsBufferCache.asMap().isEmpty()) {
            eventsBuffer.putAll(eventsBufferCache.asMap()); // 将缓存中的事件放回状态
            eventsBufferCache.invalidateAll(); // 清空缓存
        }
    }

    /**
     * 获取事件计数器的迭代器（仅供测试使用）。
     *
     * @return 事件计数器的迭代器
     * @throws Exception 如果无法访问事件计数器时抛出异常
     */
    @VisibleForTesting
    Iterator<Map.Entry<Long, Integer>> getEventCounters() throws Exception {
        return eventsCount.iterator();
    }

    /**
     * 获取事件缓冲区缓存的大小（仅供测试使用）。
     *
     * @return 缓存的大小
     */
    @VisibleForTesting
    public int getEventsBufferCacheSize() {
        return (int) eventsBufferCache.size();
    }

    /**
     * 获取事件缓冲区的大小（仅供测试使用）。
     *
     * @return 事件缓冲区的大小
     * @throws Exception 如果无法访问事件缓冲区时抛出异常
     */
    @VisibleForTesting
    public int getEventsBufferSize() throws Exception {
        return Iterables.size(eventsBuffer.entries());
    }

    /**
     * 获取共享缓冲区节点的大小（仅供测试使用）。
     *
     * @return 共享缓冲区节点的大小
     * @throws Exception 如果无法访问共享缓冲区节点时抛出异常
     */
    @VisibleForTesting
    public int getSharedBufferNodeSize() throws Exception {
        return Iterables.size(entries.entries());
    }

    /**
     * 获取共享缓冲区节点缓存的大小（仅供测试使用）。
     *
     * @return 共享缓冲区节点缓存的大小
     * @throws Exception 如果无法访问共享缓冲区节点缓存时抛出异常
     */
    @VisibleForTesting
    public int getSharedBufferNodeCacheSize() throws Exception {
        return (int) entryCache.size();
    }

}
