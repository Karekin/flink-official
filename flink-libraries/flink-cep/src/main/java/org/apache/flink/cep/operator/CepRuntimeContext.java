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

package org.apache.flink.cep.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * `CepRuntimeContext` 是 `RuntimeContext` 的一个包装类，专门用于 Flink CEP 组件。
 *
 * <p>此类主要为 **PatternProcessFunction** 和 **IterativeConditionFunction** 提供必要的运行时上下文信息，
 * 但为了简化，禁用了某些不必要的功能，如 **状态访问**、**累加器 (Accumulators)**、**广播变量 (Broadcast Variables)**
 * 和 **分布式缓存 (Distributed Cache)**。
 */
@Internal
class CepRuntimeContext implements RuntimeContext {

    /** Flink 运行时上下文对象 */
    private final RuntimeContext runtimeContext;

    /**
     * 构造方法，接收 Flink 的 `RuntimeContext` 并进行封装。
     *
     * @param runtimeContext Flink 运行时上下文
     */
    CepRuntimeContext(final RuntimeContext runtimeContext) {
        this.runtimeContext = checkNotNull(runtimeContext);
    }

    /**
     * 获取算子的度量指标（Metrics），用于收集和监控算子执行情况。
     *
     * @return 算子的 `OperatorMetricGroup`
     */
    @Override
    public OperatorMetricGroup getMetricGroup() {
        return runtimeContext.getMetricGroup();
    }

    /**
     * 获取作业的执行配置。
     *
     * @return 作业的 `ExecutionConfig`
     * @deprecated 该方法已被标记为弃用
     */
    @Override
    @Deprecated
    public ExecutionConfig getExecutionConfig() {
        return runtimeContext.getExecutionConfig();
    }

    /**
     * 创建指定类型的 `TypeSerializer`，用于序列化数据。
     *
     * @param typeInformation 需要序列化的类型信息
     * @param <T> 数据类型
     * @return `TypeSerializer<T>` 序列化器
     */
    @Override
    public <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInformation) {
        return runtimeContext.createSerializer(typeInformation);
    }

    /**
     * 获取全局作业参数，通常用于获取用户定义的作业配置。
     *
     * @return 包含作业参数的 `Map`
     */
    @Override
    public Map<String, String> getGlobalJobParameters() {
        return runtimeContext.getGlobalJobParameters();
    }

    /**
     * 判断是否启用了对象重用 (Object Reuse)。
     *
     * @return `true` 表示启用对象重用，`false` 表示未启用
     */
    @Override
    public boolean isObjectReuseEnabled() {
        return runtimeContext.isObjectReuseEnabled();
    }

    /**
     * 获取用户代码的 `ClassLoader`，用于动态加载类。
     *
     * @return 用户代码的 `ClassLoader`
     */
    @Override
    public ClassLoader getUserCodeClassLoader() {
        return runtimeContext.getUserCodeClassLoader();
    }

    /**
     * 向 `ClassLoader` 注册释放钩子 (Release Hook)，用于作业关闭时清理资源。
     *
     * @param releaseHookName 释放钩子的名称
     * @param releaseHook 需要执行的释放逻辑
     */
    @Override
    public void registerUserCodeClassLoaderReleaseHookIfAbsent(
            String releaseHookName, Runnable releaseHook) {
        runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(releaseHookName, releaseHook);
    }

    /**
     * 获取分布式缓存 (Distributed Cache)，在 `CepRuntimeContext` 中已被禁用。
     *
     * @return `DistributedCache` 对象
     */
    @Override
    public DistributedCache getDistributedCache() {
        return runtimeContext.getDistributedCache();
    }

    /**
     * 获取外部资源的信息，例如 GPU 计算资源。
     *
     * @param resourceName 资源名称
     * @return 资源信息的 `Set`
     */
    @Override
    public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
        return runtimeContext.getExternalResourceInfos(resourceName);
    }

    /**
     * 获取当前作业的信息，包括 Job ID、名称等。
     *
     * @return `JobInfo` 对象
     */
    @Override
    public JobInfo getJobInfo() {
        return runtimeContext.getJobInfo();
    }

    /**
     * 获取当前任务的信息，例如任务名称、并行度等。
     *
     * @return `TaskInfo` 对象
     */
    @Override
    public TaskInfo getTaskInfo() {
        return runtimeContext.getTaskInfo();
    }

    // -----------------------------------------------------------------------------------
    // 不支持的功能（CEP 运行时上下文不提供以下功能）
    // -----------------------------------------------------------------------------------

    /**
     * 禁止使用累加器 (Accumulators)。
     */
    @Override
    public <V, A extends Serializable> void addAccumulator(
            final String name, final Accumulator<V, A> accumulator) {
        throw new UnsupportedOperationException("累加器 (Accumulators) 不受支持。");
    }

    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(final String name) {
        throw new UnsupportedOperationException("累加器 (Accumulators) 不受支持。");
    }

    @Override
    public IntCounter getIntCounter(final String name) {
        throw new UnsupportedOperationException("Int 计数器不受支持。");
    }

    @Override
    public LongCounter getLongCounter(final String name) {
        throw new UnsupportedOperationException("Long 计数器不受支持。");
    }

    @Override
    public DoubleCounter getDoubleCounter(final String name) {
        throw new UnsupportedOperationException("Double 计数器不受支持。");
    }

    @Override
    public Histogram getHistogram(final String name) {
        throw new UnsupportedOperationException("直方图 (Histogram) 不受支持。");
    }

    /**
     * 禁止使用广播变量 (Broadcast Variables)。
     */
    @Override
    public boolean hasBroadcastVariable(final String name) {
        throw new UnsupportedOperationException("广播变量 (Broadcast Variables) 不受支持。");
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(final String name) {
        throw new UnsupportedOperationException("广播变量 (Broadcast Variables) 不受支持。");
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(
            final String name, final BroadcastVariableInitializer<T, C> initializer) {
        throw new UnsupportedOperationException("广播变量 (Broadcast Variables) 不受支持。");
    }

    /**
     * 禁止使用 Flink 状态 (State)。
     */
    @Override
    public <T> ValueState<T> getState(final ValueStateDescriptor<T> stateProperties) {
        throw new UnsupportedOperationException("Flink 状态管理 (State) 不受支持。");
    }

    @Override
    public <T> ListState<T> getListState(final ListStateDescriptor<T> stateProperties) {
        throw new UnsupportedOperationException("Flink 状态管理 (State) 不受支持。");
    }

    @Override
    public <T> ReducingState<T> getReducingState(final ReducingStateDescriptor<T> stateProperties) {
        throw new UnsupportedOperationException("Flink 状态管理 (State) 不受支持。");
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            final AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        throw new UnsupportedOperationException("Flink 状态管理 (State) 不受支持。");
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(final MapStateDescriptor<UK, UV> stateProperties) {
        throw new UnsupportedOperationException("Flink 状态管理 (State) 不受支持。");
    }
}

