/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * {@link CoordinatorStore} 用于在多个 {@link OperatorCoordinator} 实例之间共享信息。
 * 典型的应用场景是汇总或聚合由不同数据源发出的最新水位线（watermark），以实现水位线对齐（watermark alignment）。
 *
 * <p>所有该接口的实现都必须保证所有操作的原子性，以确保并发访问时的数据一致性。</p>
 */
@ThreadSafe  // 线程安全的注解，表示该接口的实现需要保证线程安全
@Internal    // 内部使用的注解，表示该接口仅供框架内部使用，不推荐外部用户依赖
public interface CoordinatorStore {

    /**
     * 检查指定的键是否存在于存储中。
     *
     * @param key 要查询的键
     * @return 如果键存在，则返回 true；否则返回 false
     */
    boolean containsKey(Object key);

    /**
     * 根据键获取存储中的值。
     *
     * @param key 要获取的键
     * @return 存储中与该键关联的值，如果键不存在则返回 null
     */
    Object get(Object key);

    /**
     * 如果键不存在，则将指定的键值对存入存储，并返回存储的值。
     * 如果键已经存在，则返回当前存储的值，不进行更新。
     *
     * @param key   要存入的键
     * @param value 要存入的值
     * @return 存储中最终的值，如果键已存在则返回已有值，否则返回新存入的值
     */
    Object putIfAbsent(Object key, Object value);

    /**
     * 如果键存在，则对其值应用指定的 remappingFunction 函数，并更新存储中的值。
     *
     * @param key               要更新的键
     * @param remappingFunction 用于计算新值的映射函数，接收当前值并返回新的值
     * @return 计算后的新值，如果键不存在则返回 null
     */
    Object computeIfPresent(Object key, BiFunction<Object, Object, Object> remappingFunction);

    /**
     * 无论键是否存在，都对其值应用指定的 mappingFunction 函数，并更新存储中的值。
     * 如果键不存在，则 mappingFunction 的第二个参数（旧值）为 null。
     *
     * @param key            要更新的键
     * @param mappingFunction 用于计算新值的映射函数，接收当前值（可能为 null）并返回新的值
     * @return 计算后的新值
     */
    Object compute(Object key, BiFunction<Object, Object, Object> mappingFunction);

    /**
     * 对指定的键应用一个函数，并返回计算后的结果，而不会修改存储中的值。
     *
     * @param key      要应用函数的键
     * @param consumer 计算逻辑，接收存储中的值并返回计算结果
     * @param <R>      计算结果的类型
     * @return 计算后的结果
     */
    <R> R apply(Object key, Function<Object, R> consumer);
}

