/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * {@link OutputTag} 是 Flink 中用于定义 **侧输出流** (Side Output) 的标记类。
 *
 * <p>在 Flink 任务中，侧输出流 (Side Output) 允许从一个 `DataStream` 任务中
 * 发送不同类型的数据到不同的 `OutputTag`，用于处理迟到数据或分流不同类别的数据。
 *
 * <p>**使用示例:**
 *
 * <pre>{@code
 * // 创建一个用于存储迟到数据的 OutputTag
 * OutputTag<Tuple2<String, Long>> lateDataTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
 *
 * // 在主流处理逻辑中，将迟到的数据发送到侧输出
 * mainStream.process(new ProcessFunction<Tuple2<String, Long>, String>() {
 *     @Override
 *     public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) {
 *         if (isLate(value)) {
 *             ctx.output(lateDataTag, value);
 *         } else {
 *             out.collect(value.f0);
 *         }
 *     }
 * });
 * }</pre>
 *
 * <p>**注意事项:**
 * - `OutputTag` 必须是 **匿名内部类**，以便 Flink 能够推导出 `TypeInformation`。
 * - 不能使用泛型类型变量 (`Tuple2<A, B>`)，否则会导致类型推导失败。
 *
 * @param <T> 侧输出流中的数据类型
 */
@PublicEvolving
public class OutputTag<T> implements Serializable {

    private static final long serialVersionUID = 2L;

    /** 侧输出流的唯一标识符 */
    private final String id;

    /** 侧输出流的数据类型信息 */
    private final TypeInformation<T> typeInfo;

    /**
     * 使用指定的 `id` 创建一个 `OutputTag`，用于标识侧输出流。
     *
     * @param id 侧输出流的唯一标识符 (不能为 null 或 空字符串)。
     */
    public OutputTag(String id) {
        Preconditions.checkNotNull(id, "OutputTag id 不能为空.");
        Preconditions.checkArgument(!id.isEmpty(), "OutputTag id 不能是空字符串.");
        this.id = id;

        try {
            // 通过 Flink 的 `TypeExtractor` 自动推导 `TypeInformation`
            this.typeInfo = TypeExtractor.createTypeInfo(this, OutputTag.class, getClass(), 0);
        } catch (InvalidTypesException e) {
            throw new InvalidTypesException(
                    "无法推导 OutputTag 的 TypeInformation。\n"
                            + "最常见的原因是：\n"
                            + "  1. 忘记将 OutputTag 定义为匿名内部类。\n"
                            + "  2. 使用了泛型变量，例如 'Tuple2<A, B>'，Flink 无法推导其类型。",
                    e);
        }
    }

    /**
     * 使用指定的 `id` 和 `TypeInformation` 创建 `OutputTag`，用于标识侧输出流。
     *
     * @param id 侧输出流的唯一标识符 (不能为 null 或 空字符串)。
     * @param typeInfo 侧输出流的数据类型信息。
     */
    public OutputTag(String id, TypeInformation<T> typeInfo) {
        Preconditions.checkNotNull(id, "OutputTag id 不能为空.");
        Preconditions.checkArgument(!id.isEmpty(), "OutputTag id 不能是空字符串.");
        this.id = id;
        this.typeInfo = Preconditions.checkNotNull(typeInfo, "TypeInformation 不能为空.");
    }

    /**
     * 判断 `other` 是否归属于 `owner` 的 `OutputTag`。
     *
     * @param owner 所属的 `OutputTag` (可能为 null)
     * @param other 需要检查的 `OutputTag`
     * @return 如果 `other` 与 `owner` 相等，则返回 `true`，否则返回 `false`
     */
    public static boolean isResponsibleFor(
            @Nullable OutputTag<?> owner, @Nonnull OutputTag<?> other) {
        return other.equals(owner);
    }

    // ------------------------------------------------------------------------

    /**
     * 获取 `OutputTag` 的唯一标识符。
     *
     * @return 侧输出流的 `id`
     */
    public String getId() {
        return id;
    }

    /**
     * 获取 `OutputTag` 的数据类型信息。
     *
     * @return 侧输出流的数据类型 `TypeInformation`
     */
    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }

    // ------------------------------------------------------------------------

    /**
     * 比较两个 `OutputTag` 是否相等，仅基于 `id` 进行判断。
     *
     * @param obj 需要比较的对象
     * @return 如果 `obj` 也是 `OutputTag` 且 `id` 相等，则返回 `true`，否则返回 `false`
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || !(obj instanceof OutputTag)) {
            return false;
        }
        OutputTag other = (OutputTag) obj;
        return Objects.equals(this.id, other.id);
    }

    /**
     * 计算 `OutputTag` 的哈希值，仅基于 `id` 计算。
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * 生成 `OutputTag` 的字符串表示，包含 `TypeInformation` 和 `id`。
     *
     * @return `OutputTag` 的字符串表示
     */
    @Override
    public String toString() {
        return "OutputTag(" + getTypeInfo() + ", " + id + ")";
    }
}

