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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * 代表变更日志中的变更集合。
 *
 * @see RowKind 变更的类型，包含插入、更新、删除等。
 */
@PublicEvolving
public final class ChangelogMode {

    // 只包含插入类型的变更日志
    private static final ChangelogMode INSERT_ONLY =
            ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();

    // 包含插入、更新后、删除三种类型的变更日志
    private static final ChangelogMode UPSERT =
            ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)    // 插入
                    .addContainedKind(RowKind.UPDATE_AFTER) // 更新（更新后的数据）
                    .addContainedKind(RowKind.DELETE)   // 删除
                    .build();

    // 包含所有变更类型：插入、更新前、更新后、删除
    private static final ChangelogMode ALL =
            ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)      // 插入
                    .addContainedKind(RowKind.UPDATE_BEFORE) // 更新前的数据
                    .addContainedKind(RowKind.UPDATE_AFTER)  // 更新后的数据
                    .addContainedKind(RowKind.DELETE)       // 删除
                    .build();

    private final Set<RowKind> kinds; // 存储变更的类型

    // 私有构造函数，创建一个包含特定变更类型的变更日志
    private ChangelogMode(Set<RowKind> kinds) {
        // 确保变更日志至少包含一个变更类型
        Preconditions.checkArgument(
                kinds.size() > 0, "At least one kind of row should be contained in a changelog.");
        this.kinds = Collections.unmodifiableSet(kinds); // 不可修改的集合
    }

    /**
     * 获取一个只包含插入类型变更的变更日志。
     */
    public static ChangelogMode insertOnly() {
        return INSERT_ONLY;
    }

    /**
     * 获取一个包含插入、更新后、删除类型的变更日志，表示幂等更新的变更日志，
     * 并且不包含更新前的数据（{@link RowKind#UPDATE_BEFORE}）。
     */
    public static ChangelogMode upsert() {
        return UPSERT;
    }

    /**
     * 获取一个可以包含所有变更类型（插入、更新前、更新后、删除）的变更日志。
     */
    public static ChangelogMode all() {
        return ALL;
    }

    /**
     * 返回一个构建器，可以用于配置并创建 {@link ChangelogMode} 实例。
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    // 获取变更日志中包含的所有变更类型
    public Set<RowKind> getContainedKinds() {
        return kinds;
    }

    // 判断变更日志中是否包含指定的变更类型
    public boolean contains(RowKind kind) {
        return kinds.contains(kind);
    }

    // 判断变更日志是否只包含某个特定的变更类型
    public boolean containsOnly(RowKind kind) {
        return kinds.size() == 1 && kinds.contains(kind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangelogMode that = (ChangelogMode) o;
        return kinds.equals(that.kinds); // 比较变更类型集合是否相等
    }

    @Override
    public int hashCode() {
        return Objects.hash(kinds); // 计算哈希值
    }

    @Override
    public String toString() {
        return kinds.toString(); // 返回变更类型集合的字符串表示
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 用于配置并创建 {@link ChangelogMode} 实例的构建器类。
     * 该类支持流式构建模式，可以方便地逐步添加包含的变更类型。
     */
    @PublicEvolving
    public static class Builder {

        private final Set<RowKind> kinds = EnumSet.noneOf(RowKind.class); // 存储变更类型

        private Builder() {
            // 默认构造函数，允许流式定义
        }

        /**
         * 向变更日志中添加一个变更类型
         * @param kind 变更类型
         * @return 当前构建器实例
         */
        public Builder addContainedKind(RowKind kind) {
            this.kinds.add(kind);
            return this; // 返回构建器自身，以支持链式调用
        }

        /**
         * 构建并返回一个 {@link ChangelogMode} 实例
         * @return 配置好的变更日志模式
         */
        public ChangelogMode build() {
            return new ChangelogMode(kinds); // 使用已配置的变更类型集合创建 ChangelogMode 实例
        }
    }
}

