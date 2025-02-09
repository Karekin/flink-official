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

package org.apache.flink.api.common.functions.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * `FunctionUtils` 是一个工具类，提供了一些辅助方法，用于操作 Flink 的 {@link Function} 类。
 *
 * <p>Flink 中的 Function 可能是普通的 {@link Function}，也可能是具有生命周期管理的 {@link RichFunction}。
 * 这个工具类封装了一些通用操作，如打开 (open)、关闭 (close) 以及设置运行时上下文 (RuntimeContext)。
 *
 * <p>此类被标记为 `@Internal`，仅供 Flink 内部使用，用户不应直接依赖此类。
 */
@Internal
public final class FunctionUtils {

    /**
     * 调用 `open` 方法，初始化 `Function` 实例。
     *
     * <p>如果传入的 `Function` 是 `RichFunction`，则会调用 `open` 方法进行初始化，否则不执行任何操作。
     *
     * @param function 需要初始化的 `Function` 实例
     * @param openContext Flink 提供的 `OpenContext`，用于提供 `open` 方法的上下文
     * @throws Exception 如果 `open` 方法执行失败，则抛出异常
     */
    public static void openFunction(Function function, OpenContext openContext) throws Exception {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.open(openContext);
        }
    }

    /**
     * 调用 `close` 方法，关闭 `Function` 实例。
     *
     * <p>如果传入的 `Function` 是 `RichFunction`，则会调用 `close` 方法进行资源释放，否则不执行任何操作。
     *
     * @param function 需要关闭的 `Function` 实例
     * @throws Exception 如果 `close` 方法执行失败，则抛出异常
     */
    public static void closeFunction(Function function) throws Exception {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.close();
        }
    }

    /**
     * 设置 `Function` 的运行时上下文 (`RuntimeContext`)。
     *
     * <p>如果 `Function` 继承了 `RichFunction`，则会调用 `setRuntimeContext` 方法，否则不执行任何操作。
     *
     * @param function 需要设置运行时上下文的 `Function` 实例
     * @param context  运行时上下文 (`RuntimeContext`)
     */
    public static void setFunctionRuntimeContext(Function function, RuntimeContext context) {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.setRuntimeContext(context);
        }
    }

    /**
     * 获取 `Function` 的 `RuntimeContext`（运行时上下文）。
     *
     * <p>如果 `Function` 是 `RichFunction`，则返回 `getRuntimeContext()` 结果。
     * 否则，返回默认的 `RuntimeContext`。
     *
     * @param function 需要获取 `RuntimeContext` 的 `Function` 实例
     * @param defaultContext 默认的 `RuntimeContext`
     * @return `Function` 对应的 `RuntimeContext`，如果 `Function` 不是 `RichFunction`，则返回 `defaultContext`
     */
    public static RuntimeContext getFunctionRuntimeContext(
            Function function, RuntimeContext defaultContext) {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            return richFunction.getRuntimeContext();
        } else {
            return defaultContext;
        }
    }

    /**
     * 私有构造方法，防止实例化 `FunctionUtils` 工具类。
     *
     * <p>该类仅提供静态方法，不应被实例化，因此在构造方法中抛出 `RuntimeException`。
     */
    private FunctionUtils() {
        throw new RuntimeException();
    }
}

