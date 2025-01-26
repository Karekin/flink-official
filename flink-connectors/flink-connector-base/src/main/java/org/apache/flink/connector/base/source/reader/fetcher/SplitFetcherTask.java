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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;

/**
 * 一个接口，类似于 {@link Runnable}，但允许抛出异常并支持唤醒操作。
 */
@PublicEvolving
public interface SplitFetcherTask {

    /**
     * 执行任务的逻辑。该方法允许在唤醒时抛出中断异常，但实现类可以选择不抛出异常。
     *
     * <p>建议实现逻辑优雅地完成任务，并通过返回一个布尔值来指示是否所有任务都已完成。
     * 如果任务未完全完成，可以通过多次调用该方法来继续处理。
     *
     * @return 一个布尔值，表示任务是否成功完成运行。
     *         - 如果返回 true，表示任务已成功完成，不需要进一步调用。
     *         - 如果返回 false，表示还有任务未完成，需要继续调用该方法。
     * @throws IOException 当执行的 I/O 操作失败时抛出该异常。
     */
    boolean run() throws IOException;

    /**
     * 唤醒正在运行的线程。
     *
     * <p>当任务逻辑因某些条件阻塞时，调用此方法可中断阻塞状态，从而使线程可以继续运行。
     */
    void wakeUp();
}
