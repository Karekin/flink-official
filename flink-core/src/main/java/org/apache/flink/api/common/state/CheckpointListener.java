/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Public;

/**
 * 该接口通常仅用于与“外部世界”的事务性交互，例如在检查点（Checkpoint）提交外部副作用。
 * 一个典型的例子是：在检查点完成后提交外部事务。
 *
 * <h3>调用保证（Invocation Guarantees）</h3>
 *
 * <p>并不保证实现类一定会收到每个已完成或已中止的检查点的通知。
 * 在大多数情况下会收到通知，但在某些情况下可能不会，例如在检查点完成后立即发生故障或恢复的情况。
 *
 * <p>为了正确处理这一点，实现类应遵循下文所述的“检查点替代契约”（Checkpoint Subsuming Contract）。
 *
 * <h3>异常处理（Exceptions）</h3>
 *
 * <p>此接口的通知是“事后”的，即通知发生在检查点已完成或已中止之后。
 * 因此，抛出异常不会改变检查点的完成或中止状态。
 *
 * <p>如果此方法抛出异常，会导致任务或作业失败，并触发恢复机制。
 *
 * <h3>检查点替代契约（Checkpoint Subsuming Contract）</h3>
 *
 * <p>检查点 ID 递增，并且 ID 较高的检查点会取代 ID 较低的检查点。
 * 例如，当检查点 T 确认完成时，代码可以假设没有 ID 更低的检查点（T-1、T-2 等）仍然处于挂起状态。
 * <b>不会在更高 ID 的检查点提交后再提交 ID 更低的检查点。</b>
 *
 * <p>这并不意味着所有较早的检查点都已成功完成。
 * 某些检查点可能因超时或未被所有任务完全确认而失败。
 * 实现类必须表现得好像该检查点从未发生过。
 * 推荐的做法是让新检查点（ID 更高）完成时自动取代所有较早检查点（ID 较低）的完成状态。
 *
 * <p>如果在检查点完成时传播的是“偏移量”（offsets）、“水位线”（watermarks）或其他进度指标，
 * 则较新的检查点会具有更高的“偏移量”（即更大进展），从而自动取代先前的检查点。
 * 记录一个检查点 ID 的提交偏移量，并在接收到该检查点完成的通知时提交。
 *
 * <p>如果你需要在检查点完成后发布特定的工件（如文件）或确认特定 ID，可以遵循以下模式：
 *
 * <h3>用于提交工件的检查点替代模式</h3>
 *
 * <p>以下模式展示了应用程序如何在检查点提交特定工件。
 * 例如，一些算子可能需要在检查点时确认特定 ID 或发布特定文件。
 *
 * <ul>
 *   <li>在处理过程中，维护两个工件集合：
 *       <ol>
 *         <li><b>“待提交集合”（ready set）</b>：存储准备在下一个检查点提交的工件。
 *             一旦工件准备好提交，就会添加到此集合。
 *             该集合是“瞬时”的，不会存储在 Flink 的状态中。
 *         <li><b>“挂起集合”（pending set）</b>：存储正在提交的工件。
 *             这些工件实际在检查点完成时发布。
 *             该集合是一个映射 {@code Map<Long, List<Artifact>>}，其中键是检查点 ID，值是该检查点准备提交的工件列表。
 *       </ol>
 *   <li>在检查点发生时，将“待提交集合”中的工件移动到“挂起集合”，并与检查点 ID 关联。
 *       整个“挂起集合”会存储在检查点状态中。
 *   <li>在 {@code notifyCheckpointComplete()} 方法中，
 *       发布所有检查点 ID 之前的“挂起集合”中的工件，并将其从集合中移除。
 * </ul>
 *
 * <p>这样，即使某些检查点未完成或检查点完成的通知丢失，工件仍将在下一个成功的检查点完成时发布。
 */
@Public
public interface CheckpointListener {

    /**
     * 通知监听器指定的检查点 {@code checkpointId} 已完成并被提交。
     *
     * <p>这些通知是“尽力而为”（best effort）的，这意味着可能会有部分通知被跳过。
     * 因此，实现类需要遵循“检查点替代契约”以正确处理此情况。
     * 详细信息请参考 {@link CheckpointListener} 类的 JavaDoc 说明。
     *
     * <p>请注意，检查点可能会重叠，因此不能假设 {@code notifyCheckpointComplete()} 方法
     * 始终针对最近的检查点（即最近触发的检查点）。
     * 它可能适用于更早触发的检查点。
     * 正确实现“检查点替代契约”可以妥善处理此情况。
     *
     * <p>请注意，此方法抛出的异常不会导致已完成的检查点被撤销。
     * 但抛出异常通常会导致任务/作业失败，并触发恢复机制。
     *
     * @param checkpointId 已完成的检查点 ID。
     * @throws Exception 该方法可以抛出异常，异常会导致任务/作业失败并触发恢复。
     *     但不会导致检查点被撤销。
     */
    void notifyCheckpointComplete(long checkpointId) throws Exception;

    /**
     * 当分布式检查点被中止时，此方法将作为通知被调用。
     *
     * <p><b>重要提示：</b>检查点被中止并不意味着在前一个检查点和当前中止的检查点之间生成的数据和工件需要被丢弃。
     * 正确的行为是：假设该检查点从未触发过，下一个成功的检查点将覆盖更长的时间跨度。
     * 详细信息请参考 {@link CheckpointListener} 类的“检查点替代契约”部分。
     *
     * <p>此通知是“尽力而为”的（best effort），意味着可能会有部分通知被跳过。
     *
     * <p>通常情况下，不需要实现该方法。
     * 由于该通知不保证每次都会触发，并且不应导致数据丢弃（遵循“检查点替代契约”），
     * 该方法主要用于清理辅助资源。
     * 例如，可以在检查点失败时主动清除本地的每检查点状态缓存。
     *
     * @param checkpointId 被中止的检查点 ID。
     * @throws Exception 该方法可以抛出异常，异常会导致任务/作业失败并触发恢复机制。
     */
    default void notifyCheckpointAborted(long checkpointId) throws Exception {}
}

