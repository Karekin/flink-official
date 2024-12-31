/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 该类是所有实现 Exactly-Once 语义的 {@link SinkFunction} 的推荐基类。
 * 它基于 {@link CheckpointedFunction} 和 {@link CheckpointListener} 实现了两阶段提交算法。
 * 用户需要提供自定义的事务类型 {@code TXN}，并实现处理该事务的抽象方法。
 *
 * @param <IN>     输入数据的类型。
 * @param <TXN>    事务句柄的类型，用于存储处理事务所需的所有信息。
 * @param <CONTEXT> 上下文类型，在 {@link TwoPhaseCommitSinkFunction} 实例的所有调用中共享。
 */
@PublicEvolving
public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> extends RichSinkFunction<IN>
        implements CheckpointedFunction, CheckpointListener {


    private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitSinkFunction.class);

    // 用于存储挂起提交的事务映射，键为检查点 ID，值为事务的持有者对象。
    protected final LinkedHashMap<Long, TransactionHolder<TXN>> pendingCommitTransactions =
            new LinkedHashMap<>();

    // 用户上下文，表示共享的事务上下文信息。
    protected transient Optional<CONTEXT> userContext;

    // 存储算子状态（包括挂起事务和上下文）的 Flink 状态变量。
    protected transient ListState<State<TXN, CONTEXT>> state;

    // 系统时钟，用于事务超时计算。
    private final Clock clock;

    // 状态描述符，用于序列化和反序列化算子状态。
    private final ListStateDescriptor<State<TXN, CONTEXT>> stateDescriptor;

    /**
     * 当前事务的持有者。
     * 包括：
     * 1. 正常事务：在任务正常运行期间的事务。
     * 2. 空值：当任务已完成时。
     */
    @Nullable private TransactionHolder<TXN> currentTransactionHolder;

    /** 指定事务保持打开的最大时间。 */
    private long transactionTimeout = Long.MAX_VALUE;

    /** 如果设置为 true，则在事务超时后，捕获但不传播 {@link #recoverAndCommit(Object)} 中抛出的异常。 */
    private boolean ignoreFailuresAfterTransactionTimeout;

    /** 当事务的耗时达到 transactionTimeout 的指定百分比时，记录警告日志。取值范围为 [0,1]，负值禁用警告。 */
    private double transactionTimeoutWarningRatio = -1;

    /** 标记当前 SinkFunction 以及其任务是否完成。 */
    private boolean finished = false;


    /**
     * 使用默认的 {@link ListStateDescriptor} 构造函数。
     * 用户可以通过 {@link TypeInformation} 或 {@link TypeHint} 提供事务和上下文的序列化器。
     *
     * 示例：
     * TwoPhaseCommitSinkFunction(TypeInformation.of(new TypeHint<State<TXN, CONTEXT>>() {}));
     *
     * @param transactionSerializer 事务类型的序列化器。
     * @param contextSerializer     上下文类型的序列化器。
     */
    public TwoPhaseCommitSinkFunction(
            TypeSerializer<TXN> transactionSerializer, TypeSerializer<CONTEXT> contextSerializer) {
        this(transactionSerializer, contextSerializer, Clock.systemUTC());
    }

    /**
     * 带有自定义时钟的构造函数。
     *
     * @param transactionSerializer 事务类型的序列化器。
     * @param contextSerializer     上下文类型的序列化器。
     * @param clock                 自定义时钟（通常用于测试）。
     */
    @VisibleForTesting
    TwoPhaseCommitSinkFunction(
            TypeSerializer<TXN> transactionSerializer,
            TypeSerializer<CONTEXT> contextSerializer,
            Clock clock) {
        this.stateDescriptor =
                new ListStateDescriptor<>(
                        "state", new StateSerializer<>(transactionSerializer, contextSerializer));
        this.clock = clock;
    }


    /**
     * 初始化用户上下文的方法。
     *
     * 用户可以通过重写此方法为每个实例化的 SinkFunction 初始化上下文。
     * 默认实现返回一个空的 {@link Optional}，即无上下文。
     *
     * @return 一个 {@link Optional<CONTEXT>} 对象，表示初始化的用户上下文（如果存在）。
     */
    protected Optional<CONTEXT> initializeUserContext() {
        return Optional.empty();
    }

    /**
     * 获取当前的用户上下文。
     *
     * 用户上下文通常是跨事务共享的信息。此方法返回当前存储的用户上下文。
     * 如果用户上下文未被初始化或不存在，将返回 {@link Optional#empty()}。
     *
     * @return 当前用户上下文（可能为空）。
     */
    protected Optional<CONTEXT> getUserContext() {
        return userContext;
    }

    /**
     * 获取当前的事务句柄。
     *
     * 当前事务句柄是指当前事务的唯一标识符或操作句柄。
     * 如果当前事务持有者为 null，则表示没有正在进行的事务，返回 null。
     *
     * @return 当前事务句柄，如果没有活动事务则返回 null。
     */
    @Nullable
    protected TXN currentTransaction() {
        return currentTransactionHolder == null ? null : currentTransactionHolder.handle;
    }

    /**
     * 获取所有挂起的事务的流式视图。
     *
     * 挂起的事务是指那些已被触发但尚未提交的事务。
     * 此方法将挂起的事务（按检查点 ID 分组）转换为一个流（Stream），
     * 每个元素是检查点 ID 和对应事务句柄的键值对。
     *
     * @return 包含挂起事务的 {@link Stream}，每个元素是检查点 ID 和事务句柄的键值对。
     */
    @Nonnull
    protected Stream<Map.Entry<Long, TXN>> pendingTransactions() {
        return pendingCommitTransactions.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().handle));
    }


    // ------ methods that should be implemented in child class to support two phase commit
    // algorithm ------

    /** Write value within a transaction. */
    protected abstract void invoke(TXN transaction, IN value, Context context) throws Exception;

    /**
     * 开启一个新的事务。
     *
     * 用户需要实现此方法以创建并初始化一个事务。该方法在每次新的检查点触发时调用。
     *
     * @return 创建的新事务。
     * @throws Exception 如果事务创建失败，抛出异常。
     */
    protected abstract TXN beginTransaction() throws Exception;

    /**
     * 预提交事务。
     *
     * 该方法的实现需要确保事务在未来可以被安全地提交。通常在此步骤中，用户会将缓冲的数据刷新到磁盘。
     * 注意：事务在预提交之后，仍可能被中止。
     *
     * @param transaction 待预提交的事务。
     * @throws Exception 如果预提交失败，抛出异常。
     */
    protected abstract void preCommit(TXN transaction) throws Exception;

    /**
     * 提交已预提交的事务。
     *
     * 如果该方法失败，Flink 会重启应用程序，并再次调用 {@link #recoverAndCommit(Object)} 来重试提交。
     * 提交后，事务被视为完成且不可再修改。
     *
     * @param transaction 待提交的事务。
     */
    protected abstract void commit(TXN transaction);


    /**
     * 在故障恢复时调用以提交已预提交的事务。
     *
     * 该方法需要确保最终能够成功。如果该方法持续失败，Flink 会不断重试。
     * 如果无法最终成功，将会导致数据丢失。
     *
     * @param transaction 需要恢复并提交的事务。
     */
    protected void recoverAndCommit(TXN transaction) {
        commit(transaction);
    }

    /**
     * 中止事务。
     *
     * 如果事务在提交前出现异常或任务失败，需要调用该方法中止事务，确保未提交的数据不会生效。
     *
     * @param transaction 待中止的事务。
     */
    protected abstract void abort(TXN transaction);

    /**
     * 在故障恢复时调用以中止事务。
     *
     * 如果事务在故障时未成功提交，需要调用该方法中止事务。
     *
     * @param transaction 需要恢复并中止的事务。
     */
    protected void recoverAndAbort(TXN transaction) {
        abort(transaction);
    }

    /**
     * 子类的回调方法，用于在恢复每个用户上下文后调用。
     *
     * 当任务从故障中恢复时，会恢复挂起事务和用户上下文。此方法允许子类在恢复用户上下文后执行额外的操作。
     *
     * @param handledTransactions 已经提交或中止的事务集合，这些事务不需要进一步处理。
     */
    protected void finishRecoveringContext(Collection<TXN> handledTransactions) {}

    /**
     * 在数据处理结束时调用的方法。
     *
     * <p>该方法的主要作用是刷新所有剩余的缓冲数据。如果该方法抛出异常，整个管道会被认为失败，
     * 因为最后的数据未被正确处理。通常在此方法中，将缓冲的元素写入当前事务，并在最后一个检查点中提交。
     *
     * @param transaction 当前事务的句柄，可以为 null。
     */
    protected void finishProcessing(@Nullable TXN transaction) {}

// ------ 以上方法是 CheckPointedFunction 和 CheckpointListener 接口的入口方法 ------

    /**
     * 不应由子类实现的最终方法。
     *
     * <p>该方法是 Flink 的 `RichSinkFunction` 中的 `invoke` 方法的重载版本。
     * 它被标记为 `final`，以确保子类不会覆盖它。实际的数据处理逻辑应该放在另一个 `invoke` 方法中。
     *
     * @param value 输入的数据。
     * @throws Exception 如果数据处理失败，抛出异常。
     */
    @Override
    public final void invoke(IN value) throws Exception {}

    /**
     * 数据处理的主要入口方法。
     *
     * <p>在数据流的每个元素到达时，该方法会被调用。
     * 它会检查当前事务是否存在，如果存在，则调用带有事务句柄的 `invoke` 方法进行处理。
     * 如果当前事务为 null，则抛出异常，因为没有活动事务的情况下不应进行数据处理。
     *
     * @param value   输入的数据。
     * @param context 上下文信息，包含时间戳、事件时间等元信息。
     * @throws Exception 如果数据处理失败，抛出异常。
     */
    @Override
    public final void invoke(IN value, Context context) throws Exception {
        // 获取当前的事务句柄
        TXN currentTransaction = currentTransaction();
        // 检查事务是否存在，如果不存在则抛出异常
        checkNotNull(
                currentTransaction,
                "两阶段提交 SinkFunction 的事务为空时不应被调用！");
        // 调用子类实现的事务处理逻辑
        invoke(currentTransaction, value, context);
    }


    /**
     * 在任务完成时调用的方法。
     *
     * <p>当数据流处理完成后，Flink 会调用此方法通知 SinkFunction 完成任务。
     * 该方法会标记当前任务为已完成（`finished = true`），并通过调用 `finishProcessing` 来处理
     * 当前未处理的事务。如果处理失败，将抛出异常。
     *
     * @throws Exception 如果任务完成时发生错误。
     */
    @Override
    public final void finish() throws Exception {
        // 标记任务已完成
        finished = true;
        // 完成处理当前事务（例如清理缓冲区数据）
        finishProcessing(currentTransaction());
    }

    /**
     * 在检查点完成时调用的方法。
     *
     * <p>该方法是 Flink 的回调方法，用于通知 SinkFunction 某个检查点已经成功完成，
     * 表示所有的状态数据和挂起事务都被安全地持久化了。此时可以安全地提交与该检查点相关的事务。
     *
     * 处理逻辑包含以下几种场景：
     * 1. 当前检查点完成，只有一个挂起的事务需要提交，这是最常见的情况。
     * 2. 存在多个挂起的事务需要提交（例如，前一个检查点失败，当前检查点覆盖了之前的状态）。
     * 3. 检查点完成通知可能不是最新的（例如，由于延迟或重叠检查点的影响）。
     *
     * <p>确保无论何种情况，都不会丢失挂起的事务。
     *
     * @param checkpointId 已完成的检查点 ID。
     * @throws Exception 如果提交事务时发生错误。
     */
    @Override
    public final void notifyCheckpointComplete(long checkpointId) throws Exception {
        // 遍历挂起的事务映射表
        Iterator<Map.Entry<Long, TransactionHolder<TXN>>> pendingTransactionIterator =
                pendingCommitTransactions.entrySet().iterator();

        // 用于捕获第一个提交失败的异常
        Throwable firstError = null;

        // 遍历挂起的事务，并提交与当前检查点 ID 或更早相关的事务
        while (pendingTransactionIterator.hasNext()) {
            Map.Entry<Long, TransactionHolder<TXN>> entry = pendingTransactionIterator.next();
            Long pendingTransactionCheckpointId = entry.getKey(); // 挂起事务对应的检查点 ID
            TransactionHolder<TXN> pendingTransaction = entry.getValue(); // 挂起的事务

            // 如果挂起事务的检查点 ID 大于当前检查点 ID，则跳过
            if (pendingTransactionCheckpointId > checkpointId) {
                continue;
            }

            // 日志记录，通知开始提交事务
            LOG.info(
                    "{} - 检查点 {} 完成，提交事务 {}（来自检查点 {}）",
                    name(),
                    checkpointId,
                    pendingTransaction,
                    pendingTransactionCheckpointId);

            // 如果事务接近超时时间，记录警告日志
            logWarningIfTimeoutAlmostReached(pendingTransaction);

            try {
                // 提交事务
                commit(pendingTransaction.handle);
            } catch (Throwable t) {
                // 捕获提交失败的第一个异常
                if (firstError == null) {
                    firstError = t;
                }
            }

            // 提交成功后，记录调试日志
            LOG.debug("{} - 已提交事务（来自检查点 {}）", name(), pendingTransaction);

            // 提交成功后，从挂起事务列表中移除
            pendingTransactionIterator.remove();
        }

        // 如果存在提交失败的异常，抛出并记录
        if (firstError != null) {
            throw new FlinkRuntimeException(
                    "提交事务时发生错误，记录第一个遇到的错误", firstError);
        }
    }


    /**
     * 在检查点被中止时调用。
     *
     * <p>该方法是 Flink 的回调方法，用于通知 SinkFunction 某个检查点未成功完成（被中止）。
     * 默认实现为空，因为两阶段提交的事务模型通常不需要特殊处理中止的检查点。
     * 如果需要处理中止的检查点，可以在子类中覆盖此方法。
     *
     * @param checkpointId 被中止的检查点 ID。
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // 默认实现为空，可根据需求在子类中覆盖
    }

    /**
     * 在触发检查点时调用，用于保存当前状态。
     *
     * <p>该方法是 Flink 的 `CheckpointedFunction` 接口的实现。
     * 每当检查点触发时，Flink 会调用此方法以保存当前的算子状态和事务信息。
     * 这相当于两阶段提交中的“预提交”阶段：
     * - 将当前事务标记为挂起，保存到 `pendingCommitTransactions` 中。
     * - 更新状态存储以包含当前事务及其上下文。
     * - 如果任务未完成，开启一个新的事务。
     *
     * @param context 检查点上下文，包含检查点 ID 和其他元信息。
     * @throws Exception 如果保存状态或预提交事务失败，则抛出异常。
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 获取当前检查点的 ID
        long checkpointId = context.getCheckpointId();

        // 记录日志，通知检查点触发并准备刷新事务
        LOG.debug(
                "{} - 检查点 {} 触发，刷新当前事务 '{}'",
                name(),
                checkpointId,
                currentTransactionHolder);

        // 如果当前存在事务，则执行预提交
        if (currentTransactionHolder != null) {
            // 预提交事务，将缓冲数据刷入持久存储（例如磁盘）
            preCommit(currentTransactionHolder.handle);
            // 将当前事务存储到挂起事务列表中，等待检查点完成后提交
            pendingCommitTransactions.put(checkpointId, currentTransactionHolder);
            LOG.debug("{} - 存储挂起事务 {}", name(), pendingCommitTransactions);
        }

        // 如果任务未完成，则开始一个新的事务
        if (!finished) {
            currentTransactionHolder = beginTransactionInternal();
        } else {
            // 如果任务已完成，则不再开启新事务
            currentTransactionHolder = null;
        }

        // 记录日志，通知新的事务已开始
        LOG.debug("{} - 开启新事务 '{}'", name(), currentTransactionHolder);

        // 更新状态存储
        // 状态包含当前事务、所有挂起事务以及用户上下文
        state.update(
                Collections.singletonList(
                        new State<>(
                                this.currentTransactionHolder, // 当前事务
                                new ArrayList<>(pendingCommitTransactions.values()), // 挂起事务列表
                                userContext))); // 用户上下文
    }


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // when we are restoring state with pendingCommitTransactions, we don't really know whether
        // the
        // transactions were already committed, or whether there was a failure between
        // completing the checkpoint on the master, and notifying the writer here.

        // (the common case is actually that is was already committed, the window
        // between the commit on the master and the notification here is very small)

        // it is possible to not have any transactions at all if there was a failure before
        // the first completed checkpoint, or in case of a scale-out event, where some of the
        // new task do not have and transactions assigned to check)

        // we can have more than one transaction to check in case of a scale-in event, or
        // for the reasons discussed in the 'notifyCheckpointComplete()' method.

        state = context.getOperatorStateStore().getListState(stateDescriptor);

        boolean recoveredUserContext = false;
        if (context.isRestored()) {
            LOG.info("{} - restoring state", name());
            for (State<TXN, CONTEXT> operatorState : state.get()) {
                userContext = operatorState.getContext();
                List<TransactionHolder<TXN>> recoveredTransactions =
                        operatorState.getPendingCommitTransactions();
                List<TXN> handledTransactions = new ArrayList<>(recoveredTransactions.size() + 1);
                for (TransactionHolder<TXN> recoveredTransaction : recoveredTransactions) {
                    // If this fails to succeed eventually, there is actually data loss
                    recoverAndCommitInternal(recoveredTransaction);
                    handledTransactions.add(recoveredTransaction.handle);
                    LOG.info("{} committed recovered transaction {}", name(), recoveredTransaction);
                }

                {
                    if (operatorState.getPendingTransaction() != null) {
                        TXN transaction = operatorState.getPendingTransaction().handle;
                        recoverAndAbort(transaction);
                        handledTransactions.add(transaction);
                        LOG.info(
                                "{} aborted recovered transaction {}",
                                name(),
                                operatorState.getPendingTransaction());
                    }
                }

                if (userContext.isPresent()) {
                    finishRecoveringContext(handledTransactions);
                    recoveredUserContext = true;
                }
            }
        }

        // if in restore we didn't get any userContext or we are initializing from scratch
        if (!recoveredUserContext) {
            LOG.info("{} - no state to restore", name());

            userContext = initializeUserContext();
        }
        this.pendingCommitTransactions.clear();

        currentTransactionHolder = beginTransactionInternal();
        LOG.debug("{} - started new transaction '{}'", name(), currentTransactionHolder);
    }

    /**
     * This method must be the only place to call {@link #beginTransaction()} to ensure that the
     * {@link TransactionHolder} is created at the same time.
     */
    private TransactionHolder<TXN> beginTransactionInternal() throws Exception {
        return new TransactionHolder<>(beginTransaction(), clock.millis());
    }

    /**
     * This method must be the only place to call {@link #recoverAndCommit(Object)} to ensure that
     * the configuration parameters {@link #transactionTimeout} and {@link
     * #ignoreFailuresAfterTransactionTimeout} are respected.
     */
    private void recoverAndCommitInternal(TransactionHolder<TXN> transactionHolder) {
        try {
            logWarningIfTimeoutAlmostReached(transactionHolder);
            recoverAndCommit(transactionHolder.handle);
        } catch (final Exception e) {
            final long elapsedTime = clock.millis() - transactionHolder.transactionStartTime;
            if (ignoreFailuresAfterTransactionTimeout && elapsedTime > transactionTimeout) {
                LOG.error(
                        "Error while committing transaction {}. "
                                + "Transaction has been open for longer than the transaction timeout ({})."
                                + "Commit will not be attempted again. Data loss might have occurred.",
                        transactionHolder.handle,
                        transactionTimeout,
                        e);
            } else {
                throw e;
            }
        }
    }

    private void logWarningIfTimeoutAlmostReached(TransactionHolder<TXN> transactionHolder) {
        final long elapsedTime = transactionHolder.elapsedTime(clock);
        if (transactionTimeoutWarningRatio >= 0
                && elapsedTime > transactionTimeout * transactionTimeoutWarningRatio) {
            LOG.warn(
                    "Transaction {} has been open for {} ms. "
                            + "This is close to or even exceeding the transaction timeout of {} ms.",
                    transactionHolder.handle,
                    elapsedTime,
                    transactionTimeout);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        TXN currentTransaction = currentTransaction();
        if (currentTransaction != null) {
            abort(currentTransaction);
        }

        currentTransactionHolder = null;
    }

    /**
     * Sets the transaction timeout. Setting only the transaction timeout has no effect in itself.
     *
     * @param transactionTimeout The transaction timeout in ms.
     * @see #ignoreFailuresAfterTransactionTimeout()
     * @see #enableTransactionTimeoutWarnings(double)
     */
    protected TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> setTransactionTimeout(
            long transactionTimeout) {
        checkArgument(transactionTimeout >= 0, "transactionTimeout must not be negative");
        this.transactionTimeout = transactionTimeout;
        return this;
    }

    /**
     * If called, the sink will only log but not propagate exceptions thrown in {@link
     * #recoverAndCommit(Object)} if the transaction is older than a specified transaction timeout.
     * The start time of an transaction is determined by {@link System#currentTimeMillis()}. By
     * default, failures are propagated.
     */
    protected TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> ignoreFailuresAfterTransactionTimeout() {
        this.ignoreFailuresAfterTransactionTimeout = true;
        return this;
    }

    /**
     * Enables logging of warnings if a transaction's elapsed time reaches a specified ratio of the
     * <code>transactionTimeout</code>. If <code>warningRatio</code> is 0, a warning will be always
     * logged when committing the transaction.
     *
     * @param warningRatio A value in the range [0,1].
     * @return
     */
    protected TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> enableTransactionTimeoutWarnings(
            double warningRatio) {
        checkArgument(
                warningRatio >= 0 && warningRatio <= 1, "warningRatio must be in range [0,1]");
        this.transactionTimeoutWarningRatio = warningRatio;
        return this;
    }

    private String name() {
        return String.format(
                "%s %s/%s",
                this.getClass().getSimpleName(),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() + 1,
                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());
    }

    /** State POJO class coupling pendingTransaction, context and pendingCommitTransactions. */
    @VisibleForTesting
    @Internal
    public static final class State<TXN, CONTEXT> {
        @Nullable protected TransactionHolder<TXN> pendingTransaction;
        protected List<TransactionHolder<TXN>> pendingCommitTransactions = new ArrayList<>();
        protected Optional<CONTEXT> context;

        public State() {}

        public State(
                @Nullable TransactionHolder<TXN> pendingTransaction,
                List<TransactionHolder<TXN>> pendingCommitTransactions,
                Optional<CONTEXT> context) {
            this.pendingTransaction = pendingTransaction;
            this.pendingCommitTransactions =
                    requireNonNull(pendingCommitTransactions, "pendingCommitTransactions is null");
            this.context = requireNonNull(context, "context is null");
        }

        @Nullable
        public TransactionHolder<TXN> getPendingTransaction() {
            return pendingTransaction;
        }

        public void setPendingTransaction(@Nullable TransactionHolder<TXN> pendingTransaction) {
            this.pendingTransaction = pendingTransaction;
        }

        public List<TransactionHolder<TXN>> getPendingCommitTransactions() {
            return pendingCommitTransactions;
        }

        public void setPendingCommitTransactions(
                List<TransactionHolder<TXN>> pendingCommitTransactions) {
            this.pendingCommitTransactions = pendingCommitTransactions;
        }

        public Optional<CONTEXT> getContext() {
            return context;
        }

        public void setContext(Optional<CONTEXT> context) {
            this.context = context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            State<?, ?> state = (State<?, ?>) o;

            return Objects.equals(pendingTransaction, state.pendingTransaction)
                    && Objects.equals(pendingCommitTransactions, state.pendingCommitTransactions)
                    && Objects.equals(context, state.context);
        }

        @Override
        public int hashCode() {
            int result = pendingTransaction != null ? pendingTransaction.hashCode() : 0;
            result =
                    31 * result
                            + (pendingCommitTransactions != null
                                    ? pendingCommitTransactions.hashCode()
                                    : 0);
            result = 31 * result + (context != null ? context.hashCode() : 0);
            return result;
        }
    }

    /**
     * Adds metadata (currently only the start time of the transaction) to the transaction object.
     */
    @VisibleForTesting
    @Internal
    public static final class TransactionHolder<TXN> {

        private final TXN handle;

        /**
         * The system time when {@link #handle} was created. Used to determine if the current
         * transaction has exceeded its timeout specified by {@link #transactionTimeout}.
         */
        private final long transactionStartTime;

        @VisibleForTesting
        public TransactionHolder(TXN handle, long transactionStartTime) {
            this.handle = handle;
            this.transactionStartTime = transactionStartTime;
        }

        long elapsedTime(Clock clock) {
            return clock.millis() - transactionStartTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TransactionHolder<?> that = (TransactionHolder<?>) o;

            return transactionStartTime == that.transactionStartTime
                    && Objects.equals(handle, that.handle);
        }

        @Override
        public int hashCode() {
            int result = handle != null ? handle.hashCode() : 0;
            result = 31 * result + (int) (transactionStartTime ^ (transactionStartTime >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "TransactionHolder{"
                    + "handle="
                    + handle
                    + ", transactionStartTime="
                    + transactionStartTime
                    + '}';
        }
    }

    /** Custom {@link TypeSerializer} for the sink state. */
    @VisibleForTesting
    @Internal
    public static final class StateSerializer<TXN, CONTEXT>
            extends TypeSerializer<State<TXN, CONTEXT>> {

        private static final long serialVersionUID = 1L;

        private final TypeSerializer<TXN> transactionSerializer;
        private final TypeSerializer<CONTEXT> contextSerializer;
        private final boolean supportNullPendingTransaction;

        public StateSerializer(
                TypeSerializer<TXN> transactionSerializer,
                TypeSerializer<CONTEXT> contextSerializer) {
            this(transactionSerializer, contextSerializer, true);
        }

        public StateSerializer(
                TypeSerializer<TXN> transactionSerializer,
                TypeSerializer<CONTEXT> contextSerializer,
                boolean supportNullPendingTransaction) {
            this.transactionSerializer = checkNotNull(transactionSerializer);
            this.contextSerializer = checkNotNull(contextSerializer);
            this.supportNullPendingTransaction = supportNullPendingTransaction;
        }

        @Override
        public boolean isImmutableType() {
            return transactionSerializer.isImmutableType() && contextSerializer.isImmutableType();
        }

        @Override
        public TypeSerializer<State<TXN, CONTEXT>> duplicate() {
            return new StateSerializer<>(
                    transactionSerializer.duplicate(),
                    contextSerializer.duplicate(),
                    supportNullPendingTransaction);
        }

        @Override
        public State<TXN, CONTEXT> createInstance() {
            return null;
        }

        @Override
        public State<TXN, CONTEXT> copy(State<TXN, CONTEXT> from) {
            final TransactionHolder<TXN> pendingTransaction = from.getPendingTransaction();
            final TransactionHolder<TXN> copiedPendingTransaction =
                    pendingTransaction == null
                            ? null
                            : new TransactionHolder<>(
                                    transactionSerializer.copy(pendingTransaction.handle),
                                    pendingTransaction.transactionStartTime);

            final List<TransactionHolder<TXN>> copiedPendingCommitTransactions = new ArrayList<>();
            for (TransactionHolder<TXN> txn : from.getPendingCommitTransactions()) {
                final TXN txnHandleCopy = transactionSerializer.copy(txn.handle);
                copiedPendingCommitTransactions.add(
                        new TransactionHolder<>(txnHandleCopy, txn.transactionStartTime));
            }

            final Optional<CONTEXT> copiedContext = from.getContext().map(contextSerializer::copy);
            return new State<>(
                    copiedPendingTransaction, copiedPendingCommitTransactions, copiedContext);
        }

        @Override
        public State<TXN, CONTEXT> copy(State<TXN, CONTEXT> from, State<TXN, CONTEXT> reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(State<TXN, CONTEXT> record, DataOutputView target)
                throws IOException {
            final TransactionHolder<TXN> pendingTransaction = record.getPendingTransaction();
            serializePendingTransaction(pendingTransaction, target);

            final List<TransactionHolder<TXN>> pendingCommitTransactions =
                    record.getPendingCommitTransactions();
            target.writeInt(pendingCommitTransactions.size());
            for (TransactionHolder<TXN> pendingTxn : pendingCommitTransactions) {
                transactionSerializer.serialize(pendingTxn.handle, target);
                target.writeLong(pendingTxn.transactionStartTime);
            }

            Optional<CONTEXT> context = record.getContext();
            if (context.isPresent()) {
                target.writeBoolean(true);
                contextSerializer.serialize(context.get(), target);
            } else {
                target.writeBoolean(false);
            }
        }

        @Override
        public State<TXN, CONTEXT> deserialize(DataInputView source) throws IOException {
            final TransactionHolder<TXN> pendingTxn = deserializePendingTransaction(source);

            int numPendingCommitTxns = source.readInt();
            List<TransactionHolder<TXN>> pendingCommitTxns = new ArrayList<>(numPendingCommitTxns);
            for (int i = 0; i < numPendingCommitTxns; i++) {
                final TXN pendingCommitTxnHandle = transactionSerializer.deserialize(source);
                final long pendingCommitTxnStartTime = source.readLong();
                pendingCommitTxns.add(
                        new TransactionHolder<>(pendingCommitTxnHandle, pendingCommitTxnStartTime));
            }

            Optional<CONTEXT> context = Optional.empty();
            boolean hasContext = source.readBoolean();
            if (hasContext) {
                context = Optional.of(contextSerializer.deserialize(source));
            }

            return new State<>(pendingTxn, pendingCommitTxns, context);
        }

        private void serializePendingTransaction(
                @Nullable TransactionHolder<TXN> pendingTransaction, DataOutputView target)
                throws IOException {
            if (supportNullPendingTransaction) {
                target.writeBoolean(pendingTransaction != null);
            } else {
                checkState(
                        pendingTransaction != null,
                        "Received null pending transaction while the serializer "
                                + "does not support null pending transaction.");
            }

            if (pendingTransaction != null) {
                transactionSerializer.serialize(pendingTransaction.handle, target);
                target.writeLong(pendingTransaction.transactionStartTime);
            }
        }

        private TransactionHolder<TXN> deserializePendingTransaction(DataInputView source)
                throws IOException {
            boolean hasPendingTransaction =
                    supportNullPendingTransaction ? source.readBoolean() : true;

            if (!hasPendingTransaction) {
                return null;
            }

            TXN pendingTxnHandle = transactionSerializer.deserialize(source);
            final long pendingTxnStartTime = source.readLong();
            return new TransactionHolder<>(pendingTxnHandle, pendingTxnStartTime);
        }

        @Override
        public State<TXN, CONTEXT> deserialize(State<TXN, CONTEXT> reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            TransactionHolder<TXN> pendingTransaction = deserializePendingTransaction(source);
            serializePendingTransaction(pendingTransaction, target);

            int numPendingCommitTxns = source.readInt();
            target.writeInt(numPendingCommitTxns);
            for (int i = 0; i < numPendingCommitTxns; i++) {
                TXN pendingCommitTxnHandle = transactionSerializer.deserialize(source);
                transactionSerializer.serialize(pendingCommitTxnHandle, target);
                final long pendingCommitTxnStartTime = source.readLong();
                target.writeLong(pendingCommitTxnStartTime);
            }

            boolean hasContext = source.readBoolean();
            target.writeBoolean(hasContext);
            if (hasContext) {
                CONTEXT context = contextSerializer.deserialize(source);
                contextSerializer.serialize(context, target);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StateSerializer<?, ?> that = (StateSerializer<?, ?>) o;

            if (!transactionSerializer.equals(that.transactionSerializer)) {
                return false;
            }
            return contextSerializer.equals(that.contextSerializer);
        }

        @Override
        public int hashCode() {
            int result = transactionSerializer.hashCode();
            result = 31 * result + contextSerializer.hashCode();
            return result;
        }

        @Override
        public StateSerializerSnapshot<TXN, CONTEXT> snapshotConfiguration() {
            return new StateSerializerSnapshot<>(this);
        }
    }

    /** Snapshot for the {@link StateSerializer}. */
    @Internal
    public static final class StateSerializerSnapshot<TXN, CONTEXT>
            extends CompositeTypeSerializerSnapshot<
                    State<TXN, CONTEXT>, StateSerializer<TXN, CONTEXT>> {

        private static final int VERSION = 4;

        private static final int FIRST_VERSION_SUPPORTING_NULL_TRANSACTIONS = 3;

        private boolean supportsNullTransaction = true;

        @SuppressWarnings("WeakerAccess")
        public StateSerializerSnapshot() {}

        StateSerializerSnapshot(StateSerializer<TXN, CONTEXT> serializerInstance) {
            super(serializerInstance);
            this.supportsNullTransaction = serializerInstance.supportNullPendingTransaction;
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected void readOuterSnapshot(
                int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readOuterSnapshotVersion < FIRST_VERSION_SUPPORTING_NULL_TRANSACTIONS) {
                supportsNullTransaction = false;
            } else if (readOuterSnapshotVersion == FIRST_VERSION_SUPPORTING_NULL_TRANSACTIONS) {
                supportsNullTransaction = true;
            } else {
                supportsNullTransaction = in.readBoolean();
            }
        }

        @Override
        protected void writeOuterSnapshot(DataOutputView out) throws IOException {
            out.writeBoolean(supportsNullTransaction);
        }

        @Override
        protected OuterSchemaCompatibility resolveOuterSchemaCompatibility(
                TypeSerializerSnapshot<State<TXN, CONTEXT>> oldSerializerSnapshot) {
            if (!(oldSerializerSnapshot instanceof StateSerializerSnapshot)) {
                return OuterSchemaCompatibility.INCOMPATIBLE;
            }

            StateSerializerSnapshot<TXN, CONTEXT> oldStateSerializerSnapshot =
                    (StateSerializerSnapshot<TXN, CONTEXT>) oldSerializerSnapshot;
            if (supportsNullTransaction != oldStateSerializerSnapshot.supportsNullTransaction) {
                return OuterSchemaCompatibility.COMPATIBLE_AFTER_MIGRATION;
            }

            return OuterSchemaCompatibility.COMPATIBLE_AS_IS;
        }

        @Override
        protected StateSerializer<TXN, CONTEXT> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            final TypeSerializer<TXN> transactionSerializer =
                    (TypeSerializer<TXN>) nestedSerializers[0];

            @SuppressWarnings("unchecked")
            final TypeSerializer<CONTEXT> contextSerializer =
                    (TypeSerializer<CONTEXT>) nestedSerializers[1];

            return new StateSerializer<>(
                    transactionSerializer, contextSerializer, supportsNullTransaction);
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                StateSerializer<TXN, CONTEXT> outerSerializer) {
            return new TypeSerializer<?>[] {
                outerSerializer.transactionSerializer, outerSerializer.contextSerializer
            };
        }
    }
}
