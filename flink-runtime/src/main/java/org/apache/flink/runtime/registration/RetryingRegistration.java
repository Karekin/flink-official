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

package org.apache.flink.runtime.registration;

import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This utility class implements the basis of registering one component at another component, for
 * example registering the TaskExecutor at the ResourceManager. This {@code RetryingRegistration}
 * implements both the initial address resolution and the retries-with-backoff strategy.
 *
 * <p>The registration gives access to a future that is completed upon successful registration. The
 * registration can be canceled, for example when the target where it tries to register at loses
 * leader status.
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 * @param <R> The type of the registration rejection responses.
 */
public abstract class RetryingRegistration<
        F extends Serializable,
        G extends RpcGateway,
        S extends RegistrationResponse.Success,
        R extends RegistrationResponse.Rejection> {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final Logger log;

    private final RpcService rpcService;

    private final String targetName;

    private final Class<G> targetType;

    private final String targetAddress;

    private final F fencingToken;

    private final CompletableFuture<RetryingRegistrationResult<G, S, R>> completionFuture;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private volatile boolean canceled;

    // ------------------------------------------------------------------------

    public RetryingRegistration(
            Logger log,
            RpcService rpcService,
            String targetName,
            Class<G> targetType,
            String targetAddress,
            F fencingToken,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

        this.log = checkNotNull(log);
        this.rpcService = checkNotNull(rpcService);
        this.targetName = checkNotNull(targetName);
        this.targetType = checkNotNull(targetType);
        this.targetAddress = checkNotNull(targetAddress);
        this.fencingToken = checkNotNull(fencingToken);
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

        this.completionFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  completion and cancellation
    // ------------------------------------------------------------------------

    public CompletableFuture<RetryingRegistrationResult<G, S, R>> getFuture() {
        return completionFuture;
    }

    /** Cancels the registration procedure. */
    public void cancel() {
        canceled = true;
        completionFuture.cancel(false);
    }

    /**
     * Checks if the registration was canceled.
     *
     * @return True if the registration was canceled, false otherwise.
     */
    public boolean isCanceled() {
        return canceled;
    }

    // ------------------------------------------------------------------------
    //  registration
    // ------------------------------------------------------------------------

    protected abstract CompletableFuture<RegistrationResponse> invokeRegistration(
            G gateway, F fencingToken, long timeoutMillis) throws Exception;

    /**
     * This method resolves the target address to a callable gateway and starts the registration
     * after that.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 开始注册
     * 1.rpcService.connect获取网关代理对象 rpcGateway
     * 2.thenAcceptAsync异步调用 register方法会接收网关代理对象 rpcGateway
    */
    @SuppressWarnings("unchecked")
    public void startRegistration() {
        if (canceled) {
            // we already got canceled
            return;
        }

        try {
            // trigger resolution of the target address to a callable gateway
            /**
             * 这是一个 CompletableFuture 类型的变量，它代表了连接远程RPC网关的异步结果。
             */
            final CompletableFuture<G> rpcGatewayFuture;
            /**
             * 检查 targetType 是否是 FencedRpcGateway 类型或其子类。
             */
            if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
                /** 调用内部方法获取到网关代理对象 */
                rpcGatewayFuture =
                        (CompletableFuture<G>)
                                rpcService.connect(
                                        targetAddress,
                                        fencingToken,
                                        targetType.asSubclass(FencedRpcGateway.class));
            } else {
                /** 调用内部方法获取到网关代理对象 */
                rpcGatewayFuture = rpcService.connect(targetAddress, targetType);
            }

            // 异步调用register方法进行注册
            CompletableFuture<Void> rpcGatewayAcceptFuture =
                    rpcGatewayFuture.thenAcceptAsync(
                            (G rpcGateway) -> {
                                log.info("Resolved {} address, beginning registration", targetName);
                                /**
                                 * 调用 register 方法来执行实际的注册逻辑，传递网关代理对象、初始重试次数和初始注册超时时间作为参数。
                                 */
                                register(
                                        rpcGateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis());
                            },
                            rpcService.getScheduledExecutor());

            // upon failure, retry, unless this is cancelled
            /**
             * whenCompleteAsync 方法异步处理 rpcGatewayAcceptFuture 的完成状态。
             * 该方法接受两个参数：一个 BiConsumer（表示当 Future 完成时执行的函数）和一个执行器（用于执行该函数）。
             */
            rpcGatewayAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        /**
                         * 如果 failure 不为 null（即发生了异常），并且 canceled 为 false（即注册没有被取消），则进入异常处理逻辑。
                         */
                        if (failure != null && !canceled) {
                            final Throwable strippedFailure =
                                    ExceptionUtils.stripCompletionException(failure);
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Could not resolve {} address {}, retrying in {} ms.",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure);
                            } else {
                                log.info(
                                        "Could not resolve {} address {}, retrying in {} ms: {}",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure.getMessage());
                            }
                            /**
                             * 再次进行注册
                              */
                            startRegistrationLater(
                                    retryingRegistrationConfiguration.getErrorDelayMillis());
                        }
                    },
                    rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    /**
     * This method performs a registration attempt and triggers either a success notification or a
     * retry, depending on the result.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 执行注册尝试，并根据结果触发成功通知或重试。
    */
    @SuppressWarnings("unchecked")
    private void register(final G gateway, final int attempt, final long timeoutMillis) {
        // eager check for canceling to avoid some unnecessary work
        if (canceled) {
            return;
        }

        try {
            log.debug(
                    "Registration at {} attempt {} (timeout={}ms)",
                    targetName,
                    attempt,
                    timeoutMillis);
            /** 真正通过代理对象向ResourceManager进行注册 */
            CompletableFuture<RegistrationResponse> registrationFuture =
                    invokeRegistration(gateway, fencingToken, timeoutMillis);

            // if the registration was successful, let the TaskExecutor know
            /** 如果注册成功，告知TaskExecutor */
            CompletableFuture<Void> registrationAcceptFuture =
                    registrationFuture.thenAcceptAsync(
                            (RegistrationResponse result) -> {
                                /** 如果没有取消 */
                                if (!isCanceled()) {
                                    /** 如果注册成功 */
                                    if (result instanceof RegistrationResponse.Success) {
                                        log.debug(
                                                "Registration with {} at {} was successful.",
                                                targetName,
                                                targetAddress);
                                        S success = (S) result;
                                        /** 异步编程设置为结束，返回值为 RetryingRegistrationResult.succes*/
                                        completionFuture.complete(
                                                RetryingRegistrationResult.success(
                                                        gateway, success));
                                        /** 如果注册被拒绝*/
                                    } else if (result instanceof RegistrationResponse.Rejection) {
                                        /** 打印日志 */
                                        log.debug(
                                                "Registration with {} at {} was rejected.",
                                                targetName,
                                                targetAddress);
                                        /** 异步编程设置为结束，返回值为 RetryingRegistrationResult.rejection*/
                                        R rejection = (R) result;
                                        completionFuture.complete(
                                                RetryingRegistrationResult.rejection(rejection));
                                    } else {
                                        // registration failure
                                        /** 如果注册失败*/
                                        if (result instanceof RegistrationResponse.Failure) {
                                            /** 异步编程设置为结束，返回值为 RetryingRegistrationResult.rejection*/
                                            RegistrationResponse.Failure failure =
                                                    (RegistrationResponse.Failure) result;
                                            log.info(
                                                    "Registration failure at {} occurred.",
                                                    targetName,
                                                    failure.getReason());
                                        } else {
                                            /** 打印日志*/
                                            log.error(
                                                    "Received unknown response to registration attempt: {}",
                                                    result);
                                        }
                                        /** 打印日志*/
                                        log.info(
                                                "Pausing and re-attempting registration in {} ms",
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());
                                        /** 失败状态重试*/
                                        registerLater(
                                                gateway,
                                                1,
                                                retryingRegistrationConfiguration
                                                        .getInitialRegistrationTimeoutMillis(),
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());
                                    }
                                }
                            },
                            rpcService.getScheduledExecutor());

            // upon failure, retry
            registrationAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        if (failure != null && !isCanceled()) {
                            /** 如果是注册超时时间 */
                            if (ExceptionUtils.stripCompletionException(failure)
                                    instanceof TimeoutException) {
                                // we simply have not received a response in time. maybe the timeout
                                // was
                                // very low (initial fast registration attempts), maybe the target
                                // endpoint is
                                // currently down.
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "Registration at {} ({}) attempt {} timed out after {} ms",
                                            targetName,
                                            targetAddress,
                                            attempt,
                                            timeoutMillis);
                                }
                                /**
                                 * 原始的超时时间（timeoutMillis）的两倍
                                 * 重试注册配置（retryingRegistrationConfiguration）中的最大注册超时时间
                                 * 获取小的那个时间
                                 */
                                long newTimeoutMillis =
                                        Math.min(
                                                2 * timeoutMillis,
                                                retryingRegistrationConfiguration
                                                        .getMaxRegistrationTimeoutMillis());
                                /** 重新注册*/
                                register(gateway, attempt + 1, newTimeoutMillis);
                            } else {

                                // a serious failure occurred. we still should not give up, but keep
                                // trying
                                /** 打印日志 */
                                log.error(
                                        "Registration at {} failed due to an error",
                                        targetName,
                                        failure);
                                log.info(
                                        "Pausing and re-attempting registration in {} ms",
                                        retryingRegistrationConfiguration.getErrorDelayMillis());
                                /** 重新注册 */
                                registerLater(
                                        gateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis(),
                                        retryingRegistrationConfiguration.getErrorDelayMillis());
                            }
                        }
                    },
                    rpcService.getScheduledExecutor());
        } catch (Throwable t) {
            /** 异常情况 设置异步变成完成，完成状态为异常 设置值为Throwable */
            completionFuture.completeExceptionally(t);
            /** 取消注册 CompletableFuture*/
            cancel();
        }
    }

    private void registerLater(
            final G gateway, final int attempt, final long timeoutMillis, long delay) {
        rpcService
                .getScheduledExecutor()
                .schedule(
                        () -> register(gateway, attempt, timeoutMillis),
                        delay,
                        TimeUnit.MILLISECONDS);
    }

    private void startRegistrationLater(final long delay) {
        rpcService
                .getScheduledExecutor()
                .schedule(this::startRegistration, delay, TimeUnit.MILLISECONDS);
    }

    static final class RetryingRegistrationResult<G, S, R> {
        @Nullable private final G gateway;

        @Nullable private final S success;

        @Nullable private final R rejection;

        private RetryingRegistrationResult(
                @Nullable G gateway, @Nullable S success, @Nullable R rejection) {
            this.gateway = gateway;
            this.success = success;
            this.rejection = rejection;
        }

        boolean isSuccess() {
            return success != null && gateway != null;
        }

        boolean isRejection() {
            return rejection != null;
        }

        public G getGateway() {
            Preconditions.checkState(isSuccess());
            return gateway;
        }

        public R getRejection() {
            Preconditions.checkState(isRejection());
            return rejection;
        }

        public S getSuccess() {
            Preconditions.checkState(isSuccess());
            return success;
        }

        static <
                        G extends RpcGateway,
                        S extends RegistrationResponse.Success,
                        R extends RegistrationResponse.Rejection>
                RetryingRegistrationResult<G, S, R> success(G gateway, S success) {
            return new RetryingRegistrationResult<>(gateway, success, null);
        }

        static <
                        G extends RpcGateway,
                        S extends RegistrationResponse.Success,
                        R extends RegistrationResponse.Rejection>
                RetryingRegistrationResult<G, S, R> rejection(R rejection) {
            return new RetryingRegistrationResult<>(null, null, rejection);
        }
    }
}
