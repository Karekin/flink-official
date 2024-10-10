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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** This handler can be used to submit jobs to a Flink cluster. */
public final class JobSubmitHandler
        extends AbstractRestHandler<
                DispatcherGateway,
                JobSubmitRequestBody,
                JobSubmitResponseBody,
                EmptyMessageParameters> {

    private static final String FILE_TYPE_JOB_GRAPH = "JobGraph";
    private static final String FILE_TYPE_JAR = "Jar";
    private static final String FILE_TYPE_ARTIFACT = "Artifact";

    private final Executor executor;
    private final Configuration configuration;

    public JobSubmitHandler(
            GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            Time timeout,
            Map<String, String> headers,
            Executor executor,
            Configuration configuration) {
        super(leaderRetriever, timeout, headers, JobSubmitHeaders.getInstance());
        this.executor = executor;
        this.configuration = configuration;
    }
    

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 接受客户端提交的任务
    */
    @Override
    protected CompletableFuture<JobSubmitResponseBody> handleRequest(
            @Nonnull HandlerRequest<JobSubmitRequestBody> request,
            @Nonnull DispatcherGateway gateway)
            throws RestHandlerException {
        /**
         * 从request对象中获取上传的文件集合，并存储在uploadedFiles变量中。
          */
        final Collection<File> uploadedFiles = request.getUploadedFiles();
        /**
         * 将每个文件的名字（通过File::getName方法获取）映射到其对应的文件路径（通过Path::fromLocalFile方法获取）
         * 组织成Map结构Map<String, Path> nameToFile
         */
        final Map<String, Path> nameToFile =
                uploadedFiles.stream()
                        .collect(Collectors.toMap(File::getName, Path::fromLocalFile));
        /**
         * 校验上传的文件数量和nameToFile映射中的条目数量是否相同。如果不同，抛出一个RestHandlerException异常，
         * 指示上传的文件数量与预期不符。
         */
        if (uploadedFiles.size() != nameToFile.size()) {
            throw new RestHandlerException(
                    String.format(
                            "The number of uploaded files was %s than the expected count. Expected: %s Actual %s",
                            uploadedFiles.size() < nameToFile.size() ? "lower" : "higher",
                            nameToFile.size(),
                            uploadedFiles.size()),
                    HttpResponseStatus.BAD_REQUEST);
        }
        /**
         * 从request对象中获取请求体，并将其存储在requestBody变量中
         * JobSubmitRequestBody应该一个自定义的类，它封装了请求体中的数据。
         */
        final JobSubmitRequestBody requestBody = request.getRequestBody();
        /**
         * requestBody中的jobGraphFileName字段是否为null。
         * 如果为null 抛出异常
         */
        if (requestBody.jobGraphFileName == null) {
            throw new RestHandlerException(
                    String.format(
                            "The %s field must not be omitted or be null.",
                            JobSubmitRequestBody.FIELD_NAME_JOB_GRAPH),
                    HttpResponseStatus.BAD_REQUEST);
        }
        /**
         * 读取文件获取JobGraph，内部异步执行
         */
        CompletableFuture<JobGraph> jobGraphFuture = loadJobGraph(requestBody, nameToFile);
        /**
         * 调用getJarFilesToUpload方法获取每个文件对应的Path路径
         */
        Collection<Path> jarFiles = getJarFilesToUpload(requestBody.jarFileNames, nameToFile);
        /**
         * 调用getArtifactFilesToUpload方法获取每个缓存文件（a_conf/a.txt）对应的Path路径
         */
        Collection<Tuple2<String, Path>> artifacts =
                getArtifactFilesToUpload(requestBody.artifactFileNames, nameToFile);
        /**
         * 上传作业图（JobGraph）的相关文件Blob 服务器。
         */
        CompletableFuture<JobGraph> finalizedJobGraphFuture =
                uploadJobGraphFiles(gateway, jobGraphFuture, jarFiles, artifacts, configuration);
        /**
         * henCompose方法将两个异步操作组合在一起。
         */
        CompletableFuture<Acknowledge> jobSubmissionFuture =
                finalizedJobGraphFuture.thenCompose(
                        /**
                         * 它负责通过gateway对象提交jobGraph，并可能等待一个指定的timeout。
                         */
                        jobGraph -> gateway.submitJob(jobGraph, timeout));
        /**
         * thenCombine方法将两个异步操作的结果合并到一起
         */
        return jobSubmissionFuture.thenCombine(
                jobGraphFuture,
                /**
                 * 将在jobSubmissionFuture和jobGraphFuture都完成时包含一个JobSubmitResponseBody对象。
                 */
                (ack, jobGraph) -> new JobSubmitResponseBody("/jobs/" + jobGraph.getJobID()));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * CompletableFuture来异步地执行任务通过读取文件，得到JobGraph
    */
    private CompletableFuture<JobGraph> loadJobGraph(
            JobSubmitRequestBody requestBody, Map<String, Path> nameToFile)
            throws MissingFileException {
        /**
         * 获取文件路径
         */
        final Path jobGraphFile =
                getPathAndAssertUpload(
                        requestBody.jobGraphFileName, FILE_TYPE_JOB_GRAPH, nameToFile);
        /**
         * 创建并启动这个异步任务。
         */
        return CompletableFuture.supplyAsync(
                () -> {
                    /** 定义一个JobGraph类型的变量jobGraph，用于存储反序列化后的对象。 */
                    JobGraph jobGraph;
                    /**
                     * ObjectInputStream从jobGraphFile指定的文件中读取对象，并将其转换为JobGraph类型。
                     */
                    try (ObjectInputStream objectIn =
                            new ObjectInputStream(
                                    jobGraphFile.getFileSystem().open(jobGraphFile))) {
                        jobGraph = (JobGraph) objectIn.readObject();
                    } catch (Exception e) {
                        /** 如果过程中出现异常，则抛出CompletionException一场 */
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Failed to deserialize JobGraph.",
                                        HttpResponseStatus.BAD_REQUEST,
                                        e));
                    }
                    /**返回反序列化得到的JobGraph对象。 */
                    return jobGraph;
                },
                executor);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private static Collection<Path> getJarFilesToUpload(
            Collection<String> jarFileNames, Map<String, Path> nameToFileMap)
            throws MissingFileException {
        /**
         * 创建一个新的 ArrayList，其初始容量与 jarFileNames 集合的大小相同，用于存储找到的JAR文件路径。
         */
        Collection<Path> jarFiles = new ArrayList<>(jarFileNames.size());
        /** 遍历 jarFileNames 集合中的每个JAR文件名。 */
        for (String jarFileName : jarFileNames) {
            /**
             * 通过getPathAndAssertUpload方法获取jarFileName对应JAR文件的路径
             */
            Path jarFile = getPathAndAssertUpload(jarFileName, FILE_TYPE_JAR, nameToFileMap);
            /**
             * 添加文件路径到结果集合
             */
            jarFiles.add(new Path(jarFile.toString()));
        }
        /** 返回包含所有找到的JAR文件路径的 jarFiles 集合。 */
        return jarFiles;
    }
   /**
    * @授课老师(微信): yi_locus
    * email: 156184212@qq.com
    * 获取缓存文件对应的路径
   */
    private static Collection<Tuple2<String, Path>> getArtifactFilesToUpload(
            Collection<JobSubmitRequestBody.DistributedCacheFile> artifactEntries,
            Map<String, Path> nameToFileMap)
            throws MissingFileException {
        /**
         * 新的 ArrayList，其初始容量与 artifactEntries 集合的大小相同，用于存储找到的工件文件路径及其条目名称。
         */
        Collection<Tuple2<String, Path>> artifacts = new ArrayList<>(artifactEntries.size());
        /**
         * 遍历 artifactEntries 集合中的每个 JobSubmitRequestBody.DistributedCacheFile 对象。
         */
        for (JobSubmitRequestBody.DistributedCacheFile artifactFileName : artifactEntries) {
            /**
             * 调用 getPathAndAssertUpload 方法得到文件对应的Path
             */
            Path artifactFile =
                    getPathAndAssertUpload(
                            artifactFileName.fileName, FILE_TYPE_ARTIFACT, nameToFileMap);
            artifacts.add(Tuple2.of(artifactFileName.entryName, new Path(artifactFile.toString())));
        }
        /** 返回结果集合 */
        return artifacts;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * uploadJobGraphFiles 方法
     * 上传作业图（JobGraph）的相关文件Blob 服务器。
     * Blob 服务器是 Flink 集群中用于存储作业依赖文件（如 JAR 包、配置文件等）的组件。
    */
    private CompletableFuture<JobGraph> uploadJobGraphFiles(
            DispatcherGateway gateway,
            CompletableFuture<JobGraph> jobGraphFuture,
            Collection<Path> jarFiles,
            Collection<Tuple2<String, Path>> artifacts,
            Configuration configuration) {
        CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);
        /**
         * thenCombine 方法将 jobGraphFuture 和 blobServerPortFuture 组合在一起。
         * 当这两个 Future 都完成时，会执行提供的 lambda 表达式。
         */
        return jobGraphFuture.thenCombine(
                blobServerPortFuture,
                (JobGraph jobGraph, Integer blobServerPort) -> {
                    /** 从 gateway 获取的主机名和 Blob 服务器的端口号，创建一个 InetSocketAddress 对象。 */
                    final InetSocketAddress address =
                            new InetSocketAddress(gateway.getHostname(), blobServerPort);
                    try {
                        /**
                         * 调用 ClientUtils 的 uploadJobGraphFiles 方法，传入 jobGraph、jarFiles、artifacts
                         *和一个 lambda 表达式（该表达式用于创建新的 BlobClient 实例）。
                         * BlobClient 用于与 Blob 服务器通信以上传文件。
                         */
                        ClientUtils.uploadJobGraphFiles(
                                jobGraph,
                                jarFiles,
                                artifacts,
                                () -> new BlobClient(address, configuration));
                    } catch (FlinkException e) {
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Could not upload job files.",
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        e));
                    }
                    /** 返回 JobGraph */
                    return jobGraph;
                });
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从给定的 uploadedFiles 映射中获取指定文件名的文件路径，并验证该文件是否存在。
     * 如果不存在，则抛出一个 MissingFileException 异常。
    */
    private static Path getPathAndAssertUpload(
            String fileName, String type, Map<String, Path> uploadedFiles)
            throws MissingFileException {
        /**
         * 使用 uploadedFiles 映射的 get 方法，通过文件名 fileName 获取对应的文件路径，并将其存储在 file 变量中
         */
        final Path file = uploadedFiles.get(fileName);
        /**
         * 如果 file 是 null，这意味着映射中不存在与 fileName 对应的文件路径。则抛出一场
         *
         */
        if (file == null) {
            throw new MissingFileException(type, fileName);
        }
        /** 如果文件路径存在（即 file 不是 null），方法将返回这个路径。 */
        return file;
    }

    private static final class MissingFileException extends RestHandlerException {

        private static final long serialVersionUID = -7954810495610194965L;

        MissingFileException(String type, String fileName) {
            super(
                    type + " file " + fileName + " could not be found on the server.",
                    HttpResponseStatus.BAD_REQUEST);
        }
    }
}
