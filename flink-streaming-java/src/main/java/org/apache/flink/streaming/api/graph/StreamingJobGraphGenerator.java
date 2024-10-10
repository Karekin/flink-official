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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphUtils;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}. */
/**
  * @授课老师(V): yi_locus
  * email: 156184212@qq.com
  * StreamingJobGraphGenerator StreamGraph转换 JobGraph。
  * StreamingJobGraphGenerator是JobGraph的生成器
  */

@Internal
public class StreamingJobGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

    // ------------------------------------------------------------------------


    @VisibleForTesting
    public static JobGraph createJobGraph(StreamGraph streamGraph) {
        return new StreamingJobGraphGenerator(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        null,
                        Runnable::run)
                .createJobGraph();
    }
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * createJobGraph方法用于从StreamGraph对象创建一个JobGraph对象。
      */
    public static JobGraph createJobGraph(
            ClassLoader userClassLoader, StreamGraph streamGraph, @Nullable JobID jobID) {
        // TODO Currently, we construct a new thread pool for the compilation of each job. In the
        // future, we may refactor the job submission framework and make it reusable across jobs.
        /**
         * TODO当前，我们为每个作业的编译构建一个新的线程池。未来，我们可能会重构作业提交框架，并使其在多个作业中可重复使用。
         * 创建了一个固定大小的线程池serializationExecutor，用于编译作业时的序列化操作。线程池的大小是基于CPU核心数
         * （通过Hardware.getNumberCPUCores()获取）和作业配置的并行度（通过streamGraph.getExecutionConfig().getParallelism()获取）来确定的。
         * 这样做是为了提高序列化操作的并行度，从而提高性能。
         * 总结：ExecutorService serializationExecutor CompletableFuture 异步对数据序列化的的时候传入当前线程池
         */
        final ExecutorService serializationExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        Hardware.getNumberCPUCores(),
                                        streamGraph.getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io"));
        try {
            /**
             * 创建了一个StreamingJobGraphGenerator对象，并调用其createJobGraph方法来生成JobGraph
             * StreamingJobGraphGenerator会做什么初始化JobGraph中要用到的结构
             */
            return new StreamingJobGraphGenerator(
                            userClassLoader, streamGraph, jobID, serializationExecutor)
                    .createJobGraph();
        } finally {
            /**
             * 无论前面的代码块是否抛出异常，finally块中的代码都会执行
             */
            serializationExecutor.shutdown();
        }
    }

    // ------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 下面的很多字段都与链接有关，从StreamGraph到JobGraph转换过程中，一个很重要的步骤就是将算子链接到一起。
     * 作为同一个任务实现串行执行，从而节省资源，提高效率的目的、
    */
    private final ClassLoader userClassloader;
    /** 传入进来的StreamGraph */
    private final StreamGraph streamGraph;
    /** 用来存放JobGraph中的节点 */
    private final Map<Integer, JobVertex> jobVertices;
    /** 声明一个JogGraph类型的变量 */
    private final JobGraph jobGraph;
    /** 用来维护已经构建的StreamNode的id */
    private final Collection<Integer> builtVertices;
    /** 物理便的集合 */
    private final List<StreamEdge> physicalEdgesInOrder;
    /** 保存链接信息 */
    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;
    /** 保存节点信息 */
    private final Map<Integer, StreamConfig> vertexConfigs;
    /** 保存链的名称 */
    private final Map<Integer, String> chainedNames;

    private final Map<Integer, ResourceSpec> chainedMinResources;
    private final Map<Integer, ResourceSpec> chainedPreferredResources;

    private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

    private final StreamGraphHasher defaultStreamGraphHasher;
    private final List<StreamGraphHasher> legacyStreamGraphHashers;

    private boolean hasHybridResultPartition = false;

    private final Executor serializationExecutor;

    // Futures for the serialization of operator coordinators
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex = new HashMap<>();

    /** The {@link OperatorChainInfo}s, key is the start node id of the chain. */
    private final Map<Integer, OperatorChainInfo> chainInfos;

    /**
     * This is used to cache the non-chainable outputs, to set the non-chainable outputs config
     * after all job vertices are created.
     */
    private final Map<Integer, List<StreamEdge>> opNonChainableOutputsCache;

    private StreamingJobGraphGenerator(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            @Nullable JobID jobID,
            Executor serializationExecutor) {
        this.userClassloader = userClassloader;
        this.streamGraph = streamGraph;
        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

        this.jobVertices = new LinkedHashMap<>();
        this.builtVertices = new HashSet<>();
        this.chainedConfigs = new HashMap<>();
        this.vertexConfigs = new HashMap<>();
        this.chainedNames = new HashMap<>();
        this.chainedMinResources = new HashMap<>();
        this.chainedPreferredResources = new HashMap<>();
        this.chainedInputOutputFormats = new HashMap<>();
        this.physicalEdgesInOrder = new ArrayList<>();
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        this.chainInfos = new HashMap<>();
        this.opNonChainableOutputsCache = new LinkedHashMap<>();

        jobGraph = new JobGraph(jobID, streamGraph.getJobName());
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 构建JobGraph
      */
    private JobGraph createJobGraph() {
        /**
         * 在创建 JobGraph 之前，首先调用 preValidate() 方法进行预验证。这通常是为了确保所有必要的条件都已满足，以便可以安全地创建 JobGraph
         */
        preValidate();
        /**
         * 设置 Job的类型，该类型通常与 StreamGraph 的类型相同。
         */
        jobGraph.setJobType(streamGraph.getJobType());
        /**
         * setDynamic，该类型通常与 StreamGraph 的类型相同。
         * 当使用AdaptiveBatchScheduler时，这将导致许多作业顶点的平行度不是基于数据量计算的，
         * 批处理调度器,自适应批调度器，即自动推导并设置作业的并行度，无需用户手动设置并行度。
         * Flink根据用户设置的期望及作业执行情况，自动设置并行度。
         */
        jobGraph.setDynamic(streamGraph.isDynamic());
        /**
         * 根据 StreamGraph 的检查点配置来决定是否启用近似本地恢复功能。
         * 是否启用近似本地恢复。此标志将与传统调度策略一起删除。
          */
        jobGraph.enableApproximateLocalRecovery(
                streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        // Generate deterministic hashes for the nodes in order to identify them across
        // submission iff they didn't change.
        /**
         * 使用 defaultStreamGraphHasher 遍历 StreamGraph 并为其中的节点生成确定性哈希。这些哈希值用于在提交时识别节点，前提是这些节点没有发生变化。
         */
        Map<Integer, byte[]> hashes =
                defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

        // Generate legacy version hashes for backwards compatibility
        /**
         * 创建一个 legacyHashes 列表，用于存储使用不同哈希器生成的旧版本哈希。这主要是为了向后兼容，确保旧的系统或组件可以正确识别节点。
         */
        List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
        for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
            legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
        }
        /**
         * 调用 setChaining 方法来设置节点之间的链接（Chaining）。Chaining 是 Flink 中的一个优化技术，用于减少数据传输的开销。
         */
        setChaining(hashes, legacyHashes);

        /**
         * 如果 JobGraph 是动态的，则调用 setVertexParallelismsForDynamicGraphIfNecessary 方法来设置顶点的并行度。这通常是为了优化计算任务的性能。
         */
        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary();
        }

        // Note that we set all the non-chainable outputs configuration here because the
        // "setVertexParallelismsForDynamicGraphIfNecessary" may affect the parallelism of job
        // vertices and partition-reuse
        /**
         * 在此处设置了所有不可链接的输出配置，因为
         * “setVertexParallelismsForDynamicGraphIfNenecessary”可能会影响作业顶点的并行性和分区重用
         */
        /** 创建一个空Map对象 opIntermediateOutputs */
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
                new HashMap<>();
        /**
         * 设置所有Operator 任务不能链的输出的配置，
         * 为了方便后面序列化，
         * 1.内部就是将 NUMBER_OF_OUTPUTS放入config
         * opNonChainedOutputs,deduplicatedOutputs 放入序列化对象的
         * Map<String, Object> toBeSerializedConfigObjects = new HashMap<>();
         * 后面统一进行序列化
         */
        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs);
        /**
         * 设置所有Vertext 任务不能链的输出的配置，
         * 也就是设置IntermediateDataSet、JobEdge
         */
        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs);
        /**
         * 设置物理边（Physical Edges）。
         * Map<String, Object> toBeSerializedConfigObjects = new HashMap<>();将StreamEdge设置给待序列化对象集合
         */
        setPhysicalEdges();
        /**
         * 标记哪些任务支持并发执行尝试。在某些情况下，Flink 允许任务尝试并发执行，以提高容错性和性能。
         */
        markSupportingConcurrentExecutionAttempts();
        /**
         * 验证shuffle是否在批处理模式下执行。
         */
        validateHybridShuffleExecuteInBatchMode();
        /**
         * 设置槽（Slot）共享和协同定位（Co-location）
         */
        setSlotSharingAndCoLocation();
        /**
         * 设置管理的内存比例。这是为了分配和管理 Flink 任务的内存资源
         */
        setManagedMemoryFraction(
                Collections.unmodifiableMap(jobVertices),
                Collections.unmodifiableMap(vertexConfigs),
                Collections.unmodifiableMap(chainedConfigs),
                id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
                id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases());
        /**
         * 配置检查点（Checkpointing）。检查点是 Flink 的容错机制，用于在任务失败时恢复状态。
         */
        configureCheckpointing();
        /**
         * 设置 JobGraph 的保存点（Savepoint）恢复设置
         */
        jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());
        /**
         * 准备用户定义的资源（如文件或对象）
         */
        final Map<String, DistributedCache.DistributedCacheEntry> distributedCacheEntries =
                JobGraphUtils.prepareUserArtifactEntries(
                        streamGraph.getUserArtifacts().stream()
                                .collect(Collectors.toMap(e -> e.f0, e -> e.f1)),
                        jobGraph.getJobID());
        /**
         * 将用户定义的资源添加到 JobGraph 中，比如cache
         */
        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                distributedCacheEntries.entrySet()) {
            jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
        }

        // set the ExecutionConfig last when it has been finalized
        try {
            /**
             * 设置 JobGraph 的执行配置（ExecutionConfig）。这个配置包含了任务执行时的各种参数和设置。
             */
            jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Could not serialize the ExecutionConfig."
                            + "This indicates that non-serializable types (like custom serializers) were registered");
        }
        /**
         * 设置 JobGraph 的作业配置（JobConfiguration）。这通常包含了作业的元数据和其他设置。
         */
        jobGraph.setJobConfiguration(streamGraph.getJobConfiguration());
        /**
         * 在顶点的名称中添加顶点索引的前缀。这可能是为了更清晰地标识图中的每个顶点。
         */
        addVertexIndexPrefixInVertexName();
        /**
         * 设置顶点的描述。这通常用于记录或显示顶点的信息，帮助用户或开发者更好地理解图中的每个顶点。
         */
        setVertexDescription();

        // Wait for the serialization of operator coordinators and stream config.
        /**
         * 触发对象配置序列化并返回可完成的future
         */
        try {
            FutureUtils.combineAll(
                            vertexConfigs.values().stream()
                                    .map(
                                            config ->
                                                    config.triggerSerializationAndReturnFuture(
                                                            serializationExecutor))
                                    .collect(Collectors.toList()))
                    .get();
            /**
             * 等待序列化完成并更新作业顶点.
             * 内部是添加协调器，OperatorCoordinator
             * 运行时Operator的协调器。OperatorCoordinator在与Operator作业顶点相关联的主机上运行。它通过发送Operator事件与Operator进行通信。
             * OperatorCoordinator代表的是runtime operators，其运行在JobMaster中，
             * 一个OperatorCoordinator对应的是一个operator的Job vertex,其和operators的交互是通过operator event。
             * 主要负责subTask的重启、失败等，以及operator的checkpoint行为。
             */
            waitForSerializationFuturesAndUpdateJobVertices();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error in serialization.", e);
        }
        /**
         * 检查 streamGraph 是否有作业状态钩子（JobStatusHooks）。
         * 作业状态钩子通常用于在作业生命周期的不同阶段执行自定义逻辑，如作业提交、恢复等。
         * 如果有，将 streamGraph 中的作业状态钩子设置到 jobGraph 中，以确保这些钩子在 jobGraph 执行时也会被触发。
         */
        if (!streamGraph.getJobStatusHooks().isEmpty()) {
            jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
        }

        return jobGraph;
    }

    private void waitForSerializationFuturesAndUpdateJobVertices()
            throws ExecutionException, InterruptedException {
        for (Map.Entry<
                        JobVertexID,
                        List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
                futuresPerJobVertex : coordinatorSerializationFuturesPerJobVertex.entrySet()) {
            final JobVertexID jobVertexId = futuresPerJobVertex.getKey();
            final JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);

            Preconditions.checkState(
                    jobVertex != null,
                    "OperatorCoordinator providers were registered for JobVertexID '%s' but no corresponding JobVertex can be found.",
                    jobVertexId);
            FutureUtils.combineAll(futuresPerJobVertex.getValue())
                    .get()
                    .forEach(jobVertex::addOperatorCoordinator);
        }
    }

    private void addVertexIndexPrefixInVertexName() {
        if (!streamGraph.isVertexNameIncludeIndexPrefix()) {
            return;
        }
        final AtomicInteger vertexIndexId = new AtomicInteger(0);
        jobGraph.getVerticesSortedTopologicallyFromSources()
                .forEach(
                        vertex ->
                                vertex.setName(
                                        String.format(
                                                "[vertex-%d]%s",
                                                vertexIndexId.getAndIncrement(),
                                                vertex.getName())));
    }

    private void setVertexDescription() {
        for (Map.Entry<Integer, JobVertex> headOpAndJobVertex : jobVertices.entrySet()) {
            Integer headOpId = headOpAndJobVertex.getKey();
            JobVertex vertex = headOpAndJobVertex.getValue();
            StringBuilder builder = new StringBuilder();
            switch (streamGraph.getVertexDescriptionMode()) {
                case CASCADING:
                    buildCascadingDescription(builder, headOpId, headOpId);
                    break;
                case TREE:
                    buildTreeDescription(builder, headOpId, headOpId, "", true);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Description mode %s not supported",
                                    streamGraph.getVertexDescriptionMode()));
            }
            vertex.setOperatorPrettyName(builder.toString());
        }
    }

    private void buildCascadingDescription(StringBuilder builder, int headOpId, int currentOpId) {
        StreamNode node = streamGraph.getStreamNode(currentOpId);
        builder.append(getDescriptionWithChainedSourcesInfo(node));

        LinkedList<Integer> chainedOutput = getChainedOutputNodes(headOpId, node);
        if (chainedOutput.isEmpty()) {
            return;
        }
        builder.append(" -> ");

        boolean multiOutput = chainedOutput.size() > 1;
        if (multiOutput) {
            builder.append("(");
        }
        while (true) {
            Integer outputId = chainedOutput.pollFirst();
            buildCascadingDescription(builder, headOpId, outputId);
            if (chainedOutput.isEmpty()) {
                break;
            }
            builder.append(" , ");
        }
        if (multiOutput) {
            builder.append(")");
        }
    }

    private LinkedList<Integer> getChainedOutputNodes(int headOpId, StreamNode node) {
        LinkedList<Integer> chainedOutput = new LinkedList<>();
        if (chainedConfigs.containsKey(headOpId)) {
            for (StreamEdge edge : node.getOutEdges()) {
                int targetId = edge.getTargetId();
                if (chainedConfigs.get(headOpId).containsKey(targetId)) {
                    chainedOutput.add(targetId);
                }
            }
        }
        return chainedOutput;
    }

    private void buildTreeDescription(
            StringBuilder builder, int headOpId, int currentOpId, String prefix, boolean isLast) {
        // Replace the '-' in prefix of current node with ' ', keep ':'
        // HeadNode
        // :- Node1
        // :  :- Child1
        // :  +- Child2
        // +- Node2
        //    :- Child3
        //    +- Child4
        String currentNodePrefix = "";
        String childPrefix = "";
        if (currentOpId != headOpId) {
            if (isLast) {
                currentNodePrefix = prefix + "+- ";
                childPrefix = prefix + "   ";
            } else {
                currentNodePrefix = prefix + ":- ";
                childPrefix = prefix + ":  ";
            }
        }

        StreamNode node = streamGraph.getStreamNode(currentOpId);
        builder.append(currentNodePrefix);
        builder.append(getDescriptionWithChainedSourcesInfo(node));
        builder.append("\n");

        LinkedList<Integer> chainedOutput = getChainedOutputNodes(headOpId, node);
        while (!chainedOutput.isEmpty()) {
            Integer outputId = chainedOutput.pollFirst();
            buildTreeDescription(builder, headOpId, outputId, childPrefix, chainedOutput.isEmpty());
        }
    }

    private String getDescriptionWithChainedSourcesInfo(StreamNode node) {

        List<StreamNode> chainedSources;
        if (!chainedConfigs.containsKey(node.getId())) {
            // node is not head operator of a vertex
            chainedSources = Collections.emptyList();
        } else {
            chainedSources =
                    node.getInEdges().stream()
                            .map(StreamEdge::getSourceId)
                            .filter(
                                    id ->
                                            streamGraph.getSourceIDs().contains(id)
                                                    && chainedConfigs
                                                            .get(node.getId())
                                                            .containsKey(id))
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toList());
        }
        return chainedSources.isEmpty()
                ? node.getOperatorDescription()
                : String.format(
                        "%s [%s]",
                        node.getOperatorDescription(),
                        chainedSources.stream()
                                .map(StreamNode::getOperatorDescription)
                                .collect(Collectors.joining(", ")));
    }

    @SuppressWarnings("deprecation")
    private void preValidate() {
        CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

        if (checkpointConfig.isCheckpointingEnabled()) {
            // temporarily forbid checkpointing for iterative jobs
            if (streamGraph.isIterative() && !checkpointConfig.isForceCheckpointing()) {
                throw new UnsupportedOperationException(
                        "Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
                                + "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
                                + "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
            }
            if (streamGraph.isIterative()
                    && checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()) {
                throw new UnsupportedOperationException(
                        "Unaligned Checkpoints are currently not supported for iterative jobs, "
                                + "as rescaling would require alignment (in addition to the reduced checkpointing guarantees)."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }
            if (checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()
                    && streamGraph.getStreamNodes().stream().anyMatch(this::hasCustomPartitioner)) {
                throw new UnsupportedOperationException(
                        "Unaligned checkpoints are currently not supported for custom partitioners, "
                                + "as rescaling is not guaranteed to work correctly."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }

            for (StreamNode node : streamGraph.getStreamNodes()) {
                StreamOperatorFactory operatorFactory = node.getOperatorFactory();
                if (operatorFactory != null) {
                    Class<?> operatorClass =
                            operatorFactory.getStreamOperatorClass(userClassloader);
                    if (InputSelectable.class.isAssignableFrom(operatorClass)) {

                        throw new UnsupportedOperationException(
                                "Checkpointing is currently not supported for operators that implement InputSelectable:"
                                        + operatorClass.getName());
                    }
                }
            }
        }

        if (checkpointConfig.isUnalignedCheckpointsEnabled()
                && getCheckpointingMode(checkpointConfig) != CheckpointingMode.EXACTLY_ONCE) {
            LOG.warn("Unaligned checkpoints can only be used with checkpointing mode EXACTLY_ONCE");
            checkpointConfig.enableUnalignedCheckpoints(false);
        }
    }

    private boolean hasCustomPartitioner(StreamNode node) {
        return node.getOutEdges().stream()
                .anyMatch(edge -> edge.getPartitioner() instanceof CustomPartitionerWrapper);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 主要目的是将物理边（StreamEdge对象）按目标节点ID分组，并设置每个节点的输入物理边列表
    */
    private void setPhysicalEdges() {
        /**
         * 使用HashMap创建一个映射physicalInEdgesInOrder，其键是目标节点ID（Integer），
         * 值是一个StreamEdge对象的列表。这个映射用来存储每个目标节点ID对应的所有输入边。
         */
        Map<Integer, List<StreamEdge>> physicalInEdgesInOrder =
                new HashMap<Integer, List<StreamEdge>>();
        /** 遍历physicalEdgesInOrder列表中的每一条边。对于每一条边，获取其目标节点ID（target）。 */
        for (StreamEdge edge : physicalEdgesInOrder) {
            /** 获取目标id */
            int target = edge.getTargetId();
            /** 使用computeIfAbsent方法检查目标节点ID是否已经在physicalInEdgesInOrder映射中存在。 */
            List<StreamEdge> inEdges =
                    physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());
            /**
             * 如果存在，则直接获取现有的列表。然后，将当前边添加到对应目标节点ID的列表中。
              */
            inEdges.add(edge);
        }
        /** 遍历physicalInEdgesInOrder映射中的每一个条目。 */
        for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
            /**
             * 对于每个条目，提取其键（节点ID，vertex）和值（输入边列表，edgeList）
             */
            int vertex = inEdges.getKey();
            List<StreamEdge> edgeList = inEdges.getValue();
            /**
             * 放入toBeSerializedConfigObjects待序列化对象Map中
             */
            vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
        }
    }

    private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs(
            final Map<Integer, byte[]> hashes, final List<Map<Integer, byte[]>> legacyHashes) {

        final Map<Integer, ChainedSourceInfo> chainedSources = new HashMap<>();
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();

        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            final StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);

            if (sourceNode.getOperatorFactory() instanceof SourceOperatorFactory
                    && sourceNode.getOutEdges().size() == 1) {
                // as long as only NAry ops support this chaining, we need to skip the other parts
                final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
                final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
                final ChainingStrategy targetChainingStrategy =
                        target.getOperatorFactory().getChainingStrategy();

                if (targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES
                        && isChainableInput(sourceOutEdge, streamGraph)) {
                    final OperatorID opId = new OperatorID(hashes.get(sourceNodeId));
                    final StreamConfig.SourceInputConfig inputConfig =
                            new StreamConfig.SourceInputConfig(sourceOutEdge);
                    final StreamConfig operatorConfig = new StreamConfig(new Configuration());
                    setOperatorConfig(sourceNodeId, operatorConfig, Collections.emptyMap());
                    setOperatorChainedOutputsConfig(operatorConfig, Collections.emptyList());
                    // we cache the non-chainable outputs here, and set the non-chained config later
                    opNonChainableOutputsCache.put(sourceNodeId, Collections.emptyList());

                    operatorConfig.setChainIndex(0); // sources are always first
                    operatorConfig.setOperatorID(opId);
                    operatorConfig.setOperatorName(sourceNode.getOperatorName());
                    chainedSources.put(
                            sourceNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));

                    final SourceOperatorFactory<?> sourceOpFact =
                            (SourceOperatorFactory<?>) sourceNode.getOperatorFactory();
                    final OperatorCoordinator.Provider coord =
                            sourceOpFact.getCoordinatorProvider(sourceNode.getOperatorName(), opId);

                    final OperatorChainInfo chainInfo =
                            chainEntryPoints.computeIfAbsent(
                                    sourceOutEdge.getTargetId(),
                                    (k) ->
                                            new OperatorChainInfo(
                                                    sourceOutEdge.getTargetId(),
                                                    hashes,
                                                    legacyHashes,
                                                    chainedSources,
                                                    streamGraph));
                    chainInfo.addCoordinatorProvider(coord);
                    chainInfo.recordChainedNode(sourceNodeId);
                    continue;
                }
            }

            chainEntryPoints.put(
                    sourceNodeId,
                    new OperatorChainInfo(
                            sourceNodeId, hashes, legacyHashes, chainedSources, streamGraph));
        }

        return chainEntryPoints;
    }

    /**
     * Sets up task chains from the source {@link StreamNode} instances.
     *
     * <p>This will recursively create all {@link JobVertex} instances.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从StreamNode 实例设置任务链。
    */
    private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes) {
        // we separate out the sources that run as inputs to another operator (chained inputs)
        // from the sources that needs to run as the main (head) operator.
        /**
         * 获取源头节点，封装为Map<Integer, OperatorChainInfo>
         */
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildChainedInputsAndGetHeadInputs(hashes, legacyHashes);
        /**
         * 将Map<Integer, OperatorChainInfo>封装为集合结合
         */
        final Collection<OperatorChainInfo> initialEntryPoints =
                chainEntryPoints.entrySet().stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        // iterate over a copy of the values, because this map gets concurrently modified
        /**
         * 循环迭代调用对OperatorChainInfo进行循环
         */
        for (OperatorChainInfo info : initialEntryPoints) {
            createChain(
                    info.getStartNodeId(),
                    1, // operators start at position 1 because 0 is for chained source inputs
                    info,
                    chainEntryPoints);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param currentNodeId 当前节点的ID。
     * @param chainIndex  操作链的索引，可能是用来追踪链的深度或位置
     * @param chainInfo 关于操作链的信息
     * @param chainEntryPoints 可能是节点ID 映射到OperatorChainInfo对象。
    */
    private List<StreamEdge> createChain(
            final Integer currentNodeId,
            final int chainIndex,
            final OperatorChainInfo chainInfo,
            final Map<Integer, OperatorChainInfo> chainEntryPoints) {
        /** 从chainInfo中获取起始节点ID */
        Integer startNodeId = chainInfo.getStartNodeId();
        /**
         * 并检查这个起始节点是否已经被构建（即是否在builtVertices中
         */
        if (!builtVertices.contains(startNodeId)) {
            /** 物理出边（transitive的意思是“传递性”，在图结构中，节点通过传递性关系所能连接到的出边。） */
            List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();
            /** 存放可以被链接的出边列表 */
            List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
            /** 存放不可以被链接的出边列表 */
            List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();
            /** 从streamGraph中获取当前节点的StreamNode对象。 */
            StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
            /**
             * 获取当前节点输出的StreamEdge循环便利
             */
            for (StreamEdge outEdge : currentNode.getOutEdges()) {
                /** 根据出边，判断该节点与其下游节点是否可以被链接在一起 */
                if (isChainable(outEdge, streamGraph)) {
                    chainableOutputs.add(outEdge);
                } else {
                    /** 如果不可以被链接在一起，则把该出边添加到 nonChainableOutputs*/
                    nonChainableOutputs.add(outEdge);
                }
            }
            /** 遍历可以被链接在一起的边，递归调用createChain方法，将所有结果全部添加到transitiveOutEdges中
             *  重点：将下一个chainable.getTargetId()也就是StreanNode传入进去
             *  同时chainInfo.getStartNodeId() 里面还有开始NodeId，
             *  这样后面就可以比较是要在当前方法中创建JobVertex
             */
            for (StreamEdge chainable : chainableOutputs) {
                transitiveOutEdges.addAll(
                        createChain(
                                chainable.getTargetId(),
                                chainIndex + 1,
                                chainInfo,
                                chainEntryPoints));
            }
            /**
             * 遍历不可以被链接在一起的边，将其添加到transitiveOutEdges中，并递归调用
             * 为什么直接放入transitiveOutEdges集合中，说明待会要直接创建链接的边
             * 此时重新构建chainInfo.newChain(nonChainable.getTargetId()) 相当于
             * startNodeId = TargetId
             */
            for (StreamEdge nonChainable : nonChainableOutputs) {
                transitiveOutEdges.add(nonChainable);
                createChain(
                        nonChainable.getTargetId(),
                        1, // operators start at position 1 because 0 is for chained source inputs
                        chainEntryPoints.computeIfAbsent(
                                nonChainable.getTargetId(),
                                (k) -> chainInfo.newChain(nonChainable.getTargetId())),
                        chainEntryPoints);
            }
            /**
             * 将当前id对应链的名字放入进去
             */
            chainedNames.put(
                    currentNodeId,
                    createChainedName(
                            currentNodeId,
                            chainableOutputs,
                            Optional.ofNullable(chainEntryPoints.get(currentNodeId))));
            /** chain的最小资源*/
            chainedMinResources.put(
                    currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
            /** chain的首选资源*/
            chainedPreferredResources.put(
                    currentNodeId,
                    createChainedPreferredResources(currentNodeId, chainableOutputs));

            /** 当前节点添加到链中，并获取该节点的操作符ID */
            OperatorID currentOperatorId =
                    chainInfo.addNodeToChain(
                            currentNodeId,
                            streamGraph.getStreamNode(currentNodeId).getOperatorName());
            /**
             * 检查当前节点是否有输入或输出格式，并如果有的话，将其添加到格式容器中
             */
            if (currentNode.getInputFormat() != null) {
                getOrCreateFormatContainer(startNodeId)
                        .addInputFormat(currentOperatorId, currentNode.getInputFormat());
            }

            if (currentNode.getOutputFormat() != null) {
                getOrCreateFormatContainer(startNodeId)
                        .addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
            }
            /**
             * 根据当前节点是否是链的起始节点(startId=currentId)，创建或获取StreamConfig对象。
             * 如果是起始节点，则调用createJobVertex方法创建它；
             * 否则，创建一个新的StreamConfig对象。
             */
            StreamConfig config =
                    currentNodeId.equals(startNodeId)
                            ? createJobVertex(startNodeId, chainInfo)
                            : new StreamConfig(new Configuration());
            /**
             * 尝试转换动态分区，AdaptiveBatchScheduler Flink根据用户作业执行情况，自动设置并行度。
             * 这里dynamic为false不会处罚
             */
            tryConvertPartitionerForDynamicGraph(chainableOutputs, nonChainableOutputs);
            /**
             * 设置当前节点的操作符配置
             * checkpoint、inputConfig、setTypeSerializerOut、时间语义配置
             */
            setOperatorConfig(currentNodeId, config, chainInfo.getChainedSources());
            /**
             * 链式输出配置，这通常涉及到可以链接的输出边的处理。
             * 比如侧输出流
             */
            setOperatorChainedOutputsConfig(config, chainableOutputs);

            // we cache the non-chainable outputs here, and set the non-chained config later
            /**
             * 缓存不可链接的输出，稍后设置不可链接配置
             * 下面会用到
             */
            opNonChainableOutputsCache.put(currentNodeId, nonChainableOutputs);
            /**
             * 如果当前节点是启始节点
             */
            if (currentNodeId.equals(startNodeId)) {
                /** 设置物理边 */
                chainInfo.setTransitiveOutEdges(transitiveOutEdges);
                /** 开始id,链信息放入map接口 */
                chainInfos.put(startNodeId, chainInfo);
                /** 设置isChainedSubtask为true */
                config.setChainStart();
                /** 设置chainIndex */
                config.setChainIndex(chainIndex);
                /** 设置OperatorName */
                config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
                /**
                 * Map<Integer, StreamConfig> chainedTaskConfigs
                 */
                config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

            } else {
                /**
                 * 如果不是构建 Map<Integer, StreamConfig> chainedTaskConfigs
                 */
                chainedConfigs.computeIfAbsent(
                        startNodeId, k -> new HashMap<Integer, StreamConfig>());
                /** 设置chainIndex */
                config.setChainIndex(chainIndex);
                /** 获取StreamNode */
                StreamNode node = streamGraph.getStreamNode(currentNodeId);
                /** 获取OperatorName*/
                config.setOperatorName(node.getOperatorName());
                /**
                 * 更新currentNodeId对应的StreamConfig
                 */
                chainedConfigs.get(startNodeId).put(currentNodeId, config);
            }
            /**
             * 设置当前operatorId uuid唯一值
             */
            config.setOperatorID(currentOperatorId);

            /**
             * 设置可连接的边chainEnd 结束标识
             */
            if (chainableOutputs.isEmpty()) {
                config.setChainEnd();
            }
            return transitiveOutEdges;

        } else {
            return new ArrayList<>();
        }
    }

    /**
     * This method is used to reset or set job vertices' parallelism for dynamic graph:
     *
     * <p>1. Reset parallelism for job vertices whose parallelism is not configured.
     *
     * <p>2. Set parallelism and maxParallelism for job vertices in forward group, to ensure the
     * parallelism and maxParallelism of vertices in the same forward group to be the same; set the
     * parallelism at early stage if possible, to avoid invalid partition reuse.
     */
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 此方法用于重置或设置动态图的作业顶点的平行度：
      * 1.重置未配置并行度的作业顶点的并行度
      * 2.为正向组中的作业顶点设置平行度和maxParallelism，以确保同一正向组中顶点的平行度和最大平行度相同；
     * 如果可能的话，在早期阶段设置并行度，以避免无效的分区重用。
      */
    private void setVertexParallelismsForDynamicGraphIfNecessary() {
        // Note that the jobVertices are reverse topological order
        final List<JobVertex> topologicalOrderVertices =
                IterableUtils.toStream(jobVertices.values()).collect(Collectors.toList());
        Collections.reverse(topologicalOrderVertices);

        // reset parallelism for job vertices whose parallelism is not configured
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    final OperatorChainInfo chainInfo = chainInfos.get(startNodeId);
                    if (!jobVertex.isParallelismConfigured()
                            && streamGraph.isAutoParallelismEnabled()) {
                        jobVertex.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
                        chainInfo
                                .getAllChainedNodes()
                                .forEach(
                                        n ->
                                                n.setParallelism(
                                                        ExecutionConfig.PARALLELISM_DEFAULT,
                                                        false));
                    }
                });

        final Map<JobVertex, Set<JobVertex>> forwardProducersByJobVertex = new HashMap<>();
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    Set<JobVertex> forwardConsumers =
                            chainInfos.get(startNodeId).getTransitiveOutEdges().stream()
                                    .filter(
                                            edge ->
                                                    edge.getPartitioner()
                                                            instanceof ForwardPartitioner)
                                    .map(StreamEdge::getTargetId)
                                    .map(jobVertices::get)
                                    .collect(Collectors.toSet());

                    for (JobVertex forwardConsumer : forwardConsumers) {
                        forwardProducersByJobVertex.compute(
                                forwardConsumer,
                                (ignored, producers) -> {
                                    if (producers == null) {
                                        producers = new HashSet<>();
                                    }
                                    producers.add(jobVertex);
                                    return producers;
                                });
                    }
                });

        // compute forward groups
        final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId =
                ForwardGroupComputeUtil.computeForwardGroups(
                        topologicalOrderVertices,
                        jobVertex ->
                                forwardProducersByJobVertex.getOrDefault(
                                        jobVertex, Collections.emptySet()));

        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getID());
                    // set parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        jobVertex.setParallelism(forwardGroup.getParallelism());
                        jobVertex.setParallelismConfigured(true);
                        chainInfos
                                .get(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setParallelism(
                                                        forwardGroup.getParallelism(), true));
                    }

                    // set max parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isMaxParallelismDecided()) {
                        jobVertex.setMaxParallelism(forwardGroup.getMaxParallelism());
                        chainInfos
                                .get(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setMaxParallelism(
                                                        forwardGroup.getMaxParallelism()));
                    }
                });
    }

    private void checkAndReplaceReusableHybridPartitionType(NonChainedOutput reusableOutput) {
        if (reusableOutput.getPartitionType() == ResultPartitionType.HYBRID_SELECTIVE) {
            // for can be reused hybrid output, it can be optimized to always use full
            // spilling strategy to significantly reduce shuffle data writing cost.
            reusableOutput.setPartitionType(ResultPartitionType.HYBRID_FULL);
            LOG.info(
                    "{} result partition has been replaced by {} result partition to support partition reuse,"
                            + " which will reduce shuffle data writing cost.",
                    reusableOutput.getPartitionType().name(),
                    ResultPartitionType.HYBRID_FULL.name());
        }
    }

    private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
        return chainedInputOutputFormats.computeIfAbsent(
                startNodeId,
                k ->
                        new InputOutputFormatContainer(
                                Thread.currentThread().getContextClassLoader()));
    }

    private String createChainedName(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            Optional<OperatorChainInfo> operatorChainInfo) {
        List<ChainedSourceInfo> chainedSourceInfos =
                operatorChainInfo
                        .map(chainInfo -> getChainedSourcesByVertexId(vertexID, chainInfo))
                        .orElse(Collections.emptyList());
        final String operatorName =
                nameWithChainedSourcesInfo(
                        streamGraph.getStreamNode(vertexID).getOperatorName(), chainedSourceInfos);
        if (chainedOutputs.size() > 1) {
            List<String> outputChainedNames = new ArrayList<>();
            for (StreamEdge chainable : chainedOutputs) {
                outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
            }
            return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
        } else if (chainedOutputs.size() == 1) {
            return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
        } else {
            return operatorName;
        }
    }

    private List<ChainedSourceInfo> getChainedSourcesByVertexId(
            Integer vertexId, OperatorChainInfo chainInfo) {
        return streamGraph.getStreamNode(vertexId).getInEdges().stream()
                .map(inEdge -> chainInfo.getChainedSources().get(inEdge.getSourceId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private ResourceSpec createChainedMinResources(
            Integer vertexID, List<StreamEdge> chainedOutputs) {
        ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
        for (StreamEdge chainable : chainedOutputs) {
            minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
        }
        return minResources;
    }

    private ResourceSpec createChainedPreferredResources(
            Integer vertexID, List<StreamEdge> chainedOutputs) {
        ResourceSpec preferredResources =
                streamGraph.getStreamNode(vertexID).getPreferredResources();
        for (StreamEdge chainable : chainedOutputs) {
            preferredResources =
                    preferredResources.merge(
                            chainedPreferredResources.get(chainable.getTargetId()));
        }
        return preferredResources;
    }

    private StreamConfig createJobVertex(Integer streamNodeId, OperatorChainInfo chainInfo) {

        JobVertex jobVertex;
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

        byte[] hash = chainInfo.getHash(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException(
                    "Cannot find node hash. "
                            + "Did you generate them before calling this method?");
        }

        JobVertexID jobVertexId = new JobVertexID(hash);

        List<Tuple2<byte[], byte[]>> chainedOperators =
                chainInfo.getChainedOperatorHashes(streamNodeId);
        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
        if (chainedOperators != null) {
            for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorID =
                        chainedOperator.f1 == null ? null : new OperatorID(chainedOperator.f1);
                operatorIDPairs.add(
                        OperatorIDPair.of(
                                new OperatorID(chainedOperator.f0), userDefinedOperatorID));
            }
        }

        if (chainedInputOutputFormats.containsKey(streamNodeId)) {
            jobVertex =
                    new InputOutputFormatVertex(
                            chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);

            chainedInputOutputFormats
                    .get(streamNodeId)
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex = new JobVertex(chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);
        }

        if (streamNode.getConsumeClusterDatasetId() != null) {
            jobVertex.addIntermediateDataSetIdToConsume(streamNode.getConsumeClusterDatasetId());
        }

        final List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>
                serializationFutures = new ArrayList<>();
        for (OperatorCoordinator.Provider coordinatorProvider :
                chainInfo.getCoordinatorProviders()) {
            serializationFutures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new SerializedValue<>(coordinatorProvider);
                                } catch (IOException e) {
                                    throw new FlinkRuntimeException(
                                            String.format(
                                                    "Coordinator Provider for node %s is not serializable.",
                                                    chainedNames.get(streamNodeId)),
                                            e);
                                }
                            },
                            serializationExecutor));
        }
        if (!serializationFutures.isEmpty()) {
            coordinatorSerializationFuturesPerJobVertex.put(jobVertexId, serializationFutures);
        }

        jobVertex.setResources(
                chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

        jobVertex.setInvokableClass(streamNode.getJobVertexClass());

        int parallelism = streamNode.getParallelism();

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        } else {
            parallelism = jobVertex.getParallelism();
        }

        jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
        }

        jobVertices.put(streamNodeId, jobVertex);
        builtVertices.add(streamNodeId);
        jobGraph.addVertex(jobVertex);

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return new StreamConfig(jobVertex.getConfiguration());
    }

    private void setOperatorConfig(
            Integer vertexId, StreamConfig config, Map<Integer, ChainedSourceInfo> chainedSources) {

        StreamNode vertex = streamGraph.getStreamNode(vertexId);

        config.setVertexID(vertexId);

        // build the inputs as a combination of source and network inputs
        final List<StreamEdge> inEdges = vertex.getInEdges();
        final TypeSerializer<?>[] inputSerializers = vertex.getTypeSerializersIn();

        final StreamConfig.InputConfig[] inputConfigs =
                new StreamConfig.InputConfig[inputSerializers.length];

        int inputGateCount = 0;
        for (final StreamEdge inEdge : inEdges) {
            final ChainedSourceInfo chainedSource = chainedSources.get(inEdge.getSourceId());

            final int inputIndex =
                    inEdge.getTypeNumber() == 0
                            ? 0 // single input operator
                            : inEdge.getTypeNumber() - 1; // in case of 2 or more inputs

            if (chainedSource != null) {
                // chained source is the input
                if (inputConfigs[inputIndex] != null) {
                    throw new IllegalStateException(
                            "Trying to union a chained source with another input.");
                }
                inputConfigs[inputIndex] = chainedSource.getInputConfig();
                chainedConfigs
                        .computeIfAbsent(vertexId, (key) -> new HashMap<>())
                        .put(inEdge.getSourceId(), chainedSource.getOperatorConfig());
            } else {
                // network input. null if we move to a new input, non-null if this is a further edge
                // that is union-ed into the same input
                if (inputConfigs[inputIndex] == null) {
                    // PASS_THROUGH is a sensible default for streaming jobs. Only for BATCH
                    // execution can we have sorted inputs
                    StreamConfig.InputRequirement inputRequirement =
                            vertex.getInputRequirements()
                                    .getOrDefault(
                                            inputIndex, StreamConfig.InputRequirement.PASS_THROUGH);
                    inputConfigs[inputIndex] =
                            new StreamConfig.NetworkInputConfig(
                                    inputSerializers[inputIndex],
                                    inputGateCount++,
                                    inputRequirement);
                }
            }
        }

        // set the input config of the vertex if it consumes from cached intermediate dataset.
        /** 如果顶点使用缓存的中间数据集，则设置顶点的输入配置。 */
        if (vertex.getConsumeClusterDatasetId() != null) {
            config.setNumberOfNetworkInputs(1);
            inputConfigs[0] = new StreamConfig.NetworkInputConfig(inputSerializers[0], 0);
        }

        config.setInputs(inputConfigs);

        config.setTypeSerializerOut(vertex.getTypeSerializerOut());

        config.setStreamOperatorFactory(vertex.getOperatorFactory());

        config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

        final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

        config.setStateBackend(streamGraph.getStateBackend());
        config.setCheckpointStorage(streamGraph.getCheckpointStorage());
        config.setGraphContainingLoops(streamGraph.isIterative());
        config.setTimerServiceProvider(streamGraph.getTimerServiceProvider());
        config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
        config.getConfiguration()
                .set(
                        ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                        streamGraph.isEnableCheckpointsAfterTasksFinish());
        config.setCheckpointMode(getCheckpointingMode(checkpointCfg));
        config.setUnalignedCheckpointsEnabled(checkpointCfg.isUnalignedCheckpointsEnabled());
        config.setAlignedCheckpointTimeout(checkpointCfg.getAlignedCheckpointTimeout());
        config.setMaxSubtasksPerChannelStateFile(checkpointCfg.getMaxSubtasksPerChannelStateFile());
        config.setMaxConcurrentCheckpoints(checkpointCfg.getMaxConcurrentCheckpoints());

        for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
            config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
        }
        //设置statekeyser
        config.setStateKeySerializer(vertex.getStateKeySerializer());

        Class<? extends TaskInvokable> vertexClass = vertex.getJobVertexClass();

        if (vertexClass.equals(StreamIterationHead.class)
                || vertexClass.equals(StreamIterationTail.class)) {
            config.setIterationId(streamGraph.getBrokerID(vertexId));
            config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexId));
        }

        vertexConfigs.put(vertexId, config);
    }

    private void setOperatorChainedOutputsConfig(
            StreamConfig config, List<StreamEdge> chainableOutputs) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : chainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        streamGraph.getExecutionConfig().getSerializerConfig()));
            }
        }
        config.setChainedOutputs(chainableOutputs);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为特定的Operator（由vertexId标识）设置非链式输出的配置。
    */
    private void setOperatorNonChainedOutputsConfig(
            Integer vertexId,
            StreamConfig config,
            List<StreamEdge> nonChainableOutputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        /**
         * 迭代边，查找边输出边为每个outputTag类型创建并保存序列化程序
         * 查看这边有没有配置侧输出流
         */
        for (StreamEdge edge : nonChainableOutputs) {
            if (edge.getOutputTag() != null) {
                /***
                 * 为typeSerializer_sideout_添加序列化配置
                 */
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        streamGraph.getExecutionConfig().getSerializerConfig()));
            }
        }

        List<NonChainedOutput> deduplicatedOutputs =
                mayReuseNonChainedOutputs(vertexId, nonChainableOutputs, outputsConsumedByEdge);
        /** 设置NUMBER_OF_OUTPUTS 对应输出的数量 */
        config.setNumberOfOutputs(deduplicatedOutputs.size());
        /**
         * opNonChainedOutputs,deduplicatedOutputs 放入序列化对象的
         * Map<String, Object> toBeSerializedConfigObjects = new HashMap<>();
         * toBeSerializedConfigObjects映射来收集所有需要序列化的对象。这些对象将同时被序列化。
         */
        config.setOperatorNonChainedOutputs(deduplicatedOutputs);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param startNodeId: 起始顶点的ID。
     * @param config: 与起始顶点关联的流配置对象。
     * @param transitiveOutEdges: 从起始顶点出发的传递性输出边的列表。
     * @param opIntermediateOutputs: 中间非链式输出的映射，其中键是操作或顶点的ID，值是一个映射，其键是StreamEdge（流图中的边），
     * 值是NonChainedOutput（非链式输出）。
     * @param
    */
    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            StreamConfig config,
            List<StreamEdge> transitiveOutEdges,
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {
        /**
         * 使用LinkedHashSet来存储非链式输出，确保输出的唯一性，同时保持插入顺序。
         */
        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        /**
         * 对于起始顶点startNodeId的每一个传递性输出边edge，
         */
        for (StreamEdge edge : transitiveOutEdges) {
            /***
             * 根据边的源ID（edge.getSourceId()）从opIntermediateOutputs映射中获取对应的非链式输出映射，然后从该映射中根据边edge获取非链式输出output。
             */
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            /**
             * 将获取到的非链式输出添加到transitiveOutputs集合中
             */
            transitiveOutputs.add(output);

            connect(startNodeId, edge, output);
        }
        /**
         * 将transitiveOutputs集合转换为一个ArrayList，并设置到顶点的流配置config中，作为该顶点的非链式输出。
         */
        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为所有 Operators 设置非链式输出（non-chained outputs）的配置。
    */
    private void setAllOperatorNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {
        // set non chainable output config
        /**
         * Map<Integer, List<StreamEdge>> opNonChainableOutputsCache里面存放了所有的StreamEdge
         * 循环遍历opNonChainableOutputsCache 获取vertexId 也就是StreamNodeId(1、2、3、5、6)
         */
        opNonChainableOutputsCache.forEach(
                (vertexId, nonChainableOutputs) -> {
                    /**
                     * 对于当前的vertexId，从opIntermediateOutputs这个Map中尝试获取其对应的输出。
                     * 如果opIntermediateOutputs中没有这个vertexId，则为其创建一个新的空的HashMap。
                     */
                    Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge =
                            opIntermediateOutputs.computeIfAbsent(
                                    vertexId, ignored -> new HashMap<>());
                    /**
                     * 调用setOperatorNonChainedOutputsConfig方法，为特定的vertexId设置非链式输出的配置。
                     */
                    setOperatorNonChainedOutputsConfig(
                            vertexId,
                            vertexConfigs.get(vertexId),
                            nonChainableOutputs,
                            outputsConsumedByEdge);
                });
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param
    */
    private void setAllVertexNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {
        jobVertices
                .keySet() /**  这段代码从jobVertices映射中提取所有的键，这些键可能是作业中顶点的ID。 */
                .forEach( /**  对jobVertices的每一个键（即每一个顶点的ID）执行一个操作。 */
                        startNodeId ->
                                /**
                                 * 对于每一个startNodeId（起始顶点ID），这个方法调用setVertexNonChainedOutputsConfig，并为该顶点设置非链式输出配置。
                                 */
                                setVertexNonChainedOutputsConfig(
                                        startNodeId,
                                        vertexConfigs.get(startNodeId),
                                        chainInfos.get(startNodeId).getTransitiveOutEdges(),
                                        opIntermediateOutputs));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 操作符的非链式输出相关的逻辑
     * @param   vertexId：操作符或顶点的ID。
     * @param  consumerEdges：包含消费非链式输出的边的列表。
     * @param  outputsConsumedByEdge：一个映射，表示每个StreamEdge对应的非链式输出。
    */
    private List<NonChainedOutput> mayReuseNonChainedOutputs(
            int vertexId,
            List<StreamEdge> consumerEdges,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge) {
        /**如果consumerEdges列表为空，方法直接返回一个空的ArrayList。*/
        if (consumerEdges.isEmpty()) {
            return new ArrayList<>();
        }
        /** 创建一个新的ArrayList来存储非链式输出，其初始大小设置为consumerEdges的大小。 */
        List<NonChainedOutput> outputs = new ArrayList<>(consumerEdges.size());
        /** 遍历consumerEdges列表中的每个StreamEdge。 */
        for (StreamEdge consumerEdge : consumerEdges) {
            /** 使用checkState方法检查当前vertexId是否与consumerEdge的源ID相同。如果不同，将抛出异常，因为顶点ID必须相同。 */
            checkState(vertexId == consumerEdge.getSourceId(), "Vertex id must be the same.");
            /** 调用getResultPartitionType方法（该方法的具体实现在这段代码中未给出）来获取consumerEdge的结果分区类型。 */
            ResultPartitionType partitionType = getResultPartitionType(consumerEdge);
            /** 创建 IntermediateDataSetID 对象 内部uuid*/
            IntermediateDataSetID dataSetId = new IntermediateDataSetID();

            boolean isPersistentDataSet =
                    isPersistentIntermediateDataset(partitionType, consumerEdge);
            /**
             * 检查consumerEdge是否关联一个持久化的中间数据集。
             * 如果是，则将分区类型设置为BLOCKING_PERSISTENT，并获取相应的中间数据集ID。
             * 总结这两端是获取partitionType与dataSetId
             */
            if (isPersistentDataSet) {
                partitionType = ResultPartitionType.BLOCKING_PERSISTENT;
                dataSetId = consumerEdge.getIntermediateDatasetIdToProduce();
            }

            if (partitionType.isHybridResultPartition()) {
                hasHybridResultPartition = true;
                if (consumerEdge.getPartitioner().isBroadcast()
                        && partitionType == ResultPartitionType.HYBRID_SELECTIVE) {
                    // for broadcast result partition, it can be optimized to always use full
                    // spilling strategy to significantly reduce shuffle data writing cost.
                    LOG.info(
                            "{} result partition has been replaced by {} result partition to support "
                                    + "broadcast optimization, which will reduce shuffle data writing cost.",
                            partitionType.name(),
                            ResultPartitionType.HYBRID_FULL.name());
                    partitionType = ResultPartitionType.HYBRID_FULL;
                }
            }

            createOrReuseOutput(
                    outputs,
                    outputsConsumedByEdge,
                    consumerEdge,
                    isPersistentDataSet,
                    dataSetId,
                    partitionType);
        }
        return outputs;
    }

    private void createOrReuseOutput(
            List<NonChainedOutput> outputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            StreamEdge consumerEdge,
            boolean isPersistentDataSet,
            IntermediateDataSetID dataSetId,
            ResultPartitionType partitionType) {
        /** 获取消费(输出)方的 StreamNode的并行度*/
        int consumerParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getParallelism();
        /** 获取消费(输出)方的 StreamNode的最大并行度*/
        int consumerMaxParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getMaxParallelism();
        /** 创建非链Operator的output 变量*/
        NonChainedOutput reusableOutput = null;
        if (isPartitionTypeCanBeReuse(partitionType)) {
            for (NonChainedOutput outputCandidate : outputsConsumedByEdge.values()) {
                // Reusing the same output can improve performance. The target output can be reused
                // if meeting the following conditions:
                // 1. all is hybrid partition or are same re-consumable partition.
                // 2. have the same partitioner, consumer parallelism, persistentDataSetId,
                // outputTag.
                if (allHybridOrSameReconsumablePartitionType(
                                outputCandidate.getPartitionType(), partitionType)
                        && consumerParallelism == outputCandidate.getConsumerParallelism()
                        && consumerMaxParallelism == outputCandidate.getConsumerMaxParallelism()
                        && Objects.equals(
                                outputCandidate.getPersistentDataSetId(),
                                consumerEdge.getIntermediateDatasetIdToProduce())
                        && Objects.equals(
                                outputCandidate.getOutputTag(), consumerEdge.getOutputTag())
                        && Objects.equals(
                                consumerEdge.getPartitioner(), outputCandidate.getPartitioner())) {
                    reusableOutput = outputCandidate;
                    outputsConsumedByEdge.put(consumerEdge, reusableOutput);
                    checkAndReplaceReusableHybridPartitionType(reusableOutput);
                    break;
                }
            }
        }
        if (reusableOutput == null) {
            NonChainedOutput output =
                    new NonChainedOutput(
                            consumerEdge.supportsUnalignedCheckpoints(),
                            consumerEdge.getSourceId(),
                            consumerParallelism,
                            consumerMaxParallelism,
                            consumerEdge.getBufferTimeout(),
                            isPersistentDataSet,
                            dataSetId,
                            consumerEdge.getOutputTag(),
                            consumerEdge.getPartitioner(),
                            partitionType);
            outputs.add(output);
            outputsConsumedByEdge.put(consumerEdge, output);
        }
    }

    private boolean isPartitionTypeCanBeReuse(ResultPartitionType partitionType) {
        // for non-hybrid partition, partition reuse only works when its re-consumable.
        // for hybrid selective partition, it still has the opportunity to be converted to
        // hybrid full partition to support partition reuse.
        return partitionType.isReconsumable() || partitionType.isHybridResultPartition();
    }

    private boolean allHybridOrSameReconsumablePartitionType(
            ResultPartitionType partitionType1, ResultPartitionType partitionType2) {
        return (partitionType1.isReconsumable() && partitionType1 == partitionType2)
                || (partitionType1.isHybridResultPartition()
                        && partitionType2.isHybridResultPartition());
    }

    private void tryConvertPartitionerForDynamicGraph(
            List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

        for (StreamEdge edge : chainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner
                    || partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        String.format(
                                "%s should only be used in dynamic graph.",
                                partitioner.getClass().getSimpleName()));
                edge.setPartitioner(new ForwardPartitioner<>());
            }
        }
        for (StreamEdge edge : nonChainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForConsecutiveHashPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(
                        ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                .getHashPartitioner());
            } else if (partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForUnspecifiedPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(new RescalePartitioner<>());
            }
        }
    }

    private CheckpointingMode getCheckpointingMode(CheckpointConfig checkpointConfig) {
        CheckpointingMode checkpointingMode = checkpointConfig.getCheckpointingMode();

        checkArgument(
                checkpointingMode == CheckpointingMode.EXACTLY_ONCE
                        || checkpointingMode == CheckpointingMode.AT_LEAST_ONCE,
                "Unexpected checkpointing mode.");

        if (checkpointConfig.isCheckpointingEnabled()) {
            return checkpointingMode;
        } else {
            // the "at-least-once" input handler is slightly cheaper (in the absence of
            // checkpoints),
            // so we use that one if checkpointing is not enabled
            return CheckpointingMode.AT_LEAST_ONCE;
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param headOfChain: 链的头部顶点的ID。
     * @param edge: 表示流图中从headOfChain到下游顶点的边的StreamEdge对象。
     * @param output: 与edge相关联的非链式输出对象。
    */
    private void connect(Integer headOfChain, StreamEdge edge, NonChainedOutput output) {
        /** 将edge添加到physicalEdgesInOrder列表中 */
        physicalEdgesInOrder.add(edge);
        /**
         * 从edge对象中获取目标（下游）顶点的ID。
         */
        Integer downStreamVertexID = edge.getTargetId();
        /**
         * 使用ID从jobVertices映射中获取链的头部顶点和下游顶点对象。
         */
        JobVertex headVertex = jobVertices.get(headOfChain);
        JobVertex downStreamVertex = jobVertices.get(downStreamVertexID);
        /**
         * 下游顶点创建一个新的流配置对象，该对象可能基于顶点的现有配置
         */
        StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());
        /**
         * 正在添加一个新的输入边，因此需要增加下游顶点的网络输入数量
          */
        downStreamConfig.setNumberOfNetworkInputs(downStreamConfig.getNumberOfNetworkInputs() + 1);
        /**
         * 从非链式输出对象中获取分区器和结果分区类型。
         */
        StreamPartitioner<?> partitioner = output.getPartitioner();
        ResultPartitionType resultPartitionType = output.getPartitionType();

        /** 检查与结果分区类型和边相关的缓冲区超时设置。 */
        checkBufferTimeout(resultPartitionType, edge);
        /**
         * 根据分区器是否是点对点的，使用不同的DistributionPattern（点对点或全对全）来连接头部顶点和下游顶点。这创建了一个新的JobEdge对象，表示两个顶点之间的连接。
         * pointwise
         下游节点在和上游节点建立连接时，只有POINTWISE和ALL_TO_ALL两种模式，
         事实上只有RescalePartitioner和ForwardPartitioner是POINTWISE模式，其他的都是ALL_TO_ALL。
         默认情况下如果不指定partitioner，如果上游节点和下游节点并行度一样为ForwardPartitioner，
         否则为RebalancePartioner ，前者POINTWISE，后者ALL_TO_ALL。
         */
        JobEdge jobEdge;
        if (partitioner.isPointwise()) {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.POINTWISE,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        } else {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.ALL_TO_ALL,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        }

        // set strategy name so that web interface can show it.
        /**
         * 这行代码将partitioner对象的字符串表示形式（通常是通过调用toString()方法得到的）设置为jobEdge的发送策略名称。
         * 发送策略决定了数据如何在不同的子任务之间传输。
         */
        jobEdge.setShipStrategyName(partitioner.toString());
        /**
         * 这行代码检查partitioner是否是ForwardPartitioner的实例。如果是，它将jobEdge的forward属性设置为true，
         * 否则为false。forward属性可能表示是否采用某种直接转发策略。
         */
        jobEdge.setForward(partitioner instanceof ForwardPartitioner);
        /**
         * 这行代码获取partitioner的下游子任务状态映射器，并将其设置为jobEdge的下游子任务状态映射器。
         * 状态映射器通常用于在容错场景中，确定哪个子任务的状态应该被哪个其他子任务恢复。
         */
        jobEdge.setDownstreamSubtaskStateMapper(partitioner.getDownstreamSubtaskStateMapper());
        /**
         * 这行代码获取partitioner的上游子任务状态映射器，并将其设置为jobEdge的上游子任务状态映射器。
         * 与下游状态映射器类似，上游状态映射器也用于在容错场景中。
         */
        jobEdge.setUpstreamSubtaskStateMapper(partitioner.getUpstreamSubtaskStateMapper());
        /**
         * 检查日志是否启用了调试级别。如果是，则记录一条调试级别的日志，
         */
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "CONNECTED: {} - {} -> {}",
                    partitioner.getClass().getSimpleName(),
                    headOfChain,
                    downStreamVertexID);
        }
    }

    private boolean isPersistentIntermediateDataset(
            ResultPartitionType resultPartitionType, StreamEdge edge) {
        /**
         *  this == BLOCKING || this == BLOCKING_PERSISTENT
         */
        return resultPartitionType.isBlockingOrBlockingPersistentResultPartition()
                && edge.getIntermediateDatasetIdToProduce() != null;
    }

    private void checkBufferTimeout(ResultPartitionType type, StreamEdge edge) {
        long bufferTimeout = edge.getBufferTimeout();
        if (!type.canBePipelinedConsumed()
                && bufferTimeout != ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT) {
            throw new UnsupportedOperationException(
                    "only canBePipelinedConsumed partition support buffer timeout "
                            + bufferTimeout
                            + " for src operator in edge "
                            + edge
                            + ". \nPlease either disable buffer timeout (via -1) or use the canBePipelinedConsumed partition.");
        }
    }

    private ResultPartitionType getResultPartitionType(StreamEdge edge) {
        switch (edge.getExchangeMode()) {
            case PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case BATCH:
                return ResultPartitionType.BLOCKING;
            case HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            case UNDEFINED:
                return determineUndefinedResultPartitionType(edge.getPartitioner());
            default:
                throw new UnsupportedOperationException(
                        "Data exchange mode " + edge.getExchangeMode() + " is not supported yet.");
        }
    }

    private ResultPartitionType determineUndefinedResultPartitionType(
            StreamPartitioner<?> partitioner) {
        switch (streamGraph.getGlobalStreamExchangeMode()) {
            case ALL_EDGES_BLOCKING:
                return ResultPartitionType.BLOCKING;
            case FORWARD_EDGES_PIPELINED:
                if (partitioner instanceof ForwardPartitioner) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case POINTWISE_EDGES_PIPELINED:
                if (partitioner.isPointwise()) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case ALL_EDGES_PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case ALL_EDGES_PIPELINED_APPROXIMATE:
                return ResultPartitionType.PIPELINED_APPROXIMATE;
            case ALL_EDGES_HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case ALL_EDGES_HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            default:
                throw new RuntimeException(
                        "Unrecognized global data exchange mode "
                                + streamGraph.getGlobalStreamExchangeMode());
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 判断Operator是否可以链到一起
    */
    public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);
        /**
         * 1.该出边的上游算子是该出边的下游算子的唯一输入
         */
        return downStreamVertex.getInEdges().size() == 1 && isChainableInput(edge, streamGraph);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 判断上下游是否可以合并为OperatorChain
    */
    private static boolean isChainableInput(StreamEdge edge, StreamGraph streamGraph) {
        /** 获取上游 StreamNode*/
        StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
        /** 获取下游 StreamNode*/
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);
        /**
         * 2.是否禁用pipeline.operator-chaining.enabled
         * 3.上下游必须在同一个slotSharingGroup
         */
        if (!(streamGraph.isChainingEnabled()
                && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
                && areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph)
                && arePartitionerAndExchangeModeChainable(
                        edge.getPartitioner(), edge.getExchangeMode(), streamGraph.isDynamic()))) {

            return false;
        }

        // check that we do not have a union operation, because unions currently only work
        // through the network/byte-channel stack.
        // we check that by testing that each "type" (which means input position) is used only once
        for (StreamEdge inEdge : downStreamVertex.getInEdges()) {
            if (inEdge != edge && inEdge.getTypeNumber() == edge.getTypeNumber()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 该出边的分区器为ForwardPartitioner，没有进行重分区
     * 该出边的ShuffleMode不为BATCH
    */
    @VisibleForTesting
    static boolean arePartitionerAndExchangeModeChainable(
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode,
            boolean isDynamicGraph) {
        if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
            checkState(isDynamicGraph);
            return true;
        } else if ((partitioner instanceof ForwardPartitioner)
                && exchangeMode != StreamExchangeMode.BATCH) {
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static boolean areOperatorsChainable(
            StreamNode upStreamVertex, StreamNode downStreamVertex, StreamGraph streamGraph) {
        StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
        StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();
        /**
         * 4.上下游的StreamOperatorFactory不能为空
         */
        if (downStreamOperator == null || upStreamOperator == null) {
            return false;
        }

        // yielding operators cannot be chained to legacy sources
        // unfortunately the information that vertices have been chained is not preserved at this
        // point
        if (downStreamOperator instanceof YieldingOperatorFactory
                && getHeadOperator(upStreamVertex, streamGraph).isLegacySource()) {
            return false;
        }

        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;
        /**
         * 5.下游的ChainingStrategy为ALWAYS
         * 6.上游的ChainingStrategy为ALWAYS、HEAD、HEAD_WITH_SOURCES
         * 7.上下游并行度必须一致
         */
        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + downStreamOperator.getChainingStrategy());
        }

        /**
         * 7.上下游并行度必须一致
         * 8.如果设置了pipeline.operator-chaining.chain-operators-with-different-max-parallelism
         * 则上下游最大并行度必须一致
         */
        // Only vertices with the same parallelism can be chained.
        isChainable &= upStreamVertex.getParallelism() == downStreamVertex.getParallelism();

        if (!streamGraph.isChainingOfOperatorsWithDifferentMaxParallelismEnabled()) {
            isChainable &=
                    upStreamVertex.getMaxParallelism() == downStreamVertex.getMaxParallelism();
        }

        return isChainable;
    }

    /** Backtraces the head of an operator chain. */
    private static StreamOperatorFactory<?> getHeadOperator(
            StreamNode upStreamVertex, StreamGraph streamGraph) {
        if (upStreamVertex.getInEdges().size() == 1
                && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
            return getHeadOperator(
                    streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)), streamGraph);
        }
        return upStreamVertex.getOperatorFactory();
    }

    private void markSupportingConcurrentExecutionAttempts() {
        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final JobVertex jobVertex = entry.getValue();
            final Set<Integer> vertexOperators = new HashSet<>();
            vertexOperators.add(entry.getKey());
            final Map<Integer, StreamConfig> vertexChainedConfigs =
                    chainedConfigs.get(entry.getKey());
            if (vertexChainedConfigs != null) {
                vertexOperators.addAll(vertexChainedConfigs.keySet());
            }

            // disable supportConcurrentExecutionAttempts of job vertex if there is any stream node
            // does not support it
            boolean supportConcurrentExecutionAttempts = true;
            for (int nodeId : vertexOperators) {
                final StreamNode streamNode = streamGraph.getStreamNode(nodeId);
                if (!streamNode.isSupportsConcurrentExecutionAttempts()) {
                    supportConcurrentExecutionAttempts = false;
                    break;
                }
            }
            jobVertex.setSupportsConcurrentExecutionAttempts(supportConcurrentExecutionAttempts);
        }
    }

    private void setSlotSharingAndCoLocation() {
        setSlotSharing();
        setCoLocation();
    }

    private void setSlotSharing() {
        final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups =
                buildVertexRegionSlotSharingGroups();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final JobVertex vertex = entry.getValue();
            final String slotSharingGroupKey =
                    streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

            checkNotNull(slotSharingGroupKey, "StreamNode slot sharing group must not be null");

            final SlotSharingGroup effectiveSlotSharingGroup;
            if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
                // fallback to the region slot sharing group by default
                effectiveSlotSharingGroup =
                        checkNotNull(vertexRegionSlotSharingGroups.get(vertex.getID()));
            } else {
                checkState(
                        !hasHybridResultPartition,
                        "hybrid shuffle mode currently does not support setting non-default slot sharing group.");

                effectiveSlotSharingGroup =
                        specifiedSlotSharingGroups.computeIfAbsent(
                                slotSharingGroupKey,
                                k -> {
                                    SlotSharingGroup ssg = new SlotSharingGroup();
                                    streamGraph
                                            .getSlotSharingGroupResource(k)
                                            .ifPresent(ssg::setResourceProfile);
                                    return ssg;
                                });
            }

            vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
        }
    }

    private void validateHybridShuffleExecuteInBatchMode() {
        if (hasHybridResultPartition) {
            checkState(
                    jobGraph.getJobType() == JobType.BATCH,
                    "hybrid shuffle mode only supports batch job, please set %s to %s",
                    ExecutionOptions.RUNTIME_MODE.key(),
                    RuntimeExecutionMode.BATCH.name());
        }
    }

    /**
     * Maps a vertex to its region slot sharing group. If {@link
     * StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()} returns true, all regions will be
     * in the same slot sharing group.
     */
    private Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups() {
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
        final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();
        streamGraph
                .getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .ifPresent(defaultSlotSharingGroup::setResourceProfile);

        final boolean allRegionsInSameSlotSharingGroup =
                streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

        final Iterable<DefaultLogicalPipelinedRegion> regions =
                DefaultLogicalTopology.fromJobGraph(jobGraph).getAllPipelinedRegions();
        for (DefaultLogicalPipelinedRegion region : regions) {
            final SlotSharingGroup regionSlotSharingGroup;
            if (allRegionsInSameSlotSharingGroup) {
                regionSlotSharingGroup = defaultSlotSharingGroup;
            } else {
                regionSlotSharingGroup = new SlotSharingGroup();
                streamGraph
                        .getSlotSharingGroupResource(
                                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .ifPresent(regionSlotSharingGroup::setResourceProfile);
            }

            for (LogicalVertex vertex : region.getVertices()) {
                vertexRegionSlotSharingGroups.put(vertex.getId(), regionSlotSharingGroup);
            }
        }

        return vertexRegionSlotSharingGroups;
    }

    private void setCoLocation() {
        final Map<String, Tuple2<SlotSharingGroup, CoLocationGroupImpl>> coLocationGroups =
                new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final StreamNode node = streamGraph.getStreamNode(entry.getKey());
            final JobVertex vertex = entry.getValue();
            final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

            // configure co-location constraint
            final String coLocationGroupKey = node.getCoLocationGroup();
            if (coLocationGroupKey != null) {
                if (sharingGroup == null) {
                    throw new IllegalStateException(
                            "Cannot use a co-location constraint without a slot sharing group");
                }

                Tuple2<SlotSharingGroup, CoLocationGroupImpl> constraint =
                        coLocationGroups.computeIfAbsent(
                                coLocationGroupKey,
                                k -> new Tuple2<>(sharingGroup, new CoLocationGroupImpl()));

                if (constraint.f0 != sharingGroup) {
                    throw new IllegalStateException(
                            "Cannot co-locate operators from different slot sharing groups");
                }

                vertex.updateCoLocationGroup(constraint.f1);
                constraint.f1.addVertex(vertex);
            }
        }
    }

    private static void setManagedMemoryFraction(
            final Map<Integer, JobVertex> jobVertices,
            final Map<Integer, StreamConfig> operatorConfigs,
            final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
            final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>>
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
            final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>>
                    slotScopeManagedMemoryUseCasesRetriever) {

        // all slot sharing groups in this job
        final Set<SlotSharingGroup> slotSharingGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // maps a job vertex ID to its head operator ID
        final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

        // maps a job vertex ID to IDs of all operators in the vertex
        final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final int headOperatorId = entry.getKey();
            final JobVertex jobVertex = entry.getValue();

            final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

            checkState(
                    jobVertexSlotSharingGroup != null,
                    "JobVertex slot sharing group must not be null");
            slotSharingGroups.add(jobVertexSlotSharingGroup);

            vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

            final Set<Integer> operatorIds = new HashSet<>();
            operatorIds.add(headOperatorId);
            operatorIds.addAll(
                    vertexChainedConfigs
                            .getOrDefault(headOperatorId, Collections.emptyMap())
                            .keySet());
            vertexOperators.put(jobVertex.getID(), operatorIds);
        }

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            setManagedMemoryFractionForSlotSharingGroup(
                    slotSharingGroup,
                    vertexHeadOperators,
                    vertexOperators,
                    operatorConfigs,
                    vertexChainedConfigs,
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
                    slotScopeManagedMemoryUseCasesRetriever);
        }
    }

    private static void setManagedMemoryFractionForSlotSharingGroup(
            final SlotSharingGroup slotSharingGroup,
            final Map<JobVertexID, Integer> vertexHeadOperators,
            final Map<JobVertexID, Set<Integer>> vertexOperators,
            final Map<Integer, StreamConfig> operatorConfigs,
            final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
            final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>>
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
            final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>>
                    slotScopeManagedMemoryUseCasesRetriever) {

        final Set<Integer> groupOperatorIds =
                slotSharingGroup.getJobVertexIds().stream()
                        .flatMap((vid) -> vertexOperators.get(vid).stream())
                        .collect(Collectors.toSet());

        final Map<ManagedMemoryUseCase, Integer> groupOperatorScopeUseCaseWeights =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(oid)
                                                .entrySet().stream())
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getKey,
                                        Collectors.summingInt(Map.Entry::getValue)));

        final Set<ManagedMemoryUseCase> groupSlotScopeUseCases =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        slotScopeManagedMemoryUseCasesRetriever.apply(oid).stream())
                        .collect(Collectors.toSet());

        for (JobVertexID jobVertexID : slotSharingGroup.getJobVertexIds()) {
            for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
                final StreamConfig operatorConfig = operatorConfigs.get(operatorNodeId);
                final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights =
                        operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(operatorNodeId);
                final Set<ManagedMemoryUseCase> slotScopeUseCases =
                        slotScopeManagedMemoryUseCasesRetriever.apply(operatorNodeId);
                setManagedMemoryFractionForOperator(
                        operatorScopeUseCaseWeights,
                        slotScopeUseCases,
                        groupOperatorScopeUseCaseWeights,
                        groupSlotScopeUseCases,
                        operatorConfig);
            }

            // need to refresh the chained task configs because they are serialized
            final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
            final StreamConfig vertexConfig = operatorConfigs.get(headOperatorNodeId);
            vertexConfig.setTransitiveChainedTaskConfigs(
                    vertexChainedConfigs.get(headOperatorNodeId));
        }
    }

    private static void setManagedMemoryFractionForOperator(
            final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            final Set<ManagedMemoryUseCase> slotScopeUseCases,
            final Map<ManagedMemoryUseCase, Integer> groupManagedMemoryWeights,
            final Set<ManagedMemoryUseCase> groupSlotScopeUseCases,
            final StreamConfig operatorConfig) {

        // For each operator, make sure fractions are set for all use cases in the group, even if
        // the operator does not have the use case (set the fraction to 0.0). This allows us to
        // learn which use cases exist in the group from either one of the stream configs.
        for (Map.Entry<ManagedMemoryUseCase, Integer> entry :
                groupManagedMemoryWeights.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            final int groupWeight = entry.getValue();
            final int operatorWeight = operatorScopeUseCaseWeights.getOrDefault(useCase, 0);
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase,
                    operatorWeight > 0
                            ? ManagedMemoryUtils.getFractionRoundedDown(operatorWeight, groupWeight)
                            : 0.0);
        }
        for (ManagedMemoryUseCase useCase : groupSlotScopeUseCases) {
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase, slotScopeUseCases.contains(useCase) ? 1.0 : 0.0);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 配置Checkpoint
    */
    private void configureCheckpointing() {
        // 获取流图的检查点配置
        CheckpointConfig cfg = streamGraph.getCheckpointConfig();
        // 获取检查点间隔
        long interval = cfg.getCheckpointInterval();
        // 如果检查点间隔小于最小检查点时间，则禁用检查点
        if (interval < MINIMAL_CHECKPOINT_TIME) {
            interval = CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
        }

        //  --- configure options ---

        // 配置检查点终止后的保留策略
        CheckpointRetentionPolicy retentionAfterTermination;
        // 如果启用了外部化检查点
        if (cfg.isExternalizedCheckpointsEnabled()) {
            // 获取外部化检查点的清理模式
            CheckpointConfig.ExternalizedCheckpointCleanup cleanup =
                    cfg.getExternalizedCheckpointCleanup();
            // Sanity check
            // 合理性检查：如果启用了外部化检查点但没有配置清理模式，则抛出异常
            if (cleanup == null) {
                throw new IllegalStateException(
                        "Externalized checkpoints enabled, but no cleanup mode configured.");
            }
            // 根据清理模式设置检查点终止后的保留策略
            retentionAfterTermination =
                    cleanup.deleteOnCancellation()
                            ? CheckpointRetentionPolicy.RETAIN_ON_FAILURE // 取消时删除，失败时保留
                            : CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION; // 取消时保留
        } else {
            // 如果没有启用外部化检查点，则检查点终止后从不保留
            retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
        }

        //  --- configure the master-side checkpoint hooks ---

        // 创建用于存储主节点侧检查点钩子的列表
        final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

        // 遍历流图中的每个节点
        for (StreamNode node : streamGraph.getStreamNodes()) {
            // 如果节点的操作工厂是UdfStreamOperatorFactory的实例
            if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
                // 获取用户定义的函数
                Function f =
                        ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();
                // 如果用户定义的函数实现了WithMasterCheckpointHook接口
                if (f instanceof WithMasterCheckpointHook) {
                    // 将函数封装为检查点钩子工厂并添加到列表中
                    hooks.add(
                            new FunctionMasterCheckpointHookFactory(
                                    (WithMasterCheckpointHook<?>) f));
                }
            }
        }

        // because the hooks can have user-defined code, they need to be stored as
        // eagerly serialized values
        // 定义一个序列化的MasterTriggerRestoreHook.Factory数组
        final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
        // 如果没有配置任何检查点钩子，则serializedHooks为null
        if (hooks.isEmpty()) {
            serializedHooks = null;
        } else {
            try {
                // 将钩子列表转换为数组
                MasterTriggerRestoreHook.Factory[] asArray =
                        hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
                // 将数组序列化为SerializedValue对象
                serializedHooks = new SerializedValue<>(asArray);
            } catch (IOException e) {
                // 如果序列化失败，则抛出运行时异常
                throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
            }
        }

        // because the state backend can have user-defined code, it needs to be stored as
        // eagerly serialized value
        // 因为状态后端可能包含用户定义的代码，所以需要将其存储为预先序列化的值
        final SerializedValue<StateBackend> serializedStateBackend;
        // 如果流图中没有设置状态后端，则serializedStateBackend为null
        if (streamGraph.getStateBackend() == null) {
            serializedStateBackend = null;
        } else {
            try {
                // 将状态后端序列化为SerializedValue对象
                serializedStateBackend =
                        new SerializedValue<StateBackend>(streamGraph.getStateBackend());
            } catch (IOException e) {
                // 如果序列化失败，则抛出运行时异常
                throw new FlinkRuntimeException("State backend is not serializable", e);
            }
        }

        // because the checkpoint storage can have user-defined code, it needs to be stored as
        // eagerly serialized value

        // 因为检查点存储可能包含用户定义的代码，所以需要将其存储为预先序列化的值
        final SerializedValue<CheckpointStorage> serializedCheckpointStorage;

        // 如果流图中没有设置检查点存储，则serializedCheckpointStorage为null
        if (streamGraph.getCheckpointStorage() == null) {
            serializedCheckpointStorage = null;
        } else {
            try {
                // 将检查点存储序列化为SerializedValue对象
                serializedCheckpointStorage =
                        new SerializedValue<>(streamGraph.getCheckpointStorage());
            } catch (IOException e) {
                // 如果序列化失败，则抛出运行时异常
                throw new FlinkRuntimeException("Checkpoint storage is not serializable", e);
            }
        }

        //  --- done, put it all together ---
        // 创建一个JobCheckpointingSettings对象，用于配置作业的检查点设置
        JobCheckpointingSettings settings =
                new JobCheckpointingSettings(
                        // 使用CheckpointCoordinatorConfiguration.builder()来配置检查点协调器的参数
                        CheckpointCoordinatorConfiguration.builder()
                                .setCheckpointInterval(interval)  // 设置检查点之间的时间间隔
                                // 设置在backlog期间检查点的时间间隔
                                .setCheckpointIntervalDuringBacklog(
                                        cfg.getCheckpointIntervalDuringBacklog())
                                .setCheckpointTimeout(cfg.getCheckpointTimeout())// 设置检查点超时时间
                                .setMinPauseBetweenCheckpoints(cfg.getMinPauseBetweenCheckpoints()) // 设置两次检查点之间的最小暂停时间
                                .setMaxConcurrentCheckpoints(cfg.getMaxConcurrentCheckpoints())// 设置同时运行的最大检查点数量
                                .setCheckpointRetentionPolicy(retentionAfterTermination) // 设置作业终止后检查点的保留策略
                                .setExactlyOnce(
                                        getCheckpointingMode(cfg) == CheckpointingMode.EXACTLY_ONCE) // 设置作业是否以恰好一次（EXACTLY_ONCE）模式执行
                                .setTolerableCheckpointFailureNumber(
                                        cfg.getTolerableCheckpointFailureNumber())  // 设置可以容忍的检查点失败次数
                                .setUnalignedCheckpointsEnabled(cfg.isUnalignedCheckpointsEnabled())  // 设置是否启用非对齐检查点
                                .setCheckpointIdOfIgnoredInFlightData(
                                        cfg.getCheckpointIdOfIgnoredInFlightData())// 设置被忽略的正在传输的数据的检查点ID
                                .setAlignedCheckpointTimeout(
                                        cfg.getAlignedCheckpointTimeout().toMillis()) // 设置对齐检查点的超时时间（毫秒）
                                .setEnableCheckpointsAfterTasksFinish(
                                        streamGraph.isEnableCheckpointsAfterTasksFinish())// 设置在任务完成后是否启用检查点
                                .build(),
                        serializedStateBackend,// 序列化的状态后端对象
                        // 检查流图中是否启用了状态变更日志（state changelog）
                        streamGraph
                                .getJobConfiguration()
                                .getOptional(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
                                .map(TernaryBoolean::fromBoolean)
                                .orElse(TernaryBoolean.UNDEFINED),
                        serializedCheckpointStorage,
                        serializedHooks);

        // 将配置好的检查点设置应用到作业图（JobGraph）上
        jobGraph.setSnapshotSettings(settings);
    }

    private static String nameWithChainedSourcesInfo(
            String operatorName, Collection<ChainedSourceInfo> chainedSourceInfos) {
        return chainedSourceInfos.isEmpty()
                ? operatorName
                : String.format(
                        "%s [%s]",
                        operatorName,
                        chainedSourceInfos.stream()
                                .map(
                                        chainedSourceInfo ->
                                                chainedSourceInfo
                                                        .getOperatorConfig()
                                                        .getOperatorName())
                                .collect(Collectors.joining(", ")));
    }

    /**
     * A private class to help maintain the information of an operator chain during the recursive
     * call in {@link #createChain(Integer, int, OperatorChainInfo, Map)}.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 私有类createChain方法中用于维护Operator链的信息
    */
    private static class OperatorChainInfo {
        /**
         * 开始的StreamNodeId
         * 当两个算子可以合并成为要给算子的时候startNodeId 就是开始的，最小StreamNodeId
         * 如果算子不可以合并startNodeId永远当前迭代的StreamNodeId
         * startNodeId 用于后面生成JobVertext
         */
        private final Integer startNodeId;
        /**
         * 节点Hash唯一值
         */
        private final Map<Integer, byte[]> hashes;
        /**
         * 为了兼容历史版本节点Hash唯一值
         */
        private final List<Map<Integer, byte[]>> legacyHashes;
        private final Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes;
        private final Map<Integer, ChainedSourceInfo> chainedSources;
        private final List<OperatorCoordinator.Provider> coordinatorProviders;
        /**
         * StreamGraph
         */
        private final StreamGraph streamGraph;
        private final List<StreamNode> chainedNodes;
        /**
         * 记录真实物理的边
         */
        private final List<StreamEdge> transitiveOutEdges;

        private OperatorChainInfo(
                int startNodeId,
                Map<Integer, byte[]> hashes,
                List<Map<Integer, byte[]>> legacyHashes,
                Map<Integer, ChainedSourceInfo> chainedSources,
                StreamGraph streamGraph) {
            this.startNodeId = startNodeId;
            this.hashes = hashes;
            this.legacyHashes = legacyHashes;
            this.chainedOperatorHashes = new HashMap<>();
            this.coordinatorProviders = new ArrayList<>();
            this.chainedSources = chainedSources;
            this.streamGraph = streamGraph;
            this.chainedNodes = new ArrayList<>();
            this.transitiveOutEdges = new ArrayList<>();
        }

        byte[] getHash(Integer streamNodeId) {
            return hashes.get(streamNodeId);
        }

        private Integer getStartNodeId() {
            return startNodeId;
        }

        private List<Tuple2<byte[], byte[]>> getChainedOperatorHashes(int startNodeId) {
            return chainedOperatorHashes.get(startNodeId);
        }

        void addCoordinatorProvider(OperatorCoordinator.Provider coordinator) {
            coordinatorProviders.add(coordinator);
        }

        private List<OperatorCoordinator.Provider> getCoordinatorProviders() {
            return coordinatorProviders;
        }

        Map<Integer, ChainedSourceInfo> getChainedSources() {
            return chainedSources;
        }

        private OperatorID addNodeToChain(int currentNodeId, String operatorName) {
            recordChainedNode(currentNodeId);
            StreamNode streamNode = streamGraph.getStreamNode(currentNodeId);

            List<Tuple2<byte[], byte[]>> operatorHashes =
                    chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

            byte[] primaryHashBytes = hashes.get(currentNodeId);

            for (Map<Integer, byte[]> legacyHash : legacyHashes) {
                operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
            }

            streamNode
                    .getCoordinatorProvider(operatorName, new OperatorID(getHash(currentNodeId)))
                    .map(coordinatorProviders::add);

            return new OperatorID(primaryHashBytes);
        }

        private void setTransitiveOutEdges(final List<StreamEdge> transitiveOutEdges) {
            this.transitiveOutEdges.addAll(transitiveOutEdges);
        }

        private List<StreamEdge> getTransitiveOutEdges() {
            return transitiveOutEdges;
        }

        private void recordChainedNode(int currentNodeId) {
            StreamNode streamNode = streamGraph.getStreamNode(currentNodeId);
            chainedNodes.add(streamNode);
        }

        private OperatorChainInfo newChain(Integer startNodeId) {
            return new OperatorChainInfo(
                    startNodeId, hashes, legacyHashes, chainedSources, streamGraph);
        }

        private List<StreamNode> getAllChainedNodes() {
            return chainedNodes;
        }
    }

    private static final class ChainedSourceInfo {
        private final StreamConfig operatorConfig;
        private final StreamConfig.SourceInputConfig inputConfig;

        ChainedSourceInfo(StreamConfig operatorConfig, StreamConfig.SourceInputConfig inputConfig) {
            this.operatorConfig = operatorConfig;
            this.inputConfig = inputConfig;
        }

        public StreamConfig getOperatorConfig() {
            return operatorConfig;
        }

        public StreamConfig.SourceInputConfig getInputConfig() {
            return inputConfig;
        }
    }
}
