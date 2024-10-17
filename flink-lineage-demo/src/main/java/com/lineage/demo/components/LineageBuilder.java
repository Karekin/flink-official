package com.lineage.demo.components;

import org.apache.calcite.rel.RelNode;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import java.util.List;
import java.util.Set;

public class LineageBuilder {

    private final LineageTopologyBuilder topologyBuilder;

    public LineageBuilder(LineageTopologyBuilder topologyBuilder) {
        this.topologyBuilder = topologyBuilder;
    }

    public static Tuple2<String, RelNode> parseStatement(StreamTableEnvironmentImpl tableEnv, String sql) {
        List<Operation> operations = tableEnv.getParser().parse(sql);
        if (operations.size() != 1) {
            throw new TableException("Unsupported SQL query! Only accepts a single SQL statement.");
        }
        Operation operation = operations.get(0);
        SinkModifyOperation sinkOperation = (SinkModifyOperation) operation;
        PlannerQueryOperation queryOperation = (PlannerQueryOperation) sinkOperation.getChild();
        RelNode relNode = queryOperation.getCalciteTree();
        return new Tuple2<>(sinkOperation.getContextResolvedTable().getIdentifier().asSummaryString(), relNode);
    }

    // 构建字段级别的血缘关系并填充到拓扑结构中
    public void buildFiledLineageResult(StreamTableEnvironment tableEnv, String sinkTable, RelNode optRelNode) {
        ResolvedSchema schema = tableEnv.from(sinkTable).getResolvedSchema();
        List<String> targetColumnList = schema.getColumnNames();
        RelMetadataQuery metadataQuery = optRelNode.getCluster().getMetadataQuery();

        for (int index = 0; index < targetColumnList.size(); index++) {
            String targetColumn = targetColumnList.get(index);
            Set<RelColumnOrigin> relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index);
            if (relColumnOriginSet != null && !relColumnOriginSet.isEmpty()) {
                for (RelColumnOrigin relColumnOrigin : relColumnOriginSet) {
                    RelOptTable table = relColumnOrigin.getOriginTable();
                    String sourceTable = String.join(".", table.getQualifiedName());
                    int ordinal = relColumnOrigin.getOriginColumnOrdinal();
                    List<String> fieldNames = ((TableSourceTable) table).contextResolvedTable().getResolvedSchema().getColumnNames();
                    String sourceColumn = fieldNames.get(ordinal);

                    // 构建拓扑中的节点和边
                    LineageTopologyBuilder.Node sourceNode = new LineageTopologyBuilder.Node(sourceTable, sourceColumn);
                    LineageTopologyBuilder.Node targetNode = new LineageTopologyBuilder.Node(sinkTable, targetColumn);
                    topologyBuilder.addEdge(sourceNode, targetNode);

                    // 输出血缘信息（可以根据需求可视化或存储）
                    System.out.println("sourceTable: " + sourceTable + ", sourceColumn: " + sourceColumn + ", sinkTable: " + sinkTable + ", targetColumn: " + targetColumn);
                }
            }
        }
    }
}


