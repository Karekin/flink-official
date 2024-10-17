package com.flink.lineage

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.{RelColumnOrigin, RelMetadataQuery}
import org.apache.commons.collections.CollectionUtils
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.operations.{Operation, SinkModifyOperation}
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.schema.TableSourceTable

import java.util

object LineageDemo {
  def main(args: Array[String]): Unit = {
    // 设置执行环境
    val env = StreamExecutionEnvironment.createLocalEnvironment
    // 创建 TableEnvironment// 创建 TableEnvironment
    val context = StreamTableEnvironment.create(env)
    context.executeSql("CREATE TABLE datagen (\n" + "        f_sequence1 INT,\n" + "        f_random1 INT,\n" + "        f_random_str1 STRING,\n" + "        ts AS localtimestamp,\n" + "        WATERMARK FOR ts AS ts\n" + "    ) WITH (\n" + "        'connector' = 'datagen',\n" + "        'rows-per-second'='5',\n" + "        'fields.f_sequence1.kind'='sequence',\n" + "        'fields.f_sequence1.start'='1',\n" + "        'fields.f_sequence1.end'='500',\n" + "        'fields.f_random1.min'='1',\n" + "        'fields.f_random1.max'='500',\n" + "        'fields.f_random_str1.length'='10'\n" + "        )")
    context.executeSql("CREATE TABLE print_table (\n" + "        f_sequence INT,\n" + "        f_random INT,\n" + "        f_random_str STRING\n" + "    ) WITH (\n" + "        'connector' = 'print'\n" + "        )")
    val sql = "INSERT INTO print_table select t1.f_sequence1,t1.f_random1,t2.f_random_str1 from datagen t1 " + "left join datagen t2 on t1.f_sequence1=t2.f_sequence1"
    val tableEnv: StreamTableEnvironmentImpl = context.asInstanceOf[StreamTableEnvironmentImpl]
    val parsed  = parseStatement(tableEnv, sql)
    val sinkTable = parsed._1
    val oriRelNode = parsed._2
    buildFiledLineageResult(context, sinkTable, oriRelNode)
  }

  private def parseStatement(tableEnv: StreamTableEnvironmentImpl, sql: String): (String, RelNode) = {
    val operations: util.List[Operation] = tableEnv.getParser.parse(sql)
    if (operations.size != 1) throw new TableException("Unsupported SQL query! only accepts a single SQL statement.")
    val operation: Operation = operations.get(0)
    val sinkOperation: SinkModifyOperation = operation.asInstanceOf[SinkModifyOperation]
    val queryOperation: PlannerQueryOperation = sinkOperation.getChild.asInstanceOf[PlannerQueryOperation]
    val relNode: RelNode = queryOperation.getCalciteTree
    (sinkOperation.getContextResolvedTable.getIdentifier.asSummaryString, relNode)
  }

  private def buildFiledLineageResult(tableEnv: StreamTableEnvironment, sinkTable: String, optRelNode: RelNode) = {
    // target columns
    val targetColumnList = tableEnv.from(sinkTable).getResolvedSchema.getColumnNames
    // check the size of query and sink fields match
    val metadataQuery = optRelNode.getCluster.getMetadataQuery
    val resultList = new util.ArrayList[Nothing]
    for (index <- 0 until targetColumnList.size) {
      val targetColumn = targetColumnList.get(index)
      val relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index)
      if (CollectionUtils.isNotEmpty(relColumnOriginSet)) {
        import scala.collection.JavaConversions._
        for (relColumnOrigin <- relColumnOriginSet) {
          // table
          val table = relColumnOrigin.getOriginTable
          val sourceTable = String.join(".", table.getQualifiedName)
          // filed
          val ordinal = relColumnOrigin.getOriginColumnOrdinal
          val fieldNames = table.asInstanceOf[TableSourceTable].contextResolvedTable.getResolvedSchema.getColumnNames
          val sourceColumn = fieldNames.get(ordinal)
          // add record
          println("sourceTable:" + sourceTable + ", sourceColumn:" + sourceColumn + ", sinkTable:" + sinkTable + ", targetColumn:" + targetColumn)
        }
      }
    }
  }
}
