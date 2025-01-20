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
package org.apache.flink.table.planner.plan.`trait`

import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.RowKind

import org.apache.calcite.plan.{RelOptPlanner, RelTrait, RelTraitDef}

import scala.collection.JavaConversions._

/**
 * ModifyKindSetTrait用于描述此节点将产生的修改操作类型。
 * 该特性表示节点可能会执行的修改操作，比如插入、更新或删除。
 */
class ModifyKindSetTrait(val modifyKindSet: ModifyKindSet) extends RelTrait {

  /**
   * 判断当前特性是否满足给定的特性要求。
   * 当当前的修改操作集合包含在要求的修改操作集合中时，返回true。
   * 例如：[I, U] 满足 [I, U, D]，但 [I, U, D] 不满足 [I, D]。
   */
  override def satisfies(relTrait: RelTrait): Boolean = relTrait match {
    case other: ModifyKindSetTrait =>
      // 当当前的修改操作集合包含在目标修改操作集合中时返回true
      this.modifyKindSet.getContainedKinds.forall(other.modifyKindSet.contains)
    case _ => false
  }

  /**
   * 获取当前特性定义。
   * 这里返回的是ModifyKindSetTrait的定义实例。
   */
  override def getTraitDef: RelTraitDef[_ <: RelTrait] = ModifyKindSetTraitDef.INSTANCE

  /**
   * 注册当前特性到给定的优化器规划器中。
   * 目前该方法为空，表示没有具体的注册行为。
   */
  override def register(planner: RelOptPlanner): Unit = {}

  /**
   * 重写hashCode方法，用于基于修改操作集合的哈希值计算当前对象的哈希值。
   */
  override def hashCode(): Int = modifyKindSet.hashCode()

  /**
   * 重写equals方法，用于比较当前特性是否等于给定的对象。
   * 比较的是修改操作集合是否相等。
   */
  override def equals(obj: Any): Boolean = obj match {
    case t: ModifyKindSetTrait => this.modifyKindSet.equals(t.modifyKindSet)
    case _ => false
  }

  /**
   * 重写toString方法，返回修改操作集合的字符串表示。
   */
  override def toString: String = s"[${modifyKindSet.toString}]"
}

object ModifyKindSetTrait {

  /**
   * 一个空的[[ModifyKindSetTrait]]，不包含任何[[ModifyKind]]。
   * 这意味着该特性表示的操作节点不涉及任何修改操作，如插入、删除或更新。
   */
  val EMPTY = new ModifyKindSetTrait(ModifyKindSet.newBuilder().build())

  /**
   * 仅包含插入操作的[[ModifyKindSetTrait]]。
   * 这表明该特性描述的操作节点只产生插入操作，不涉及删除或更新。
   */
  val INSERT_ONLY = new ModifyKindSetTrait(ModifyKindSet.INSERT_ONLY)

  /**
   * 包含所有变更操作的[[ModifyKindSetTrait]]。
   * 这意味着该特性描述的操作节点可能产生插入、删除或更新中的任何一种或多种操作。
   */
  val ALL_CHANGES = new ModifyKindSetTrait(ModifyKindSet.ALL_CHANGES)

  /**
   * 从给定的[[ChangelogMode]]创建一个[[ModifyKindSetTrait]]实例。
   * 根据ChangelogMode中的变更类型，构建相应的修改操作集合。
   */
  def fromChangelogMode(changelogMode: ChangelogMode): ModifyKindSetTrait = {
    val builder = ModifyKindSet.newBuilder
    changelogMode.getContainedKinds.foreach {
      case RowKind.INSERT => builder.addContainedKind(ModifyKind.INSERT) // 插入操作
      case RowKind.DELETE => builder.addContainedKind(ModifyKind.DELETE) // 删除操作
      case _ => builder.addContainedKind(ModifyKind.UPDATE) // 否则，视为更新操作
    }
    new ModifyKindSetTrait(builder.build) // 使用构建器创建ModifyKindSetTrait实例
  }
}
