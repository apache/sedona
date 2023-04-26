/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.execution

import org.apache.spark.sql.execution.SparkPlan

trait SedonaUnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil

  final def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    assert(newChildren.size == 1, "Incorrect number of children")
    withNewChildInternal(newChildren.head)
  }

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan
}

trait SedonaBinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)

  final def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    assert(newChildren.size == 2, "Incorrect number of children")
    withNewChildrenInternal(newChildren(0), newChildren(1))
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan
}