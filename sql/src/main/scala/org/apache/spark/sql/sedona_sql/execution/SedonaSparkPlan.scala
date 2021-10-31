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