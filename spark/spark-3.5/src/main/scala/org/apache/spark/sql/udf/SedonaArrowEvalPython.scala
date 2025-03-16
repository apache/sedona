package org.apache.spark.sql.udf

import org.apache.spark.sql.catalyst.expressions.{Attribute, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{BaseEvalPython, LogicalPlan}

case class SedonaArrowEvalPython(
                                  udfs: Seq[PythonUDF],
                                  resultAttrs: Seq[Attribute],
                                  child: LogicalPlan,
                                  evalType: Int)
  extends BaseEvalPython {
  override protected def withNewChildInternal(newChild: LogicalPlan): SedonaArrowEvalPython =
    copy(child = newChild)
}

