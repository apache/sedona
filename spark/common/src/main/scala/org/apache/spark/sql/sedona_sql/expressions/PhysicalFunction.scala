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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ImplicitCastInputTypes, Literal, ScalarSubquery, Unevaluable}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import org.apache.spark.sql.sedona_sql.DataFrameShims

import scala.reflect.ClassTag

/**
 * PhysicalFunctions are Functions that will be replaced with a Physical Node for their
 * evaluation.
 *
 * execute is the method that will be called in order to evaluate the function. PhysicalFunction
 * is marked non-deterministic to avoid the filter push-down optimization pass which duplicates
 * the PhysicalFunction when pushing down aliased PhysicalFunction calls through a Project
 * operator. Otherwise the PhysicalFunction would be evaluated twice.
 */
trait PhysicalFunction
    extends Expression
    with ImplicitCastInputTypes
    with Unevaluable
    with Serializable {
  final override lazy val deterministic: Boolean = false

  override def nullable: Boolean = true

  protected final lazy val sparkSession = SparkSession.getActiveSession.get

  protected final lazy val geometryColumnName = getInputName(0, "geometry")

  protected def getInputName(i: Int, fieldName: String): String = children(i) match {
    case ref: AttributeReference => ref.name
    case _ =>
      throw new IllegalArgumentException(
        f"$fieldName argument must be a named reference to an existing column")
  }

  protected def getScalarValue[T](i: Int, name: String)(implicit ct: ClassTag[T]): T = {
    children(i) match {
      case Literal(l: T, _) => l
      case _: Literal =>
        throw new IllegalArgumentException(f"$name must be an instance of  ${ct.runtimeClass}")
      case s: ScalarSubquery =>
        s.eval() match {
          case t: T => t
          case _ =>
            throw new IllegalArgumentException(
              f"$name must be an instance of  ${ct.runtimeClass}")
        }
      case _ => throw new IllegalArgumentException(f"$name must be a scalar value")
    }
  }

  protected def getInputNames(i: Int, fieldName: String): Seq[String] = children(
    i).dataType match {
    case StructType(fields) => fields.map(_.name)
    case _ => throw new IllegalArgumentException(f"$fieldName argument must be a struct")
  }

  protected def getResultName(resultAttrs: Seq[Attribute]): String = resultAttrs match {
    case Seq(attr) => attr.name
    case _ => throw new IllegalArgumentException("resultAttrs must have exactly one attribute")
  }

  def execute(plan: SparkPlan, resultAttrs: Seq[Attribute]): RDD[InternalRow]
}

/**
 * DataframePhysicalFunctions are Functions that will be replaced with a Physical Node for their
 * evaluation.
 *
 * The physical node will transform the input dataframe into the output dataframe. execute handles
 * conversion of the RDD[InternalRow] to a DataFrame and back. Each DataframePhysicalFunction
 * should implement transformDataframe. The output dataframe should have the same schema as the
 * input dataframe, except for the resultAttrs which should be added to the output dataframe.
 */
trait DataframePhysicalFunction extends PhysicalFunction {

  protected def transformDataframe(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame

  override def execute(plan: SparkPlan, resultAttrs: Seq[Attribute]): RDD[InternalRow] = {
    val df = transformDataframe(
      DataFrameShims.createDataFrame(sparkSession, plan.execute(), plan.schema),
      resultAttrs)
    df.queryExecution.toRdd
  }

}
