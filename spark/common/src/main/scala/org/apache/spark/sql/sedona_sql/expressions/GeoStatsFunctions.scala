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

import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.stats.Weighting.{addBinaryDistanceBandColumn, addWeightedDistanceBandColumn}
import org.apache.sedona.stats.clustering.DBSCAN.dbscan
import org.apache.sedona.stats.hotspotDetection.GetisOrd.gLocal
import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor.localOutlierFactor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ImplicitCastInputTypes, Literal, ScalarSubquery, Unevaluable}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.ClassTag

// We mark ST_GeoStatsFunction as non-deterministic to avoid the filter push-down optimization pass
// duplicates the ST_GeoStatsFunction when pushing down aliased ST_GeoStatsFunction through a
// Project operator. This will make ST_GeoStatsFunction being evaluated twice.
trait ST_GeoStatsFunction
    extends Expression
    with ImplicitCastInputTypes
    with Unevaluable
    with Serializable {

  final override lazy val deterministic: Boolean = false

  override def nullable: Boolean = true

  private final lazy val sparkSession = SparkSession.getActiveSession.get

  protected final lazy val geometryColumnName = getInputName(0, "geometry")

  protected def getInputName(i: Int, fieldName: String): String = children(i) match {
    case ref: AttributeReference => ref.name
    case _ =>
      throw new IllegalArgumentException(
        f"$fieldName argument must be a named reference to an existing column")
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

  protected def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame

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

  def execute(plan: SparkPlan, resultAttrs: Seq[Attribute]): RDD[InternalRow] = {
    val df = doExecute(
      Dataset.ofRows(sparkSession, LogicalRDD(plan.output, plan.execute())(sparkSession)),
      resultAttrs)
    df.queryExecution.toRdd
  }

}

case class ST_DBSCAN(children: Seq[Expression]) extends ST_GeoStatsFunction {

  override def dataType: DataType = StructType(
    Seq(StructField("isCore", BooleanType), StructField("cluster", LongType)))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT, DoubleType, IntegerType, BooleanType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame = {
    require(
      !dataframe.columns.contains("__isCore"),
      "__isCore is a  reserved name by the dbscan algorithm. Please rename the columns before calling the ST_DBSCAN function.")
    require(
      !dataframe.columns.contains("__cluster"),
      "__cluster is a  reserved name by the dbscan algorithm. Please rename the columns before calling the ST_DBSCAN function.")

    dbscan(
      dataframe,
      getScalarValue[Double](1, "epsilon"),
      getScalarValue[Int](2, "minPts"),
      geometryColumnName,
      SedonaConf.fromActiveSession().getDBSCANIncludeOutliers,
      getScalarValue[Boolean](3, "useSpheroid"),
      "__isCore",
      "__cluster")
      .withColumn(getResultName(resultAttrs), struct(col("__isCore"), col("__cluster")))
      .drop("__isCore", "__cluster")
  }
}

case class ST_LocalOutlierFactor(children: Seq[Expression]) extends ST_GeoStatsFunction {

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT, IntegerType, BooleanType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame = {
    localOutlierFactor(
      dataframe,
      getScalarValue[Int](1, "k"),
      geometryColumnName,
      SedonaConf.fromActiveSession().isIncludeTieBreakersInKNNJoins,
      getScalarValue[Boolean](2, "useSphere"),
      getResultName(resultAttrs))
  }
}

case class ST_GLocal(children: Seq[Expression]) extends ST_GeoStatsFunction {

  override def dataType: DataType = StructType(
    Seq(
      StructField("G", DoubleType),
      StructField("EG", DoubleType),
      StructField("VG", DoubleType),
      StructField("Z", DoubleType),
      StructField("P", DoubleType)))

  override def inputTypes: Seq[AbstractDataType] = {
    val xDataType = children(0).dataType
    require(xDataType == DoubleType || xDataType == IntegerType, "x must be a numeric value")
    Seq(
      xDataType,
      children(1).dataType, // Array of the weights
      BooleanType)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame = {
    gLocal(
      dataframe,
      getInputName(0, "x"),
      getInputName(1, "weights"),
      0,
      getScalarValue[Boolean](2, "star"),
      0.0)
      .withColumn(
        getResultName(resultAttrs),
        struct(col("G"), col("EG"), col("VG"), col("Z"), col("P")))
      .drop("G", "EG", "VG", "Z", "P")
  }
}

case class ST_BinaryDistanceBandColumn(children: Seq[Expression]) extends ST_GeoStatsFunction {
  override def dataType: DataType = ArrayType(
    StructType(
      Seq(StructField("neighbor", children(5).dataType), StructField("value", DoubleType))))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT, DoubleType, BooleanType, BooleanType, BooleanType, children(5).dataType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame = {
    val attributeNames = getInputNames(5, "attributes")
    require(attributeNames.nonEmpty, "attributes must have at least one column")
    require(
      attributeNames.contains(geometryColumnName),
      "attributes must contain the geometry column")

    addBinaryDistanceBandColumn(
      dataframe,
      getScalarValue[Double](1, "threshold"),
      getScalarValue[Boolean](2, "includeZeroDistanceNeighbors"),
      getScalarValue[Boolean](3, "includeSelf"),
      geometryColumnName,
      getScalarValue[Boolean](4, "useSpheroid"),
      attributeNames,
      getResultName(resultAttrs))
  }
}

case class ST_WeightedDistanceBandColumn(children: Seq[Expression]) extends ST_GeoStatsFunction {

  override def dataType: DataType = ArrayType(
    StructType(
      Seq(StructField("neighbor", children(7).dataType), StructField("value", DoubleType))))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      GeometryUDT,
      DoubleType,
      DoubleType,
      BooleanType,
      BooleanType,
      DoubleType,
      BooleanType,
      children(7).dataType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def doExecute(dataframe: DataFrame, resultAttrs: Seq[Attribute]): DataFrame = {
    val attributeNames = getInputNames(7, "attributes")
    require(attributeNames.nonEmpty, "attributes must have at least one column")
    require(
      attributeNames.contains(geometryColumnName),
      "attributes must contain the geometry column")

    addWeightedDistanceBandColumn(
      dataframe,
      getScalarValue[Double](1, "threshold"),
      getScalarValue[Double](2, "alpha"),
      getScalarValue[Boolean](3, "includeZeroDistanceNeighbors"),
      getScalarValue[Boolean](4, "includeSelf"),
      getScalarValue[Double](5, "selfWeight"),
      geometryColumnName,
      getScalarValue[Boolean](6, "useSpheroid"),
      attributeNames,
      getResultName(resultAttrs))
  }
}
