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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.sedona_sql.DataFrameShims
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._

private[apache] case class ST_DBSCAN(children: Seq[Expression])
    extends DataframePhysicalFunction {

  private lazy val materializedGeometryName =
    "__sedona_dbscan_geometry_" + java.util.UUID.randomUUID().toString.replace("-", "")

  override protected def getInputName(i: Int, fieldName: String): String = {
    if (i == 0 && !children.head.isInstanceOf[AttributeReference]) materializedGeometryName
    else super.getInputName(i, fieldName)
  }

  override def execute(plan: SparkPlan, resultAttrs: Seq[Attribute]): RDD[InternalRow] = {
    children.head match {
      case _: AttributeReference => super.execute(plan, resultAttrs)
      case geometry =>
        // Materialize a named geometry after optimization can inline its expression.
        val projected = ProjectExec(plan.output :+ Alias(geometry, geometryColumnName)(), plan)
        transformDataframe(
          DataFrameShims.createDataFrame(sparkSession, projected.execute(), projected.schema),
          resultAttrs)
          .drop(geometryColumnName)
          .queryExecution
          .toRdd
    }
  }

  override def dataType: DataType =
    StructType(Seq(StructField("isCore", BooleanType), StructField("cluster", LongType)))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT(), DoubleType, IntegerType, BooleanType) ++
      (if (children.length > 4) Seq(BooleanType) else Seq.empty)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def transformDataframe(
      dataframe: DataFrame,
      resultAttrs: Seq[Attribute]): DataFrame = {
    val isCoreColumnName = materializedGeometryName + "_isCore"
    val clusterColumnName = materializedGeometryName + "_cluster"
    val includeOutliers =
      if (children.length > 4) getScalarValue[Boolean](4, "includeOutliers")
      else SedonaConf.fromActiveSession().getDBSCANIncludeOutliers

    val ret = dbscan(
      dataframe,
      getScalarValue[Double](1, "epsilon"),
      getScalarValue[Int](2, "minPts"),
      geometryColumnName,
      includeOutliers,
      getScalarValue[Boolean](3, "useSpheroid"),
      isCoreColumnName,
      clusterColumnName)
      .withColumn(
        getResultName(resultAttrs),
        struct(col(isCoreColumnName), col(clusterColumnName)))
      .drop(isCoreColumnName, clusterColumnName)

    ret

  }
}

private[apache] case class ST_LocalOutlierFactor(children: Seq[Expression])
    extends DataframePhysicalFunction {

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT(), IntegerType, BooleanType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def transformDataframe(
      dataframe: DataFrame,
      resultAttrs: Seq[Attribute]): DataFrame = {
    localOutlierFactor(
      dataframe,
      getScalarValue[Int](1, "k"),
      geometryColumnName,
      SedonaConf.fromActiveSession().isIncludeTieBreakersInKNNJoins,
      getScalarValue[Boolean](2, "useSphere"),
      getResultName(resultAttrs))
  }
}

private[apache] case class ST_GLocal(children: Seq[Expression])
    extends DataframePhysicalFunction {

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

  override def transformDataframe(
      dataframe: DataFrame,
      resultAttrs: Seq[Attribute]): DataFrame = {
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

private[apache] case class ST_BinaryDistanceBandColumn(children: Seq[Expression])
    extends DataframePhysicalFunction {
  override def dataType: DataType = ArrayType(
    StructType(
      Seq(StructField("neighbor", children(5).dataType), StructField("value", DoubleType))))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(GeometryUDT(), DoubleType, BooleanType, BooleanType, BooleanType, children(5).dataType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def transformDataframe(
      dataframe: DataFrame,
      resultAttrs: Seq[Attribute]): DataFrame = {
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

private[apache] case class ST_WeightedDistanceBandColumn(children: Seq[Expression])
    extends DataframePhysicalFunction {

  override def dataType: DataType = ArrayType(
    StructType(
      Seq(StructField("neighbor", children(7).dataType), StructField("value", DoubleType))))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      GeometryUDT(),
      DoubleType,
      DoubleType,
      BooleanType,
      BooleanType,
      DoubleType,
      BooleanType,
      children(7).dataType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def transformDataframe(
      dataframe: DataFrame,
      resultAttrs: Seq[Attribute]): DataFrame = {
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
