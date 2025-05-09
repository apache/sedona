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
package org.apache.sedona.sql

import org.apache.sedona.stats.Weighting.{addBinaryDistanceBandColumn, addWeightedDistanceBandColumn}
import org.apache.sedona.stats.clustering.DBSCAN.dbscan
import org.apache.sedona.stats.hotspotDetection.GetisOrd.gLocal
import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor.localOutlierFactor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_DBSCAN, ST_LocalOutlierFactor}

class GeoStatsSuite extends TestBaseScala {
  private val spark = sparkSession

  case class Record(id: Int, x: Double, y: Double)

  def getData: DataFrame = {
    spark
      .createDataFrame(
        Seq(
          Record(10, 1.0, 1.8),
          Record(11, 1.0, 1.9),
          Record(12, 1.0, 2.0),
          Record(13, 1.0, 2.1),
          Record(14, 2.0, 2.0),
          Record(15, 3.0, 1.9),
          Record(16, 3.0, 2.0),
          Record(17, 3.0, 2.1),
          Record(18, 3.0, 2.2)))
      .withColumn("geometry", expr("ST_Point(x, y)"))
  }

  it("test dbscan function") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    dbscan(getData.withColumn("sql_results", expr("ST_DBSCAN(geometry, 1.0, 4, false)")), 1.0, 4)
      .where("sql_results.cluster = cluster and sql_results.isCore = isCore")
      .count() == getData.count()
  }

  it("test dbscan function df method") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    dbscan(
      getData.withColumn("sql_results", ST_DBSCAN(col("geometry"), lit(1.0), lit(4), lit(false))),
      1.0,
      4)
      .where("sql_results.cluster = cluster and sql_results.isCore = isCore")
      .count() == getData.count()
  }

  it("test dbscan function with distance column") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    dbscan(
      getData.withColumn("sql_results", expr("ST_DBSCAN(geometry, 1.0, 4, true)")),
      1.0,
      4,
      useSpheroid = true)
      .where("sql_results.cluster = cluster and sql_results.isCore = isCore")
      .count() == getData.count()
  }

  it("test dbscan function with scalar subquery") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    dbscan(
      getData.withColumn(
        "sql_results",
        expr("ST_DBSCAN(geometry, (SELECT ARRAY(1.0, 2.0)[0]), 4, false)")),
      1.0,
      4)
      .where("sql_results.cluster = cluster and sql_results.isCore = isCore")
      .count() == getData.count()
  }

  it("test dbscan with geom literal") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    val error = intercept[IllegalArgumentException] {
      spark.sql("SELECT ST_DBSCAN(ST_GeomFromWKT('POINT(0.0 1.1)'), 1.0, 4, false)").collect()
    }
    assert(
      error
        .asInstanceOf[IllegalArgumentException]
        .getMessage == "geometry argument must be a named reference to an existing column")
  }

  it("test dbscan with minPts variable") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    val error = intercept[IllegalArgumentException] {
      getData
        .withColumn("result", ST_DBSCAN(col("geometry"), lit(1.0), col("id"), lit(false)))
        .collect()
    }

    assert(
      error
        .asInstanceOf[IllegalArgumentException]
        .getMessage
        .contains("minPts must be a scalar value"))
  }

  it("test lof") {
    localOutlierFactor(
      getData.withColumn("sql_result", expr("ST_LocalOutlierFactor(geometry, 4, false)")),
      4)
      .where("sql_result = lof")
      .count() == getData.count()
  }

  it("test lof with dataframe method") {
    localOutlierFactor(
      getData.withColumn(
        "sql_result",
        ST_LocalOutlierFactor(col("geometry"), lit(4), lit(false))),
      4)
      .where("sql_result = lof")
      .count() == getData.count()
  }

  it("test geostats function in another function") {
    getData
      .withColumn("sql_result", expr("SQRT(ST_LocalOutlierFactor(geometry, 4, false))"))
      .collect()
  }

  it("test DBSCAN with a column named __isCore in input df") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    val exception = intercept[IllegalArgumentException] {
      getData
        .withColumn("__isCore", lit(1))
        .withColumn("sql_result", expr("ST_DBSCAN(geometry, 0.1, 4, false)"))
        .collect()
    }
    assert(
      exception.getMessage == "requirement failed: __isCore is a  reserved name by the dbscan algorithm. Please rename the columns before calling the ST_DBSCAN function.")
  }

  it("test ST_BinaryDistanceBandColumn") {
    val weightedDf = getData
      .withColumn(
        "someWeights",
        expr(
          "array_sort(ST_BinaryDistanceBandColumn(geometry, 1.0, true, true, false, struct(id, geometry)))"))

    val resultsDf = addBinaryDistanceBandColumn(
      weightedDf,
      1.0,
      true,
      true,
      savedAttributes = Seq("id", "geometry"))
      .withColumn("weights", expr("array_sort(weights)"))
      .where("someWeights = weights")

    assert(resultsDf.count == weightedDf.count())
  }

  it("test ST_WeightedDistanceBandColumn") {
    val weightedDf = getData
      .withColumn(
        "someWeights",
        expr(
          "array_sort(ST_WeightedDistanceBandColumn(geometry, 1.0, -1.0, true, true, 1.0, false, struct(id, geometry)))"))

    val resultsDf = addWeightedDistanceBandColumn(
      weightedDf,
      1.0,
      -1.0,
      true,
      true,
      savedAttributes = Seq("id", "geometry"),
      selfWeight = 1.0)
      .withColumn("weights", expr("array_sort(weights)"))
      .where("someWeights = weights")

    assert(resultsDf.count == weightedDf.count())
  }

  it("test GI with ST_BinaryDistanceBandColumn") {
    val weightedDf = getData
      .withColumn(
        "someWeights",
        expr(
          "ST_BinaryDistanceBandColumn(geometry, 1.0, true, true, false, struct(id, geometry))"))

    val giDf = weightedDf
      .withColumn("gi", expr("ST_GLocal(id, someWeights, true)"))
    assert(
      gLocal(giDf, "id", weights = "someWeights", star = true)
        .where("G = gi.G")
        .count() == weightedDf.count())
  }

  it("test nested ST_Geostats calls with getis ord") {
    getData
      .withColumn(
        "GI",
        expr(
          "ST_GLocal(id, ST_BinaryDistanceBandColumn(geometry, 1.0, true, true, false, struct(id, geometry)), true)"))
      .collect()
  }

  it("test ST_Geostats with string column") {
    assume(spark.version.startsWith("3"), "DBSCAN doesn't work on Spark 4 yet")
    getData
      .withColumn("someString", lit("test"))
      .withColumn("sql_results", expr("ST_DBSCAN(geometry, 1.0, 4, false)"))
      .collect()
  }
}
