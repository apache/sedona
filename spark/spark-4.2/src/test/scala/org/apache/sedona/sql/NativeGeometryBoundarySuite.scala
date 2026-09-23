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

import java.nio.file.Files

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.UDF.Catalog
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.stats.clustering.DBSCAN
import org.apache.sedona.util.DfUtils
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sedona_sql.expressions.st_constructors
import org.apache.spark.sql.sedona_sql.optimization.ExtractPhysicalFunctions
import org.apache.spark.sql.sedona_sql.strategy.physical.function.EvalPhysicalFunctionStrategy
import org.apache.spark.sql.sedona_sql.UDT.{Box2DUDT, Box3DUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.adapters.StructuredAdapter
import org.apache.spark.sql.sedona_sql.optimization.{Box2DCastResolutionRule, Box3DCastResolutionRule}
import org.apache.spark.sql.types.{GeometryType, StringType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class NativeGeometryBoundarySuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("NativeGeometryBoundarySuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .withExtensions { extensions =>
        extensions.injectResolutionRule(_ => new Box2DCastResolutionRule)
        extensions.injectResolutionRule(_ => new Box3DCastResolutionRule)
      }
      .getOrCreate()
    spark.sparkContext.setCheckpointDir(Files.createTempDirectory("native-dbscan").toString)
    UdtRegistrator.registerAll()
    Catalog.registerAll(spark)
    spark.experimental.extraOptimizations = Seq(ExtractPhysicalFunctions)
    spark.experimental.extraStrategies = Seq(new EvalPhysicalFunctionStrategy(spark))
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def point(srid: Int, label: String): Geometry = {
    val geometry = new WKTReader().read("POINT (1 2)")
    geometry.setSRID(srid)
    geometry.setUserData(label)
    geometry
  }

  private def spatialRdd(geometries: Seq[Geometry]): SpatialRDD[Geometry] = {
    val rdd = new SpatialRDD[Geometry]
    rdd.rawSpatialRDD = spark.sparkContext.parallelize(geometries).toJavaRDD()
    rdd
  }

  test("geometry column discovery accepts fixed, mixed and explicit legacy geometry types") {
    assert(
      DfUtils.getGeometryColumnName(
        StructType(Seq(
          StructField("shape", GeometryType(4326)),
          StructField("label", StringType)))) == "shape")
    assert(
      DfUtils.getGeometryColumnName(
        StructType(Seq(
          StructField("shape", GeometryType("ANY")),
          StructField("geometry", GeometryUDT())))) == "geometry")
  }

  test("Adapter defaults to native ANY and preserves SRID and user data through RDD conversion") {
    val frame =
      Adapter.toDf(spatialRdd(Seq(point(4326, "a"), point(3857, "b"))), Seq("label"), spark)
    assert(frame.schema("geometry").dataType == GeometryType("ANY"))
    assert(
      frame
        .selectExpr("ST_SRID(geometry)", "label")
        .collect()
        .map(row => (row.getInt(0), row.getString(1)))
        .toSet == Set((4326, "a"), (3857, "b")))
    val restored = Adapter.toSpatialRdd(frame, "geometry").rawSpatialRDD.collect()
    assert(restored.get(0).getSRID == 4326)
    assert(restored.get(0).getUserData == "a")
  }

  test("Adapter preserves explicit legacy schemas and supports explicit native schemas") {
    val rdd = spatialRdd(Seq(point(4326, "a")))
    val legacy =
      StructType(Seq(StructField("geometry", GeometryUDT()), StructField("label", StringType)))
    assert(Adapter.toDf(rdd, legacy, spark).head().getAs[Geometry](0).getSRID == 4326)
    val native = StructType(
      Seq(StructField("geometry", GeometryType(4326)), StructField("label", StringType)))
    val frame = Adapter.toDf(rdd, native, spark)
    assert(frame.schema == native)
    assert(frame.selectExpr("ST_SRID(geometry)").head().getInt(0) == 4326)
  }

  test("Adapter pair defaults use native geometry values on both sides") {
    val pairs = JavaPairRDD.fromRDD(
      spark.sparkContext.parallelize(Seq((point(4326, "a"), point(3857, "b")))))
    val frame = Adapter.toDf(pairs, Seq("leftlabel"), Seq("rightlabel"), spark)
    assert(frame.schema("leftgeometry").dataType == GeometryType("ANY"))
    assert(frame.schema("rightgeometry").dataType == GeometryType("ANY"))
    val row = frame
      .selectExpr("ST_SRID(leftgeometry)", "ST_SRID(rightgeometry)", "leftlabel", "rightlabel")
      .head()
    assert(row.toSeq == Seq(4326, 3857, "a", "b"))
  }

  test("StructuredAdapter preserves native internal and external row representations") {
    val frame = spark.sql(
      "SELECT ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040'), 4326) AS shape, 7 AS id")
    val internal = StructuredAdapter.toSpatialRdd(frame)
    assert(internal.rawSpatialRDD.first().getSRID == 4326)
    val restored = StructuredAdapter.toDf(internal, spark)
    assert(restored.schema == frame.schema)
    assert(restored.selectExpr("ST_SRID(shape)", "id").head().toSeq == Seq(4326, 7))
    val external = StructuredAdapter.toSpatialRdd(frame.rdd)
    assert(external.rawSpatialRDD.first().getSRID == 4326)
    val rows = StructuredAdapter.toRowRdd(external)
    assert(
      spark
        .createDataFrame(rows, frame.schema)
        .selectExpr("ST_SRID(shape)")
        .head()
        .getInt(0) == 4326)
  }

  test("Box casts accept native input and honor the requested native output type") {
    val points = spark.sql("SELECT ST_Point(1D, 2D) AS geometry")
    val box = points.select(col("geometry").cast(Box2DUDT()).as("box"))
    assert(box.selectExpr("ST_XMin(box)").head().getDouble(0) == 1d)
    val geometry = box.select(col("box").cast(GeometryType(0)).as("geometry"))
    assert(geometry.schema("geometry").dataType == GeometryType(0))
    assert(geometry.selectExpr("ST_SRID(geometry)", "ST_X(geometry)").head().toSeq == Seq(0, 1d))
    val anyGeometry = box.select(col("box").cast(GeometryType("ANY")).as("geometry"))
    assert(anyGeometry.schema("geometry").dataType == GeometryType("ANY"))
    assert(anyGeometry.selectExpr("ST_SRID(geometry)").head().getInt(0) == 0)
    intercept[org.apache.spark.sql.AnalysisException] {
      box.select(col("box").cast(GeometryType(3857))).queryExecution.analyzed
    }
    val legacy = box.select(col("box").cast(GeometryUDT()).as("geometry"))
    assert(legacy.head().getAs[Geometry](0).getCoordinate.x == 1d)
    val box3d = spark
      .sql("SELECT ST_PointZ(1D, 2D, 3D) AS geometry")
      .select(col("geometry").cast(Box3DUDT()).as("box"))
    assert(box3d.selectExpr("ST_ZMin(box)").head().getDouble(0) == 3d)
  }

  test("DBSCAN accepts native geometry and retains native geometry in its results") {
    val frame = spark.sql("""SELECT id, ST_Point(x, 0D) AS geometry
      FROM VALUES (1, 0D), (2, 0.1D), (3, 10D) AS input(id, x)""")
    val result = DBSCAN.dbscan(frame, 0.2, 2)
    assert(result.schema("geometry").dataType.isInstanceOf[GeometryType])
    val rows = result.select("id", "cluster", "isCore").collect().sortBy(_.getInt(0))
    assert(rows.length == 3)
    assert(rows(0).getLong(1) == rows(1).getLong(1))
    assert(rows(0).getBoolean(2) && rows(1).getBoolean(2))
    assert(rows(2).getLong(1) == -1L)
  }
  test("SQL DBSCAN resolves native geometry column references") {
    spark
      .sql("""SELECT id, ST_Point(x, 0D) AS geometry
      FROM VALUES (1, 0D), (2, 0.1D), (3, 10D) AS input(id, x)""")
      .createOrReplaceTempView("native_dbscan_points")
    val result = spark.sql("""SELECT id, ST_DBSCAN(geometry, 0.2D, 2, false) AS cluster
      FROM native_dbscan_points""")
    val rows =
      result.selectExpr("id", "cluster.isCore", "cluster.cluster").collect().sortBy(_.getInt(0))
    assert(rows.length == 3)
    assert(rows(0).getBoolean(1) && rows(1).getBoolean(1))
    assert(rows(0).getLong(2) == rows(1).getLong(2))
    assert(rows(2).getLong(2) == -1L)
  }

  test("Scala geography text constructors default to native SRID 4326") {
    val constructors = Seq(
      st_constructors.ST_GeogFromWKT(lit("POINT (1 2)")),
      st_constructors.ST_GeogFromText(lit("POINT (1 2)")),
      st_constructors.ST_GeogCollFromText(lit("GEOMETRYCOLLECTION (POINT (1 2))")))
    val namedConstructors = Seq(
      st_constructors.ST_GeogFromWKT("wkt"),
      st_constructors.ST_GeogFromText("wkt"),
      st_constructors.ST_GeogCollFromText("wkt"))
    namedConstructors.foreach { constructor =>
      assert(
        spark
          .sql("SELECT 'GEOMETRYCOLLECTION (POINT (1 2))' AS wkt")
          .select(constructor.as("geography"))
          .selectExpr("ST_SRID(geography)")
          .head()
          .getInt(0) == 4326)
    }
    constructors.foreach { constructor =>
      assert(
        spark
          .range(1)
          .select(constructor.as("geography"))
          .selectExpr("ST_SRID(geography)")
          .head()
          .getInt(0) == 4326)
    }
  }
  test("DBSCAN preserves nested spatial attributes and nulls") {
    val frame = spark.sql("""SELECT id, ST_Point(x, 0D) AS geometry,
      CASE WHEN id = 3 THEN null ELSE named_struct('location', ST_Point(x, 0D)) END AS metadata,
      array(ST_Point(x, 0D), null) AS locations,
      map('home', ST_GeogFromWKT('POINT (1 2)'), 'missing', null) AS geography_map
      FROM VALUES (1, 0D), (2, 0.1D), (3, 10D) AS input(id, x)""")
    val result = DBSCAN.dbscan(frame, 0.2, 2)
    assert(result.schema("metadata").dataType == frame.schema("metadata").dataType)
    assert(result.schema("locations").dataType == frame.schema("locations").dataType)
    assert(result.schema("geography_map").dataType == frame.schema("geography_map").dataType)
    val rows = result
      .selectExpr(
        "id",
        "metadata IS NULL",
        "locations[1] IS NULL",
        "ST_SRID(geography_map['home'])",
        "geography_map['missing'] IS NULL")
      .collect()
      .sortBy(_.getInt(0))
    assert(rows.length == 3)
    assert(!rows(0).getBoolean(1) && rows(2).getBoolean(1))
    assert(rows.forall(row => row.getBoolean(2) && row.getInt(3) == 4326 && row.getBoolean(4)))
  }
}
