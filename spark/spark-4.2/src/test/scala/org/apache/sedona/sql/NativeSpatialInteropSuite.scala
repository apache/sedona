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

import org.apache.sedona.sql.UDF.Catalog
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, GeometryType, GeographyType}
import org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class NativeSpatialInteropSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("NativeSpatialInteropSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
    UdtRegistrator.registerAll()
    Catalog.registerAll(spark)
    spark.experimental.extraStrategies = Seq(new JoinQueryDetector(spark))
  }
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("native geometry flows through Sedona operations and back to native functions") {
    val result = spark.sql("""
      SELECT ST_SRID(ST_Buffer(ST_GeomFromWKB(
        unhex('0101000000000000000000F03F0000000000000040'), 3857), 1D)) AS srid,
        ST_Area(ST_Buffer(ST_GeomFromWKB(
        unhex('0101000000000000000000F03F0000000000000040'), 3857), 1D)) AS area
    """)
    val row = result.head()
    assert(row.getInt(0) == 3857)
    assert(row.getDouble(1) > 3d && row.getDouble(1) < 3.2d)
  }

  test("Sedona constructors expose native types and survive a shuffle") {
    val points = spark.sql("SELECT ST_Point(cast(id as double), 2D) AS geom FROM range(4)")
    assert(points.schema("geom").dataType.isInstanceOf[GeometryType])
    val result = points
      .repartition(2)
      .selectExpr("ST_X(geom) AS x", "ST_SRID(geom) AS srid")
      .collect()
      .sortBy(_.getDouble(0))
    assert(result.map(_.getDouble(0)).toSeq == Seq(0d, 1d, 2d, 3d))
    assert(result.forall(_.getInt(1) == 0))
  }

  test("geography constructors and arrays cross the native boundary") {
    val result = spark.sql("SELECT ST_GeogFromWKT('POINT (1 2)') AS geog")
    assert(result.schema("geog").dataType.isInstanceOf[GeographyType])
    assert(result.selectExpr("ST_SRID(geog)").head().getInt(0) == 4326)
    assert(
      spark
        .sql("SELECT ST_NumGeometries(ST_Collect(array(ST_Point(1D, 2D), ST_Point(3D, 4D))))")
        .head()
        .getInt(0) == 2)
  }

  test("spatial aggregates return native values after shuffle") {
    val result = spark
      .sql("SELECT ST_Point(cast(id as double), 2D) AS geom FROM range(4)")
      .repartition(2)
      .selectExpr("ST_Collect_Agg(geom) AS geom")
    assert(result.schema("geom").dataType.isInstanceOf[GeometryType])
    assert(result.selectExpr("ST_NumGeometries(geom)").head().getInt(0) == 4)
  }

  test("native values retain nulls and declared dimensions across Sedona functions") {
    assert(
      spark.sql("SELECT ST_AsText(ST_Reverse(cast(NULL AS GEOMETRY(0))))").head().isNullAt(0))
    val row = spark
      .sql("SELECT ST_AsEWKT(ST_Reverse(ST_GeomFromEWKT('SRID=4326;POINT ZM (1 2 3 4)')))")
      .head()
    assert(row.getString(0).contains("4326"))
    assert(row.getString(0).contains("1 2 3 4"))
  }

  test("arrays of native geometry survive projection and explode") {
    val result =
      spark.sql("SELECT ST_DumpPoints(ST_GeomFromWKT('LINESTRING (1 2, 3 4)', 4326)) AS points")
    assert(
      result
        .schema("points")
        .dataType
        .asInstanceOf[ArrayType]
        .elementType
        .isInstanceOf[GeometryType])
    val rows = result
      .selectExpr("explode(points) AS point")
      .repartition(2)
      .selectExpr("ST_X(point)", "ST_SRID(point)")
      .collect()
      .sortBy(_.getDouble(0))
    assert(rows.map(_.getDouble(0)).toSeq == Seq(1d, 3d))
    assert(rows.forall(_.getInt(1) == 4326))
  }

  test("spatial broadcast joins accept native columns and use Sedona's index") {
    spark
      .sql("SELECT id, ST_Point(cast(id AS DOUBLE), 0D) AS geom FROM range(5)")
      .createOrReplaceTempView("native_points")
    spark
      .sql("SELECT ST_Buffer(ST_Point(cast(id + 2 AS DOUBLE), 0D), 0.5D) AS geom FROM range(2)")
      .createOrReplaceTempView("native_regions")
    val joined = spark.sql("""SELECT /*+ BROADCAST(r) */ p.id, p.geom
      FROM native_points p JOIN native_regions r ON ST_Contains(r.geom, p.geom)""")
    assert(joined.queryExecution.executedPlan.toString.contains("BroadcastIndexJoin"))
    assert(
      joined
        .selectExpr("id", "ST_X(geom)")
        .collect()
        .map(r => (r.getLong(0), r.getDouble(1)))
        .toSeq
        .sorted == Seq((2L, 2d), (3L, 3d)))
  }

  test("fixed SRID native results round-trip through Spark Parquet") {
    val path = java.nio.file.Files.createTempDirectory("sedona-native-parquet").toFile
    try {
      val data = spark.sql("SELECT ST_Buffer(ST_GeomFromWKT('POINT (1 2)', 3857), 1D) AS geom")
      assert(data.schema("geom").dataType == GeometryType(3857))
      data.write.mode("overwrite").parquet(path.toString)
      val restored = spark.read.parquet(path.toString)
      assert(restored.schema("geom").dataType == GeometryType(3857))
      assert(restored.selectExpr("ST_SRID(geom)", "ST_Area(geom)").head().getInt(0) == 3857)
    } finally {
      org.apache.commons.io.FileUtils.deleteDirectory(path)
    }
  }

  test("GeoParquet pushdown recognizes native geometry columns") {
    import org.apache.spark.sql.catalyst.expressions.AttributeReference
    import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
    import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
    import org.apache.spark.sql.sedona_sql.expressions.{ST_Intersects, ST_Point}
    import org.apache.spark.sql.sedona_sql.optimization.SpatialFilterPushDownForGeoParquet
    import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
    import org.apache.spark.sql.catalyst.expressions.Literal
    val geom = AttributeReference("geom", GeometryType(0))()
    val point = SpatialTypeSupport.adaptFunction(ST_Point.apply)(Seq(Literal(1d), Literal(2d)))
    val condition = SpatialTypeSupport.adaptFunction(ST_Intersects.apply)(Seq(geom, point))
    val plan = ConstantFolding(Filter(condition, LocalRelation(geom)))
    val predicate = plan.asInstanceOf[Filter].condition
    val filters = new SpatialFilterPushDownForGeoParquet(spark)
      .translateToGeoParquetSpatialFilters(Seq(predicate))
    assert(filters.nonEmpty)
  }

  test("constructors with an SRID argument report matching native schemas") {
    Seq(
      "ST_PointZ(1D, 2D, 3D, 4326)",
      "ST_PointM(1D, 2D, 4D, 4326)",
      "ST_PointZM(1D, 2D, 3D, 4D, 4326)",
      "ST_MakeEnvelope(0D, 0D, 1D, 1D, 4326)").foreach { expression =>
      val result = spark.sql(s"SELECT $expression AS geom")
      assert(result.schema("geom").dataType == GeometryType(4326), expression)
      assert(result.selectExpr("ST_SRID(geom)").head().getInt(0) == 4326)
    }
    Seq("4326L", "'4326'").foreach { srid =>
      assert(
        spark
          .sql(s"SELECT ST_SRID(ST_GeomFromWKT('POINT (1 2)', $srid))")
          .head()
          .getInt(0) == 4326)
    }
    val varying = spark.sql("SELECT ST_PointZ(1D, 2D, 3D, cast(id as int)) AS geom FROM range(1)")
    assert(varying.schema("geom").dataType == GeometryType("ANY"))
  }

  test("geometry generators remain generators and return native values") {
    val result = spark.sql(
      "SELECT ST_SubDivideExplode(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)', 4326), 5) AS geom")
    assert(result.schema("geom").dataType.isInstanceOf[GeometryType])
    assert(result.selectExpr("ST_SRID(geom)", "ST_NumPoints(geom)").head().toSeq == Seq(4326, 3))
  }

  test("native geometry serialization rejects a fixed-schema SRID mismatch") {
    import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
    val geometry = org.apache.sedona.common.Constructors.geomFromWKT("POINT (1 2)", 4326)
    val error = intercept[RuntimeException] {
      SpatialTypeSupport.serializeGeometry(geometry, GeometryType(3857))
    }
    assert(error.getMessage.contains("GEO_ENCODER_SRID_MISMATCH_ERROR"))
  }

  test("ISO WKB conversion preserves dimensional values through native materialization") {
    Seq(
      "POINT Z (1 2 3)",
      "POINT M (1 2 4)",
      "POINT ZM (1 2 3 4)",
      "LINESTRING Z (1 2 3, 4 5 6)",
      "MULTIPOINT ZM ((1 2 3 4), (5 6 7 8))",
      "POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 0 1))",
      "POINT Z EMPTY",
      "POINT ZM EMPTY",
      "GEOMETRYCOLLECTION Z (POINT Z (1 2 3), LINESTRING Z (0 0 1, 1 1 2))",
      "MULTIPOLYGON Z (((0 0 1, 4 0 1, 4 4 1, 0 4 1, 0 0 1), (1 1 1, 1 2 1, 2 2 1, 1 1 1)))")
      .foreach { wkt =>
        val result = spark.sql(s"SELECT ST_GeomFromEWKT('SRID=4326;$wkt') AS geom").repartition(2)
        val native = result.collect().head.getAs[org.apache.spark.sql.types.Geometry](0)
        assert(native.getSrid == 4326)
        val original = org.apache.sedona.common.Constructors.geomFromEWKT(s"SRID=4326;$wkt")
        val restored =
          org.apache.sedona.common.Constructors.geomFromWKB(native.getBytes, native.getSrid)
        assert(
          org.apache.sedona.common.Functions
            .asEWKT(restored) == org.apache.sedona.common.Functions
            .asEWKT(original),
          wkt)
      }
  }

  test("representation adapters preserve public expression and column names") {
    assert(spark.sql("SELECT ST_Point(1.0, 2.0)").columns.toSeq == Seq("st_point(1.0, 2.0)"))
    val points = spark.sql("SELECT ST_Point(cast(id as double), 0D) AS geom FROM range(2)")
    assert(points.selectExpr("ST_X(geom)").columns.toSeq == Seq("st_x(geom)"))
    assert(points.selectExpr("ST_Collect_Agg(geom)").columns.toSeq == Seq("st_collect_agg(geom)"))
  }

  test("mixed-dimensional collections follow Spark's native rejection contract") {
    val wkb =
      "01EF0300000200000001E9030000000000000000F03F00000000000000400000000000000840010100000000000000000010400000000000001440"
    Seq(
      s"ST_GeomFromWKB(unhex('$wkb'), 4326)",
      "ST_GeomFromEWKT('SRID=4326;GEOMETRYCOLLECTION (POINT Z (1 2 3), POINT (4 5))')").foreach {
      expression =>
        val error = intercept[Exception] { spark.sql(s"SELECT $expression").head() }
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
        assert(
          causes.exists(cause => Option(cause.getMessage).exists(_.contains("WKB_PARSE_ERROR"))))
    }
  }
}
