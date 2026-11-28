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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.sedona.sql.GeoParquetSpatialFilterPushDownSuite.readGeoParquetMetaDataMap
import org.apache.sedona.sql.GeoParquetSpatialFilterPushDownSuite.writeTestDataAsGeoParquet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SimpleMode
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.spark.sql.execution.datasources.geoparquet.{GeoParquetFileFormat, GeoParquetMetaData, GeoParquetSpatialFilter, GeometryFieldMetaData}
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter.{DistanceLeafFilter, LeafFilter}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader
import org.scalatest.prop.TableDrivenPropertyChecks

import java.io.File
import java.nio.file.Files

class GeoParquetSpatialFilterPushDownSuite extends TestBaseScala with TableDrivenPropertyChecks {

  val tempDir: String =
    Files.createTempDirectory("sedona_geoparquet_test_").toFile.getAbsolutePath
  val geoParquetDir: String = tempDir + "/geoparquet"
  var df: DataFrame = _
  var geoParquetDf: DataFrame = _
  var geoParquetMetaDataMap: Map[Int, Seq[GeoParquetMetaData]] = _
  private val wktReader = new WKTReader()

  override def beforeAll(): Unit = {
    super.beforeAll()
    df = GeoParquetSpatialFilterPushDownSuite.generateTestData(sparkSession)
    writeTestDataAsGeoParquet(df, geoParquetDir)
    geoParquetDf = sparkSession.read.format("geoparquet").load(geoParquetDir)
    geoParquetMetaDataMap = readGeoParquetMetaDataMap(geoParquetDir)
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(tempDir))

  describe("GeoParquet spatial filter push down tests") {
    it("Push down ST_Contains") {
      testFilter(
        "ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(1))
      testFilter(
        "ST_Contains(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)",
        Seq(0))
      testFilter(
        "ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'), geom)",
        Seq.empty)
      testFilter("ST_Contains(geom, ST_GeomFromText('POINT (15 -15)'))", Seq(3))
      testFilter(
        "ST_Contains(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))",
        Seq(3))
      testFilter(
        "ST_Contains(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))",
        Seq.empty)
    }

    it("Push down ST_Covers") {
      testFilter(
        "ST_Covers(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(1))
      testFilter(
        "ST_Covers(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)",
        Seq(0))
      testFilter(
        "ST_Covers(ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'), geom)",
        Seq.empty)
      testFilter("ST_Covers(geom, ST_GeomFromText('POINT (15 -15)'))", Seq(3))
      testFilter(
        "ST_Covers(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))",
        Seq(3))
      testFilter(
        "ST_Covers(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))",
        Seq.empty)
    }

    it("Push down ST_Within") {
      testFilter(
        "ST_Within(geom, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        Seq(1))
      testFilter(
        "ST_Within(geom, ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'))",
        Seq(0))
      testFilter(
        "ST_Within(geom, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        Seq.empty)
      testFilter("ST_Within(ST_GeomFromText('POINT (15 -15)'), geom)", Seq(3))
      testFilter(
        "ST_Within(ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'), geom)",
        Seq(3))
      testFilter(
        "ST_Within(ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'), geom)",
        Seq.empty)
    }

    it("Push down ST_CoveredBy") {
      testFilter(
        "ST_CoveredBy(geom, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        Seq(1))
      testFilter(
        "ST_CoveredBy(geom, ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'))",
        Seq(0))
      testFilter(
        "ST_CoveredBy(geom, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        Seq.empty)
      testFilter("ST_CoveredBy(ST_GeomFromText('POINT (15 -15)'), geom)", Seq(3))
      testFilter(
        "ST_CoveredBy(ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'), geom)",
        Seq(3))
      testFilter(
        "ST_CoveredBy(ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'), geom)",
        Seq.empty)
    }

    it("Push down ST_Intersects") {
      testFilter(
        "ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(1))
      testFilter(
        "ST_Intersects(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)",
        Seq(0))
      testFilter(
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        Seq.empty)
      testFilter("ST_Intersects(geom, ST_GeomFromText('POINT (15 -15)'))", Seq(3))
      testFilter(
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))",
        Seq(3))
      testFilter(
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))",
        Seq(3))
      testFilter(
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))'))",
        Seq(1, 3))
    }

    it("Push down ST_Equals") {
      testFilter(
        "ST_Equals(geom, ST_GeomFromText('POLYGON ((-16 -16, -16 -14, -14 -14, -14 -16, -16 -16))'))",
        Seq(2))
      testFilter("ST_Equals(geom, ST_GeomFromText('POINT (-15 -15)'))", Seq(2))
      testFilter("ST_Equals(geom, ST_GeomFromText('POINT (-16 -16)'))", Seq(2))
      testFilter(
        "ST_Equals(geom, ST_GeomFromText('POLYGON ((1 -5, 5 -5, 5 -1, 1 -1, 1 -5))'))",
        Seq.empty)
    }

    it("Push down ST_DWithin") {
      testFilter("ST_DWithin(geom, ST_GeomFromText('POINT (3 4)'), 1)", Seq(1))
      testFilter("ST_DWithin(geom, ST_GeomFromText('POINT (0 0)'), 1)", Seq.empty)
      testFilter("ST_DWithin(geom, ST_GeomFromText('POINT (0 0)'), 3.9)", Seq.empty)
      // The corner of the bounding box of each file that is next to (0 0) is 4 units from it
      // in X and in Y, which is a distance of some 5.66
      testFilter("ST_DWithin(geom, ST_GeomFromText('POINT (0 0)'), 5)", Seq.empty)
      testFilter("ST_DWithin(geom, ST_GeomFromText('POINT (0 0)'), 5.7)", Seq(0, 1, 2, 3))
      testFilter("ST_DWithin(geom, ST_GeomFromText('POINT (-5 -5)'), 1)", Seq(2))
      testFilter(
        "ST_DWithin(geom, ST_GeomFromText('POLYGON ((-16 -16, -16 -14, -14 -14, -14 -16, -16 -16))'), 10)",
        Seq(2))
      testFilter("ST_DWithin(ST_GeomFromText('POINT (-5 -5)'), geom, 1)", Seq(2))
      testFilter("ST_DWithin(ST_GeomFromText('POINT (-5 -5)'), geom, 1, false)", Seq(2))
    }

    forAll(Table("op", "<", "<=")) { op =>
      it(s"Push down ST_Distance $op d") {
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (0 0)')) $op 1", Seq.empty)
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (0 0)')) $op 3.9", Seq.empty)
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (3 4)')) $op 1", Seq(1))
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (0 0)')) $op 5", Seq.empty)
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (0 0)')) $op 5.7", Seq(0, 1, 2, 3))
        testFilter(s"ST_Distance(geom, ST_GeomFromText('POINT (-5 -5)')) $op 1", Seq(2))
        testFilter(
          s"ST_Distance(geom, ST_GeomFromText('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')) $op 2",
          Seq.empty)
        testFilter(
          s"ST_Distance(geom, ST_GeomFromText('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')) $op 4.3",
          Seq(0, 1, 2, 3))
        testFilter(
          s"ST_Distance(geom, ST_GeomFromText('LINESTRING (17 17, 18 18)')) $op 1",
          Seq.empty)
        testFilter(
          s"ST_Distance(geom, ST_GeomFromText('LINESTRING (17 17, 18 18)')) $op 1.5",
          Seq(1))
      }
    }

    it("Pushes no filter for a distance on the sphere or on the spheroid") {
      // The bounding box of a file is one of a plane. A distance in meters is measured between
      // the centroids of two geometries, on a sphere or a spheroid that the box says nothing of.
      Seq(
        "ST_DistanceSpheroid(geom, ST_GeomFromText('POINT (3 4)')) < 500000",
        "ST_DistanceSpheroid(geom, ST_GeomFromText('POINT (3 4)')) <= 500000",
        "ST_DistanceSphere(geom, ST_GeomFromText('POINT (-5 -5)')) < 500000",
        "ST_DistanceSphere(geom, ST_GeomFromText('POINT (-5 -5)')) <= 500000",
        "ST_DistanceSphere(geom, ST_GeomFromText('POINT (-5 -5)'), 5000000) <= 400000",
        "ST_DWithin(geom, ST_GeomFromText('POINT (3 4)'), 1000000, true)",
        "ST_DWithin(ST_GeomFromText('POINT (3 4)'), geom, 1000000, true)").foreach { condition =>
        val filtered = geoParquetDf.where(condition)
        assert(getPushedDownSpatialFilter(filtered).isEmpty, condition)
        val expected = df.where(condition).orderBy("region", "id").select("region", "id")
        assert(expected.count() > 0, condition)
        assert(
          filtered.orderBy("region", "id").select("region", "id").collect() sameElements
            expected.collect(),
          condition)
      }
      // Next to a predicate that is pushed down, the file is left out by that predicate
      testFilter(
        "ST_DistanceSphere(geom, ST_GeomFromText('POINT (5 5)')) < 500000 AND " +
          "ST_Intersects(geom, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        Seq(1))
    }

    it("Push down And(filters...)") {
      testFilter(
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))')) AND ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(1))
      testFilter(
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))')) AND ST_Intersects(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))'))",
        Seq(3))
    }

    it("Push down Or(filters...)") {
      testFilter(
        "ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom) OR ST_Intersects(ST_GeomFromText('POLYGON ((-16 14, -16 16, -14 16, -14 14, -16 14))'), geom)",
        Seq(0, 1))
      testFilter(
        "ST_Distance(geom, ST_GeomFromText('POINT (-5 -5)')) <= 1 OR ST_Intersects(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(1, 2))
    }

    it("Ignore negated spatial filters") {
      testFilter(
        "NOT ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(0, 1, 2, 3))
      testFilter(
        "ST_Contains(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))')) AND NOT ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(3))
      testFilter(
        "ST_Contains(geom, ST_GeomFromText('POLYGON ((4 -5, 5 -5, 5 -4, 4 -4, 4 -5))')) OR NOT ST_Contains(ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), geom)",
        Seq(0, 1, 2, 3))
    }

    it("Mixed spatial filter with other filter") {
      testFilter(
        "id < 10 AND ST_Intersects(geom, ST_GeomFromText('POLYGON ((5 -5, 15 -5, 15 5, 5 5, 5 -5))'))",
        Seq(1, 3))
    }

    it("Explain geoparquet scan with spatial filter push-down") {
      val dfFiltered = geoParquetDf.where(
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'))")
      val explainString = dfFiltered.queryExecution.explainString(SimpleMode)
      assert(explainString.contains("FileScan geoparquet"))
      assert(explainString.contains("with spatial filter"))
    }

    it("Manually disable spatial filter push-down") {
      withConf(Map("spark.sedona.geoparquet.spatialFilterPushDown" -> "false")) {
        val dfFiltered = geoParquetDf.where(
          "ST_Intersects(geom, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'))")
        val explainString = dfFiltered.queryExecution.explainString(SimpleMode)
        assert(explainString.contains("FileScan geoparquet"))
        assert(!explainString.contains("with spatial filter"))
        assert(getPushedDownSpatialFilter(dfFiltered).isEmpty)
      }
    }

    it("Reads the X and Y bounds of a bbox that also has Z or M bounds") {
      // The bboxes span x from 1 to 10 and y from 2 to 15, so that a window can lie inside one
      // range and outside the other.
      val inside = "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))"
      val aboveXmaxBelowYmax = "POLYGON ((11 11, 14 11, 14 14, 11 14, 11 11))"
      val belowXmaxAboveYmax = "POLYGON ((4 16, 6 16, 6 18, 4 18, 4 16))"
      val belowXminInsideY = "POLYGON ((-30 4, -20 4, -20 6, -30 6, -30 4))"
      val insideByYOnly = "POLYGON ((4 11, 6 11, 6 14, 4 14, 4 11))"
      val bboxes = Table(
        "bbox",
        // xmin, ymin, xmax, ymax
        Seq(1.0, 2.0, 10.0, 15.0),
        // xmin, ymin, zmin, xmax, ymax, zmax
        Seq(1.0, 2.0, -50.0, 10.0, 15.0, -40.0),
        // xmin, ymin, zmin, mmin, xmax, ymax, zmax, mmax
        Seq(1.0, 2.0, -50.0, -7.0, 10.0, 15.0, -40.0, -3.0))
      forAll(bboxes) { bbox =>
        val columns = Map("geom" -> GeometryFieldMetaData("WKB", Seq.empty, Some(bbox)))
        Seq(SpatialPredicate.INTERSECTS, SpatialPredicate.COVERS).foreach { predicate =>
          def keeps(window: String): Boolean =
            LeafFilter("geom", predicate, wktReader.read(window)).evaluate(columns)
          assert(keeps(inside))
          // y from 11 to 14 is inside the bbox only if ymax is read as 15
          assert(keeps(insideByYOnly))
          assert(!keeps(aboveXmaxBelowYmax))
          assert(!keeps(belowXmaxAboveYmax))
          assert(!keeps(belowXminInsideY))
        }
      }
    }

    it("Keeps a file whose bbox crosses the antimeridian") {
      // RFC 7946 lists the west bound first, so xmin is greater than xmax: here from 170
      // degrees east to 170 degrees west. A point at 179 degrees east is inside.
      val window = wktReader.read("POINT (179 0)")
      Seq(
        Seq(170.0, -5.0, -170.0, 5.0),
        Seq(170.0, -5.0, 200.0, -170.0, 5.0, 210.0),
        Seq(170.0, -5.0, 200.0, 0.0, -170.0, 5.0, 210.0, 1.0)).foreach { bbox =>
        val columns = Map("geom" -> GeometryFieldMetaData("WKB", Seq.empty, Some(bbox)))
        Seq(SpatialPredicate.INTERSECTS, SpatialPredicate.COVERS).foreach { predicate =>
          assert(LeafFilter("geom", predicate, window).evaluate(columns), bbox)
        }
      }
    }

    it("Keeps a file whose bbox has bounds that are not in order") {
      val window = wktReader.read("POINT (100 100)")
      Seq(
        Seq(1.0, 15.0, 10.0, 2.0),
        Seq(Double.NaN, 2.0, 10.0, 15.0),
        Seq(1.0, 2.0, 10.0, Double.NaN))
        .foreach { bbox =>
          val columns = Map("geom" -> GeometryFieldMetaData("WKB", Seq.empty, Some(bbox)))
          assert(LeafFilter("geom", SpatialPredicate.INTERSECTS, window).evaluate(columns))
        }
    }

    it("Keeps a file whose bbox has an unexpected number of values") {
      val window = wktReader.read("POLYGON ((-30 4, -20 4, -20 6, -30 6, -30 4))")
      Seq(Seq(1.0, 2.0, 10.0), Seq(1.0, 2.0, -50.0, 10.0, 10.0), Seq.fill(10)(1.0)).foreach {
        bbox =>
          val columns = Map("geom" -> GeometryFieldMetaData("WKB", Seq.empty, Some(bbox)))
          assert(LeafFilter("geom", SpatialPredicate.INTERSECTS, window).evaluate(columns))
      }
    }

    it("Filters a file whose bbox has Z bounds") {
      // Written by GDAL 3.13 from three points with Z. Its bbox is [1, 2, -50, 10, 10, -40].
      val pointsWithZ =
        sparkSession.read
          .format("geoparquet")
          .load(resourceFolder + "geoparquet/gdal/point-z.parquet")
      val filtered = pointsWithZ.where(
        "ST_Intersects(geometry, ST_GeomFromText('POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))'))")
      assert(getPushedDownSpatialFilter(filtered).isDefined)
      assert(filtered.select("id").collect().map(_.getString(0)).toSeq == Seq("2"))
    }

    it("Push down ST_Intersects against a Box2D column") {
      val (box2dDf, box2dDir) = setupBox2DCoveringFixture()
      try {
        // Q1 region only (region 1, center +10/+10)
        val q1Filter =
          "ST_Intersects(geom_bbox, ST_MakeBox2D(ST_Point(5.0, 5.0), ST_Point(15.0, 15.0)))"
        verifyBox2DFilter(box2dDf, q1Filter)

        // Window covering Q2 and Q4 (negative X)
        val leftHalfFilter =
          "ST_Intersects(geom_bbox, ST_MakeBox2D(ST_Point(-20.0, -20.0), ST_Point(-1.0, 20.0)))"
        verifyBox2DFilter(box2dDf, leftHalfFilter)

        // Disjoint window prunes everything
        val disjointFilter =
          "ST_Intersects(geom_bbox, ST_MakeBox2D(ST_Point(100.0, 100.0), ST_Point(200.0, 200.0)))"
        verifyBox2DFilter(box2dDf, disjointFilter)

        // Reverse argument order: ST_Intersects(lit, col) is symmetric.
        val reversedFilter =
          "ST_Intersects(ST_MakeBox2D(ST_Point(5.0, 5.0), ST_Point(15.0, 15.0)), geom_bbox)"
        verifyBox2DFilter(box2dDf, reversedFilter)
      } finally {
        FileUtils.deleteDirectory(new File(box2dDir).getParentFile)
      }
    }

    it("ST_Intersects with inverted-bound literal falls back to runtime throw") {
      val (box2dDf, box2dDir) = setupBox2DCoveringFixture()
      try {
        // xmin > xmax: must not push down — otherwise Parquet's row-group filter could prune all
        // matches and hide the expected IllegalArgumentException from the predicate's runtime
        // evaluation.
        val invertedFilter =
          "ST_Intersects(geom_bbox, ST_MakeBox2D(ST_Point(20.0, -20.0), ST_Point(-20.0, 20.0)))"
        val dfFiltered = box2dDf.where(invertedFilter)
        assert(
          getPushedDownSpatialFilter(dfFiltered).isEmpty,
          "Inverted-bound Box2D literal must not be pushed down")
        val ex = intercept[Exception](dfFiltered.collect())
        assert(
          Iterator
            .iterate(ex: Throwable)(_.getCause)
            .takeWhile(_ != null)
            .exists(_.isInstanceOf[IllegalArgumentException]),
          s"Expected IllegalArgumentException in cause chain, got: $ex")
      } finally {
        FileUtils.deleteDirectory(new File(box2dDir).getParentFile)
      }
    }

    it("Push down ST_Contains against a Box2D column") {
      val (box2dDf, box2dDir) = setupBox2DCoveringFixture()
      try {
        // ST_Contains(box_col, lit_box) — the column box must contain the literal box. A tiny
        // query inside Q1 is contained only by rows from region 1.
        val containsFilter =
          "ST_Contains(geom_bbox, ST_MakeBox2D(ST_Point(9.0, 9.0), ST_Point(10.0, 10.0)))"
        verifyBox2DFilter(box2dDf, containsFilter)

        // Reverse argument order: ST_Contains(lit_box, col) — the literal box must contain the
        // column box. The 10x10 window in Q1 contains the 2x2 polygons centered at (5,5), (5,15),
        // (15,5), (15,15) only partially; only rows whose envelopes lie entirely inside the window
        // survive.
        val reversedFilter =
          "ST_Contains(ST_MakeBox2D(ST_Point(4.0, 4.0), ST_Point(16.0, 16.0)), geom_bbox)"
        verifyBox2DFilter(box2dDf, reversedFilter)
      } finally {
        FileUtils.deleteDirectory(new File(box2dDir).getParentFile)
      }
    }
  }

  describe("GeoParquet spatial filter push down for a column whose name needs quotes") {

    /** The rows of `condition` with and without the filter of `condition` pushed down. */
    def verify(data: DataFrame, condition: String, column: String): Unit = {
      val filtered = data.where(condition)
      val pushed = getPushedDownSpatialFilter(filtered)
      assert(pushed.exists(_.simpleString.startsWith(column + " ")), condition)
      val expected = withConf(Map("spark.sedona.geoparquet.spatialFilterPushDown" -> "false")) {
        data.where(condition).orderBy("region", "id").select("region", "id").collect().toSeq
      }
      assert(expected.nonEmpty, condition)
      assert(
        filtered.orderBy("region", "id").select("region", "id").collect().toSeq == expected,
        condition)
    }

    def withData(data: DataFrame)(run: DataFrame => Unit): Unit = {
      val parent = Files.createTempDirectory("sedona_geoparquet_quoted_").toFile
      try {
        val path = parent.getAbsolutePath + "/data"
        writeTestDataAsGeoParquet(data, path)
        run(sparkSession.read.format("geoparquet").load(path))
      } finally FileUtils.deleteDirectory(parent)
    }

    val window = "ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))')"

    it("Push down a filter on a geometry column with a hyphen in its name") {
      withData(df.withColumnRenamed("geom", "geo-site")) { data =>
        verify(data, s"ST_Intersects(`geo-site`, $window)", "geo-site")
        verify(data, s"ST_Intersects($window, `geo-site`)", "geo-site")
        verify(data, s"ST_Within(`geo-site`, $window)", "geo-site")
        verify(data, s"ST_Equals(`geo-site`, ST_GeomFromText('POINT (5 5)'))", "geo-site")
        verify(data, "ST_DWithin(`geo-site`, ST_GeomFromText('POINT (5 5)'), 2.0)", "geo-site")
        verify(data, "ST_Distance(`geo-site`, ST_GeomFromText('POINT (5 5)')) < 2.0", "geo-site")
      }
    }

    it("Push down a filter on a geometry column with a backtick in its name") {
      // Next to a column of that name without the backticks, whose geometries are elsewhere
      val data = df
        .withColumnRenamed("geom", "`geom`")
        .withColumn("geom", expr("ST_Point(1000, 1000)"))
      withData(data) { data =>
        verify(data, s"ST_Intersects(```geom```, $window)", "`geom`")
      }
    }

    it("Push down a filter on a Box2D column with a hyphen in its name") {
      withData(df.withColumn("geom-bbox", expr("ST_Box2D(geom)"))) { data =>
        val condition =
          "ST_Intersects(`geom-bbox`, ST_MakeBox2D(ST_Point(5.0, 5.0), ST_Point(15.0, 15.0)))"
        verify(data, condition, "geom-bbox")
        verifyBox2DFilter(data, condition)
      }
    }
  }

  describe("GeoParquet spatial filter push down for a file that a scan rejects at a value") {
    it("Reads a file that the filter leaves out when a scan rejects another column of it") {
      if (SpatialTypeSupport.usesNativeTypes) {
        val parent = Files.createTempDirectory("sedona_geoparquet_rejected_").toFile
        try {
          def schema(payload: DataType) = StructType(
            Seq(
              StructField("id", LongType),
              StructField("geom", SpatialTypeSupport.geometryType(4326)),
              StructField("payload", payload)))
          // The bounding box of the file is far from the window of the query
          val condition =
            "ST_Intersects(geom, ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326))"
          // The read takes the column payload as well, whose type names another SRID than the
          // file has for it. A scan finds that at the first value of the column. A geometry
          // without an SRID is written without a CRS.
          Seq(
            (
              "ST_GeogFromWKT('POINT (1 2)', 4269)",
              SpatialTypeSupport.geographyType(4326),
              "requires SRID 4326 but contains SRID 4269"),
            (
              "ST_Point(1, 2)",
              SpatialTypeSupport.geometryType(4326),
              "requires SRID 4326 but contains SRID 0"),
            // A column of strings, which are no geographies
            (
              "'POINT (1 2)'",
              SpatialTypeSupport.geographyType(4326),
              "is stored as binary, which is read as values of Sedona's own geography type")).zipWithIndex
            .foreach { case ((payload, dataType, expected), index) =>
              val path = parent.getAbsolutePath + s"/data$index"
              sparkSession
                .range(10)
                .selectExpr(
                  "id",
                  "ST_SetSRID(ST_Point(45 + id, 68.2), 4326) AS geom",
                  s"$payload AS payload")
                .coalesce(1)
                .write
                .format("geoparquet")
                .option("geoparquet.version", "2.0.0")
                .save(path)
              for (push <- Seq("false", "true")) {
                withConf(Map("spark.sedona.geoparquet.spatialFilterPushDown" -> push)) {
                  val error = intercept[Exception] {
                    sparkSession.read
                      .schema(schema(dataType))
                      .format("geoparquet")
                      .load(path)
                      .where(condition)
                      .collect()
                  }
                  val messages = Iterator
                    .iterate[Throwable](error)(_.getCause)
                    .takeWhile(_ != null)
                    .flatMap(cause => Option(cause.getMessage))
                    .mkString("\n")
                  assert(messages.contains(expected), s"$payload $push")
                }
              }

              // With the SRID that the file has for the column, the file is left out
              val data = sparkSession.read.format("geoparquet").load(path).where(condition)
              assert(data.schema("payload").dataType != dataType)
              assert(getPushedDownSpatialFilter(data).isDefined)
              assert(data.collect().isEmpty)
              val scan = data.queryExecution.executedPlan.collectFirst {
                case scan: FileSourceScanExec => scan
              }
              assert(scan.get.metrics("numOutputRows").value == 0)
            }
        } finally FileUtils.deleteDirectory(parent)
      }
    }
  }

  describe("GeoParquet spatial filter push down keeps the files with a row of the predicate") {
    val PUSH_DOWN = "spark.sedona.geoparquet.spatialFilterPushDown"

    /** A file of the rows of `sql`, which have an id and a geometry `geom`. */
    def withFile(sql: String)(run: DataFrame => Unit): Unit = {
      val parent = Files.createTempDirectory("sedona_geoparquet_kept_").toFile
      try {
        val path = parent.getAbsolutePath + "/data"
        sparkSession.sql(sql).coalesce(1).write.format("geoparquet").save(path)
        run(sparkSession.read.format("geoparquet").load(path))
      } finally FileUtils.deleteDirectory(parent)
    }

    /**
     * Reads the rows of `condition` with and without its filter pushed down, which have to be the
     * same rows and more than none, and gives the filter that was pushed down.
     */
    def compare(data: DataFrame, condition: String): Option[GeoParquetSpatialFilter] = {
      def ids(df: DataFrame) = df.select("id").collect().map(_.getLong(0)).sorted.toSeq
      val expected = withConf(Map(PUSH_DOWN -> "false")) {
        val unfiltered = data.where(condition)
        assert(getPushedDownSpatialFilter(unfiltered).isEmpty)
        ids(unfiltered)
      }
      assert(expected.nonEmpty, condition)
      val filtered = data.where(condition)
      assert(ids(filtered) == expected, condition)
      getPushedDownSpatialFilter(filtered)
    }

    it("Keeps a file whose geometries are at the distance of the predicate after rounding") {
      // The distance from -2 to 0.3 rounds to 2.3, and -2 + 2.3 is below 0.3
      assert(0.3 - -2.0 == 2.3 && -2.0 + 2.3 < 0.3)
      withFile("SELECT id, ST_Point(0.3, 0.0) AS geom FROM range(3)") { data =>
        Seq(
          "ST_DWithin(geom, ST_Point(-2.0, 0.0), 2.3)",
          "ST_DWithin(ST_Point(-2.0, 0.0), geom, 2.3)",
          "ST_Distance(geom, ST_Point(-2.0, 0.0)) <= 2.3").foreach(condition =>
          assert(compare(data, condition).isDefined, condition))
      }
      // The distance from 9.999999999999998 to 100 rounds to 90
      assert(100.0 - 9.999999999999998 == 90.0 && 100.0 - 90.0 > 9.999999999999998)
      withFile("SELECT id, ST_Point(9.999999999999998, 0.0) AS geom FROM range(3)") { data =>
        Seq(
          "ST_DWithin(geom, ST_Point(100.0, 0.0), 90.0)",
          "ST_Distance(geom, ST_Point(100.0, 0.0)) <= 90.0").foreach(condition =>
          assert(compare(data, condition).isDefined, condition))
      }
      // Coordinates of a projected CRS, and a distance to a line
      withFile("SELECT id, ST_Point(500000.3, 4649776.1) AS geom FROM range(3)") { data =>
        val line = "ST_GeomFromWKT('LINESTRING (499998 4649000, 499998 4650000)')"
        assert(compare(data, s"ST_DWithin(geom, $line, 2.3)").isDefined)
        assert(compare(data, s"ST_Distance($line, geom) <= 2.3").isDefined)
      }
    }

    it("Keeps a file with a geometry that is at the distance of the predicate, anywhere") {
      // A geometry of a row and one of the query, and the distance between them as the
      // predicate computes it. A predicate with that distance keeps the row, so its filter has
      // to keep a file whose bounding box is that of the geometry of the row.
      val random = new scala.util.Random(53)
      val factory = new GeometryFactory()
      def geometry(scale: Double, extent: Double): Geometry = {
        def coordinate() = new Coordinate(
          (random.nextDouble() - 0.5) * scale + (random.nextDouble() - 0.5) * extent,
          (random.nextDouble() - 0.5) * scale + (random.nextDouble() - 0.5) * extent)
        random.nextInt(4) match {
          case 0 => factory.createPoint(coordinate())
          case 1 => factory.createLineString(Array(coordinate(), coordinate(), coordinate()))
          // A line along an axis is as far from a geometry next to it as its bounding box is,
          // and the distance to it is not computed as the distance to that box is
          case 2 =>
            val (start, end) = (coordinate(), coordinate())
            factory.createLineString(Array(start, new Coordinate(start.x, end.y)))
          case _ =>
            val (start, end) = (coordinate(), coordinate())
            factory.createLineString(Array(start, new Coordinate(end.x, start.y)))
        }
      }
      var apart = 0
      var rounded = 0
      (1 to 20000).foreach { _ =>
        // Coordinates from thousandths to tens of millions, and geometries of any extent
        val scale = math.pow(10, random.nextInt(11) - 3)
        val row = geometry(scale, scale * math.pow(10, -random.nextInt(8)))
        val query = geometry(scale, scale * math.pow(10, -random.nextInt(8)))
        val distance = row.distance(query)
        val box = row.getEnvelopeInternal
        if (box.distance(query.getEnvelopeInternal) > 0) apart += 1
        if (box.distance(query.getEnvelopeInternal) > distance) rounded += 1
        val columns = Map(
          "geom" -> GeometryFieldMetaData(
            "WKB",
            Seq.empty,
            Some(Seq(box.getMinX, box.getMinY, box.getMaxX, box.getMaxY))))
        val filter = DistanceLeafFilter("geom", query.getEnvelopeInternal, distance)
        assert(filter.evaluate(columns), s"$row $query $distance")
      }
      assert(apart > 10000)
      // The distance of some of these predicates is below the distance between the boxes
      assert(rounded > 0)
    }

    it("Leaves out a file that is farther away than the distance of the predicate") {
      withFile("SELECT id, ST_Point(0.3, 0.0) AS geom FROM range(3)") { data =>
        Seq(
          "ST_DWithin(geom, ST_Point(-2.0, 0.0), 2.299)",
          "ST_Distance(geom, ST_Point(-2.0, 0.0)) < 2.299").foreach { condition =>
          val filtered = data.where(condition)
          val bbox = Map(
            "geom" -> GeometryFieldMetaData("WKB", Seq("Point"), Some(Seq(0.3, 0.0, 0.3, 0.0))))
          assert(!getPushedDownSpatialFilter(filtered).get.evaluate(bbox), condition)
          assert(filtered.collect().isEmpty, condition)
        }
      }
    }

    it("Keeps a file with an empty geometry for a query that is equal to an empty geometry") {
      withFile("""SELECT id, CASE WHEN id = 0 THEN ST_GeomFromWKT('POINT EMPTY')
          |  ELSE ST_Point(CAST(id AS DOUBLE), CAST(id AS DOUBLE)) END AS geom
          |FROM range(10)""".stripMargin) { data =>
        val empty = "ST_GeomFromWKT('POINT EMPTY')"
        Seq(
          s"ST_Equals(geom, $empty)",
          s"ST_Equals($empty, geom)",
          s"ST_OrderingEquals(geom, $empty)",
          s"ST_Equals(geom, $empty) OR ST_Intersects(geom, ST_Point(9.0, 9.0))").foreach(
          compare(data, _))
      }
    }

    it("Pushes no filter for a distance that is no number, below zero or without an end") {
      withFile("SELECT id, ST_Point(0.3, 0.0) AS geom FROM range(3)") { data =>
        Seq(
          "ST_DWithin(geom, ST_Point(-2.0, 0.0), double('NaN'))",
          "ST_DWithin(geom, ST_Point(-2.0, 0.0), double('Infinity'))",
          "ST_DWithin(geom, ST_Point(-2.0, 0.0), -1.0)",
          "ST_Distance(geom, ST_Point(-2.0, 0.0)) <= double('Infinity')",
          "ST_Distance(geom, ST_Point(-2.0, 0.0)) < double('NaN')").foreach { condition =>
          val filtered = data.where(condition)
          assert(getPushedDownSpatialFilter(filtered).isEmpty, condition)
          val expected = withConf(Map(PUSH_DOWN -> "false")) {
            data.where(condition).select("id").collect().map(_.getLong(0)).sorted.toSeq
          }
          assert(
            filtered.select("id").collect().map(_.getLong(0)).sorted.toSeq == expected,
            condition)
        }
        assert(
          compare(data, "ST_Distance(geom, ST_Point(-2.0, 0.0)) <= double('Infinity')").isEmpty)
      }
    }

    it("Pushes no filter for a distance whose model is not a constant of the query") {
      // Points to the west of the antimeridian, some 22 centimetres from a point to its east
      withFile("""SELECT id, id % 2 = 0 AS spheroid, CAST(1 + id % 2 AS DOUBLE) AS radius,
          |  ST_Point(-179.999999, 0.0) AS geom
          |FROM range(6)""".stripMargin) { data =>
        val east = "ST_Point(179.999999, 0.0)"
        // On the spheroid for every second row, in meters, and on the plane for the others
        assert(compare(data, s"ST_DWithin(geom, $east, 1.0, spheroid)").isEmpty)
        // On a sphere of a radius of one or two meters
        assert(compare(data, s"ST_DistanceSphere(geom, $east, radius) <= 0.1").isEmpty)
        // In meters across the antimeridian
        assert(compare(data, s"ST_DWithin(geom, $east, 300.0, true)").isEmpty)
        assert(compare(data, s"ST_DistanceSpheroid(geom, $east) <= 300.0").isEmpty)
        assert(compare(data, s"ST_DistanceSphere(geom, $east) <= 300.0").isEmpty)
        // On the plane, by a constant
        assert(compare(data, "ST_DWithin(geom, ST_Point(-179.0, 0.0), 1.5, false)").isDefined)
      }
    }

    it("Keeps a file for a distance to the centroid of a geometry of the query") {
      // The centroid of the two points is (0 0), which is far from both of them
      withFile("SELECT id, ST_Point(0.0, 0.0) AS geom FROM range(3)") { data =>
        val ends = "ST_GeomFromWKT('MULTIPOINT ((-20 0), (20 0))')"
        assert(compare(data, s"ST_DistanceSpheroid(geom, $ends) <= 100.0").isEmpty)
        assert(compare(data, s"ST_DWithin(geom, $ends, 100.0, true)").isEmpty)
        assert(compare(data, s"ST_DistanceSphere(geom, $ends) <= 100.0").isEmpty)
      }
    }
  }

  private def setupBox2DCoveringFixture(): (DataFrame, String) = {
    val box2dParent =
      Files.createTempDirectory("sedona_geoparquet_box2d_").toFile.getAbsolutePath
    val box2dDir = box2dParent + "/data"
    val withBox = df.withColumn("geom_bbox", expr("ST_Box2D(geom)"))
    withBox.coalesce(1).write.partitionBy("region").format("geoparquet").save(box2dDir)
    val box2dDf = sparkSession.read.format("geoparquet").load(box2dDir)
    (box2dDf, box2dDir)
  }

  private def verifyBox2DFilter(box2dDf: DataFrame, condition: String): Unit = {
    val dfFiltered = box2dDf.where(condition)

    // Pushdown is attached and translates to a Parquet row-group filter.
    val pushed = getPushedDownSpatialFilter(dfFiltered)
    assert(pushed.isDefined, s"Expected spatial filter push-down for: $condition")
    assert(
      pushed.get.toParquetFilter.isDefined,
      s"Expected a Parquet FilterPredicate for: $condition")

    // Correctness: pushdown must not drop any matching rows. Compare against a run with the
    // spatial filter rule disabled (so no Parquet predicate is injected from Sedona).
    val expectedResult =
      withConf(Map("spark.sedona.geoparquet.spatialFilterPushDown" -> "false")) {
        box2dDf
          .where(condition)
          .orderBy("region", "id")
          .select("region", "id")
          .collect()
          .toSeq
      }
    val actualResult =
      dfFiltered.orderBy("region", "id").select("region", "id").collect().toSeq
    assert(expectedResult == actualResult, s"Result mismatch under push-down for: $condition")
  }

  /**
   * Test filter push down using specified query condition, and verify if the pushed down filter
   * prunes regions as expected. We'll also verify the correctness of query results.
   * @param condition
   *   SQL query condition
   * @param expectedPreservedRegions
   *   Regions that should be preserved after filter push down
   */
  private def testFilter(condition: String, expectedPreservedRegions: Seq[Int]): Unit = {
    val dfFiltered = geoParquetDf.where(condition)
    val preservedRegions = getPushedDownSpatialFilter(dfFiltered) match {
      case Some(spatialFilter) => resolvePreservedRegions(spatialFilter)
      case None => (0 until 4)
    }
    assert(expectedPreservedRegions == preservedRegions)
    val expectedResult =
      df.where(condition).orderBy("region", "id").select("region", "id").collect()
    val actualResult = dfFiltered.orderBy("region", "id").select("region", "id").collect()
    assert(expectedResult sameElements actualResult)
  }

  private def getPushedDownSpatialFilter(df: DataFrame): Option[GeoParquetSpatialFilter] = {
    val executedPlan = df.queryExecution.executedPlan
    val fileSourceScanExec = executedPlan.find(_.isInstanceOf[FileSourceScanExec])
    assert(fileSourceScanExec.isDefined)
    val fileFormat = fileSourceScanExec.get.asInstanceOf[FileSourceScanExec].relation.fileFormat
    assert(fileFormat.isInstanceOf[GeoParquetFileFormat])
    fileFormat.asInstanceOf[GeoParquetFileFormat].spatialFilter
  }

  private def resolvePreservedRegions(spatialFilter: GeoParquetSpatialFilter): Seq[Int] = {
    geoParquetMetaDataMap
      .filter { case (_, metaDataList) =>
        metaDataList.exists(metadata => spatialFilter.evaluate(metadata.columns))
      }
      .keys
      .toSeq
  }
}

object GeoParquetSpatialFilterPushDownSuite {
  case class TestDataItem(id: Int, region: Int, geom: Geometry)

  /**
   * Generate test data centered at (0, 0). The entire dataset was divided into 4 quadrants, each
   * with a unique region ID. The dataset contains 4 points and 4 polygons in each quadrant.
   * @param sparkSession
   *   SparkSession object
   * @return
   *   DataFrame containing test data
   */
  def generateTestData(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val regionCenters = Seq((-10, 10), (10, 10), (-10, -10), (10, -10))
    val testData = regionCenters.zipWithIndex.flatMap { case ((x, y), i) =>
      generateTestDataForRegion(i, x, y)
    }
    testData.toDF()
  }

  private def generateTestDataForRegion(region: Int, centerX: Double, centerY: Double) = {
    val factory = new GeometryFactory()
    val points = Seq(
      factory.createPoint(new Coordinate(centerX - 5, centerY + 5)),
      factory.createPoint(new Coordinate(centerX + 5, centerY + 5)),
      factory.createPoint(new Coordinate(centerX - 5, centerY - 5)),
      factory.createPoint(new Coordinate(centerX + 5, centerY - 5)))
    val polygons = points.map { p =>
      val envelope = p.getEnvelopeInternal
      envelope.expandBy(1)
      factory.toGeometry(envelope)
    }
    (points ++ polygons).zipWithIndex.map { case (g, i) => TestDataItem(i, region, g) }
  }

  /**
   * Write the test dataframe as GeoParquet files. Each region is written to a separate file.
   * We'll test spatial filter push down by examining which regions were preserved/pruned by
   * evaluating the pushed down spatial filters
   * @param testData
   *   dataframe containing test data
   * @param path
   *   path to write GeoParquet files
   */
  def writeTestDataAsGeoParquet(testData: DataFrame, path: String): Unit = {
    testData.coalesce(1).write.partitionBy("region").format("geoparquet").save(path)
  }

  /**
   * Load GeoParquet metadata for each region. Note that there could be multiple files for each
   * region, thus each region ID was associated with a list of GeoParquet metadata.
   * @param path
   *   path to directory containing GeoParquet files
   * @return
   *   Map of region ID to list of GeoParquet metadata
   */
  def readGeoParquetMetaDataMap(path: String): Map[Int, Seq[GeoParquetMetaData]] = {
    (0 until 4).map { k =>
      val geoParquetMetaDataSeq = readGeoParquetMetaDataByRegion(path, k)
      k -> geoParquetMetaDataSeq
    }.toMap
  }

  private def readGeoParquetMetaDataByRegion(
      geoParquetSavePath: String,
      region: Int): Seq[GeoParquetMetaData] = {
    val parquetFiles = new File(geoParquetSavePath + s"/region=$region")
      .listFiles()
      .filter(_.getName.endsWith(".parquet"))
    parquetFiles.flatMap { filePath =>
      val metadata = ParquetFileReader
        .open(HadoopInputFile.fromPath(new Path(filePath.getPath), new Configuration()))
        .getFooter
        .getFileMetaData
        .getKeyValueMetaData
      assert(metadata.containsKey("geo"))
      GeoParquetMetaData.parseKeyValueMetaData(metadata)
    }
  }
}
