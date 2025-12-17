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

import org.apache.spark.sql.DataFrame
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}

import scala.util.Random

class aggregateFunctionTestScala extends TestBaseScala {

  describe("Sedona-SQL Aggregate Function Test") {

    it("Passed ST_Envelope_aggr") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      var boundary =
        sparkSession.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")
      val coordinates: Array[Coordinate] = new Array[Coordinate](5)
      coordinates(0) = new Coordinate(1.1, 101.1)
      coordinates(1) = new Coordinate(1.1, 1100.1)
      coordinates(2) = new Coordinate(1000.1, 1100.1)
      coordinates(3) = new Coordinate(1000.1, 101.1)
      coordinates(4) = coordinates(0)
      val geometryFactory = new GeometryFactory()
      geometryFactory.createPolygon(coordinates)
      assert(boundary.take(1)(0).get(0) == geometryFactory.createPolygon(coordinates))
    }

    it("Passed ST_Union_aggr") {

      var polygonCsvDf = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(unionPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var union = sparkSession.sql("select ST_Union_Aggr(polygondf.polygonshape) from polygondf")
      assert(union.take(1)(0).get(0).asInstanceOf[Geometry].getArea == 10100)
    }

    it("Measured ST_Union_aggr wall time") {
      // number of random polygons to generate
      val numPolygons = 1000
      val df = createPolygonDataFrame(numPolygons)

      df.createOrReplaceTempView("geometry_table_for_measuring_union_aggr")

      // cache the table to eliminate the time of table scan
      df.cache()
      sparkSession
        .sql("select count(*) from geometry_table_for_measuring_union_aggr")
        .take(1)(0)
        .get(0)

      // measure time for optimized ST_Union_Aggr
      val startTimeOptimized = System.currentTimeMillis()
      val unionOptimized =
        sparkSession.sql(
          "SELECT ST_Union_Aggr(geom) AS union_geom FROM geometry_table_for_measuring_union_aggr")
      assert(unionOptimized.take(1)(0).get(0).asInstanceOf[Geometry].getArea > 0)
      val endTimeOptimized = System.currentTimeMillis()
      val durationOptimized = endTimeOptimized - startTimeOptimized

      assert(durationOptimized > 0, "Duration of optimized ST_Union_Aggr should be positive")

      // clear cache
      df.unpersist()
    }

    it("Passed ST_Intersection_aggr") {

      val twoPolygonsAsWktDf =
        sparkSession.read.textFile(intersectionPolygonInputLocation).toDF("polygon_wkt")
      twoPolygonsAsWktDf.createOrReplaceTempView("two_polygons_wkt")

      sparkSession
        .sql("select ST_GeomFromWKT(polygon_wkt) as polygon from two_polygons_wkt")
        .createOrReplaceTempView("two_polygons")

      val intersectionDF =
        sparkSession.sql("select ST_Intersection_Aggr(polygon) from two_polygons")

      assertResult(0.0034700160226227607)(
        intersectionDF.take(1)(0).get(0).asInstanceOf[Geometry].getArea)
    }

    it("Passed ST_Intersection_aggr no intersection gives empty polygon") {

      val twoPolygonsAsWktDf = sparkSession.read
        .textFile(intersectionPolygonNoIntersectionInputLocation)
        .toDF("polygon_wkt")
      twoPolygonsAsWktDf.createOrReplaceTempView("two_polygons_no_intersection_wkt")

      sparkSession
        .sql(
          "select ST_GeomFromWKT(polygon_wkt) as polygon from two_polygons_no_intersection_wkt")
        .createOrReplaceTempView("two_polygons_no_intersection")

      val intersectionDF =
        sparkSession.sql("select ST_Intersection_Aggr(polygon) from two_polygons_no_intersection")

      assertResult(0.0)(intersectionDF.take(1)(0).get(0).asInstanceOf[Geometry].getArea)
    }

    it("Passed ST_Collect_Agg with points") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POINT(1 2)'),
          |  ST_GeomFromWKT('POINT(3 4)'),
          |  ST_GeomFromWKT('POINT(5 6)')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("points_table")

      val collectDF = sparkSession.sql("SELECT ST_Collect_Agg(geom) FROM points_table")
      val result = collectDF.take(1)(0).get(0).asInstanceOf[Geometry]

      assert(result.getGeometryType == "MultiPoint")
      assert(result.getNumGeometries == 3)
    }

    it("Passed ST_Collect_Agg with polygons") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
          |  ST_GeomFromWKT('POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("polygons_table")

      val collectDF = sparkSession.sql("SELECT ST_Collect_Agg(geom) FROM polygons_table")
      val result = collectDF.take(1)(0).get(0).asInstanceOf[Geometry]

      assert(result.getGeometryType == "MultiPolygon")
      assert(result.getNumGeometries == 2)
      // Total area should be 2 (each polygon has area 1)
      assert(result.getArea == 2.0)
    }

    it("Passed ST_Collect_Agg with mixed geometry types") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POINT(1 2)'),
          |  ST_GeomFromWKT('LINESTRING(0 0, 1 1)'),
          |  ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("mixed_geom_table")

      val collectDF = sparkSession.sql("SELECT ST_Collect_Agg(geom) FROM mixed_geom_table")
      val result = collectDF.take(1)(0).get(0).asInstanceOf[Geometry]

      assert(result.getGeometryType == "GeometryCollection")
      assert(result.getNumGeometries == 3)
    }

    it("Passed ST_Collect_Agg with GROUP BY") {
      sparkSession
        .sql("""
          |SELECT * FROM (VALUES
          |  (1, ST_GeomFromWKT('POINT(1 2)')),
          |  (1, ST_GeomFromWKT('POINT(3 4)')),
          |  (2, ST_GeomFromWKT('POINT(5 6)')),
          |  (2, ST_GeomFromWKT('POINT(7 8)')),
          |  (2, ST_GeomFromWKT('POINT(9 10)'))
          |) AS t(group_id, geom)
        """.stripMargin)
        .createOrReplaceTempView("grouped_points_table")

      val collectDF = sparkSession.sql(
        "SELECT group_id, ST_Collect_Agg(geom) as collected FROM grouped_points_table GROUP BY group_id ORDER BY group_id")
      val results = collectDF.collect()

      // Group 1 should have 2 points
      assert(results(0).getAs[Geometry]("collected").getNumGeometries == 2)
      // Group 2 should have 3 points
      assert(results(1).getAs[Geometry]("collected").getNumGeometries == 3)
    }

    it("Passed ST_Collect_Agg preserves duplicates unlike ST_Union_Aggr") {
      // Test that ST_Collect_Agg keeps duplicate geometries (unlike ST_Union_Aggr which merges them)
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
          |  ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("duplicate_polygons_table")

      val collectDF =
        sparkSession.sql("SELECT ST_Collect_Agg(geom) FROM duplicate_polygons_table")
      val result = collectDF.take(1)(0).get(0).asInstanceOf[Geometry]

      // ST_Collect_Agg should preserve both polygons
      assert(result.getNumGeometries == 2)
      // Area should be 2 because it doesn't merge overlapping areas
      assert(result.getArea == 2.0)
    }

    it("Passed ST_Collect_Agg with null values") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POINT(1 2)'),
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT('POINT(3 4)')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("points_with_null_table")

      val collectDF = sparkSession.sql("SELECT ST_Collect_Agg(geom) FROM points_with_null_table")
      val result = collectDF.take(1)(0).get(0).asInstanceOf[Geometry]

      // Should only have 2 points (nulls are skipped)
      assert(result.getNumGeometries == 2)
    }

    it("ST_Union_Aggr should handle null values") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT('POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("polygons_with_null_for_union")

      val unionDF =
        sparkSession.sql("SELECT ST_Union_Aggr(geom) FROM polygons_with_null_for_union")
      val result = unionDF.take(1)(0).get(0).asInstanceOf[Geometry]

      // Should union the 2 non-null polygons (total area = 2.0)
      assert(result.getArea == 2.0)
    }

    it("ST_Envelope_Aggr should handle null values") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POINT(1 2)'),
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT('POINT(3 4)')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("points_with_null_for_envelope")

      val envelopeDF =
        sparkSession.sql("SELECT ST_Envelope_Aggr(geom) FROM points_with_null_for_envelope")
      val result = envelopeDF.take(1)(0).get(0).asInstanceOf[Geometry]

      // Should create envelope from the 2 non-null points
      assert(result.getGeometryType == "Polygon")
      val envelope = result.getEnvelopeInternal
      assert(envelope.getMinX == 1.0)
      assert(envelope.getMinY == 2.0)
      assert(envelope.getMaxX == 3.0)
      assert(envelope.getMaxY == 4.0)
    }

    it("ST_Intersection_Aggr should handle null values") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'),
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT('POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("polygons_with_null_for_intersection")

      val intersectionDF = sparkSession.sql(
        "SELECT ST_Intersection_Aggr(geom) FROM polygons_with_null_for_intersection")
      val result = intersectionDF.take(1)(0).get(0).asInstanceOf[Geometry]

      // Should intersect the 2 non-null polygons (intersection area = 4.0)
      assert(result.getArea == 4.0)
    }

    it("ST_Union_Aggr should return null if all inputs are null") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT(NULL)
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("all_null_union")

      val unionDF = sparkSession.sql("SELECT ST_Union_Aggr(geom) FROM all_null_union")
      val result = unionDF.take(1)(0).get(0)

      assert(result == null)
    }

    it("ST_Envelope_Aggr should return null if all inputs are null") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT(NULL)
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("all_null_envelope")

      val envelopeDF = sparkSession.sql("SELECT ST_Envelope_Aggr(geom) FROM all_null_envelope")
      val result = envelopeDF.take(1)(0).get(0)

      assert(result == null)
    }

    it("ST_Intersection_Aggr should return null if all inputs are null") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT(NULL)
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("all_null_intersection")

      val intersectionDF =
        sparkSession.sql("SELECT ST_Intersection_Aggr(geom) FROM all_null_intersection")
      val result = intersectionDF.take(1)(0).get(0)

      assert(result == null)
    }

    it("ST_Collect_Agg should return null if all inputs are null") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  ST_GeomFromWKT(NULL),
          |  ST_GeomFromWKT(NULL)
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("all_null_collect")

      val collectDF = sparkSession.sql("SELECT ST_Collect_Agg(geom) FROM all_null_collect")
      val result = collectDF.take(1)(0).get(0)

      assert(result == null)
    }

    it(
      "ST_Envelope_Aggr should return empty geometry if inputs are mixed with null and empty geometries") {
      sparkSession
        .sql("""
          |SELECT explode(array(
          |  NULL,
          |  NULL,
          |  ST_GeomFromWKT('POINT EMPTY'),
          |  NULL,
          |  ST_GeomFromWKT('POLYGON EMPTY')
          |)) AS geom
        """.stripMargin)
        .createOrReplaceTempView("mixed_null_empty_envelope")

      val envelopeDF =
        sparkSession.sql("SELECT ST_Envelope_Aggr(geom) FROM mixed_null_empty_envelope")
      val result = envelopeDF.take(1)(0).get(0)

      assert(result != null)
      assert(result.asInstanceOf[Geometry].isEmpty)
    }
  }

  def generateRandomPolygon(index: Int): String = {
    val random = new Random()
    val x = random.nextDouble() * index
    val y = random.nextDouble() * index
    s"POLYGON (($x $y, ${x + 1} $y, ${x + 1} ${y + 1}, $x ${y + 1}, $x $y))"
  }

  def createPolygonDataFrame(numPolygons: Int): DataFrame = {
    val polygons = (1 to numPolygons).map(generateRandomPolygon).toArray
    val polygonArray = polygons.map(polygon => s"ST_GeomFromWKT('$polygon')")
    val polygonArrayStr = polygonArray.mkString(", ")

    val sqlQuery =
      s"""
         |SELECT explode(array($polygonArrayStr)) AS geom
     """.stripMargin

    sparkSession.sql(sqlQuery)
  }
}
