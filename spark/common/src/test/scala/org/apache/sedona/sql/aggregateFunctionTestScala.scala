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
