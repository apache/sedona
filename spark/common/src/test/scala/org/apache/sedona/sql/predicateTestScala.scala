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

import org.apache.sedona.common.geometrySerde.GeometrySerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Literal}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.{ST_Contains, ST_CoveredBy, ST_Covers, ST_Crosses, ST_DWithin, ST_Disjoint, ST_Equals, ST_EqualsExact, ST_EqualsIdentical, ST_GeomFromWKT, ST_Intersects, ST_OrderingEquals, ST_Overlaps, ST_Point, ST_Touches, ST_Within}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{CALLS_REAL_METHODS, mockStatic, never, times}

import java.util.Arrays

class predicateTestScala extends TestBaseScala {

  describe("Sedona-SQL Predicate Test") {

    it("Passed ST_Intersects and ST_Contains on Box2D inputs") {
      val df = sparkSession.sql("""
        WITH t AS (
          SELECT
            ST_Box2D(ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'))   AS a,
            ST_Box2D(ST_GeomFromText('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'))   AS inside,
            ST_Box2D(ST_GeomFromText('POLYGON((3 3, 3 7, 7 7, 7 3, 3 3))'))   AS overlap,
            ST_Box2D(ST_GeomFromText('POLYGON((5 0, 5 5, 10 5, 10 0, 5 0))')) AS edge,
            ST_Box2D(ST_GeomFromText('POLYGON((6 6, 6 7, 7 7, 7 6, 6 6))'))   AS disjoint,
            ST_Box2D(ST_GeomFromText(NULL))                                    AS box_null
        )
        SELECT
          ST_Intersects(a, inside),
          ST_Intersects(a, overlap),
          ST_Intersects(a, edge),
          ST_Intersects(a, disjoint),
          ST_Intersects(a, box_null),
          ST_Contains(a, inside),
          ST_Contains(a, overlap),
          ST_Contains(a, a),
          ST_Contains(a, box_null)
        FROM t
        """)
      val row = df.collect()(0)
      assert(row.getBoolean(0)) // intersects: inside
      assert(row.getBoolean(1)) // intersects: overlap
      assert(row.getBoolean(2)) // intersects: edge-touch
      assert(!row.getBoolean(3)) // intersects: disjoint
      assert(row.isNullAt(4)) // intersects: NULL propagates
      assert(row.getBoolean(5)) // contains: inside
      assert(!row.getBoolean(6)) // contains: overlap (extends past)
      assert(row.getBoolean(7)) // contains: equal
      assert(row.isNullAt(8)) // contains: NULL propagates
    }

    it("Passed ST_Contains") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql(
        "select * from pointdf where ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Intersects") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql(
        "select * from pointdf where ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Within") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql(
        "select * from pointdf where ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))")
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Covers") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql(
        "select * from pointdf where ST_Covers(ST_PolygonFromEnvelope(1.0,100.0,101.0,201.0), pointdf.arealandmark)")
      assert(resultDf.count() == 100)
    }
    it("Passed ST_CoveredBy") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql(
        "select * from pointdf where ST_CoveredBy(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,101.0,201.0))")
      assert(resultDf.count() == 100)
    }

    it("Passed ST_Equals for ST_Point") {
      // Select a point from the table and check if any point in the table is equal to the selected point.

      // Read csv to get the points table
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPoint1InputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")

      // Convert the pointtable to pointdf using ST_Point
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as point from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var equaldf = sparkSession.sql(
        "select * from pointdf where ST_Equals(pointdf.point, ST_Point(100.1, 200.1)) ")

      assert(equaldf.count() == 5, s"Expected 5 value but got ${equaldf.count()}")

    }

    it("Passed ST_Equals for ST_Polygon") {

      // Select a polygon from the table and check if any polygon in the table is equal to the selected polygon.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      // Selected polygon is Polygon (100.01,200.01,100.5,200.5)
      var equaldf1 = sparkSession.sql(
        "select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromEnvelope(100.01,200.01,100.5,200.5)) ")

      assert(equaldf1.count() == 5, s"Expected 5 value but got ${equaldf1.count()}")

      // Change the order of the polygon points (100.5,200.5,100.01,200.01)
      var equaldf2 = sparkSession.sql(
        "select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromEnvelope(100.5,200.5,100.01,200.01)) ")

      assert(equaldf2.count() == 5, s"Expected 5 value but got ${equaldf2.count()}")

    }

    it("Passed ST_Equals for ST_Point and ST_Polygon") {

      // Test a Point against any polygon in the table for equality.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope and cast
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      // Selected point is Point (91.01,191.01)
      var equaldf = sparkSession.sql(
        "select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_Point(91.01,191.01)) ")

      assert(equaldf.count() == 0, s"Expected 0 value but got ${equaldf.count()}")

    }

    it("Passed ST_Equals for ST_LineString and ST_Polygon") {

      // Test a LineString against any polygon in the table for equality.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope and cast
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      /* Selected LineString is ST_LineStringFromText - (100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01)
       * It forms the boundary of the polygon Polygon(100.01,200.01,100.5,200.5)
       * x1 = 100.01, y1 = 200.01, x2 = 100.5, y2 = 200.5
       * LineString(P1, P2, P3, P4) -
       * P1->100.01,200.01
       * P2->100.5,200.01
       * P3->100.5,200.5
       * P4->100.01,200.5
       * P5->100.01,200.01
       */
      val string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"

      var equaldf = sparkSession.sql(
        s"select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_LineStringFromText(\'$string\', \',\')) ")

      assert(equaldf.count() == 0, s"Expected 0 value but got ${equaldf.count()}")

    }
    it("Passed ST_Equals for ST_PolygonFromEnvelope and ST_PolygonFromText") {

      // Test a Polygon formed using ST_PolygonFromText against any polygon in the table formed using ST_PolygonFromEnvelope for equality.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope and cast
      var polygonDf = sparkSession.sql(
        "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      // Selected Polygon is ST_PolygonFromText - Polygon(100.01,200.01,100.5,200.5) formed using ST_PolygonFromText.
      val string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"

      var equaldf = sparkSession.sql(
        s"select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromText(\'$string\', \',\')) ")

      assert(equaldf.count() == 5, s"Expected 5 value but got ${equaldf.count()}")
    }

    it("Passed ST_Crosses") {
      var crossesTesttable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('LINESTRING(1 5, 5 1)') as b")
      crossesTesttable.createOrReplaceTempView("crossesTesttable")
      var crosses = sparkSession.sql("select(ST_Crosses(a, b)) from crossesTesttable")

      var notCrossesTesttable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))') as b")
      notCrossesTesttable.createOrReplaceTempView("notCrossesTesttable")
      var notCrosses = sparkSession.sql("select(ST_Crosses(a, b)) from notCrossesTesttable")

      assert(crosses.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notCrosses.take(1)(0).get(0).asInstanceOf[Boolean])
    }

    it("Passed ST_Crosses with GeometryCollections") {
      val cases = Seq(
        ("GEOMETRYCOLLECTION (POINT (1 1))", "LINESTRING (1 1, 1 2)", false),
        (
          "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (0 2, 2 0))",
          "LINESTRING (0 0, 2 2)",
          true),
        ("GEOMETRYCOLLECTION EMPTY", "LINESTRING (0 0, 2 2)", false))
      cases.foreach { case (left, right, expected) =>
        val result = sparkSession
          .sql(s"SELECT ST_GeomFromWKT('$left') AS a, ST_GeomFromWKT('$right') AS b")
          .selectExpr("ST_Crosses(a, b)", "ST_Crosses(b, a)")
          .first()
        assert(result.getBoolean(0) == expected)
        assert(result.getBoolean(1) == expected)
      }
      val nullResult = sparkSession
        .sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION (POINT (1 1))') AS geom")
        .selectExpr("ST_Crosses(geom, NULL)", "ST_Crosses(NULL, geom)")
        .first()
      assert(nullResult.isNullAt(0) && nullResult.isNullAt(1))
    }

    it("Passed ST_Relate") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (1 1, 5 5)') AS g1, ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))') as g2, '1010F0212' as im")
      val actual = baseDf.selectExpr("ST_Relate(g1, g2)").first().get(0)
      assert(actual.equals("1010F0212"))

      val actualBoolean = baseDf.selectExpr("ST_Relate(g1, g2, im)").first().getBoolean(0)
      assert(actualBoolean)
    }

    it("Passed ST_RelateMatch") {
      val actual =
        sparkSession.sql("SELECT ST_RelateMatch('101202FFF', 'TTTTTTFFF')").first().getBoolean(0)
      assert(actual)
    }

    it("Passed ST_Touches") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql(
        "select * from pointdf where ST_Touches(pointdf.arealandmark, ST_PolygonFromEnvelope(0.0,99.0,1.1,101.1))")
      assert(resultDf.count() == 1)
    }
    it("Passed ST_Overlaps") {
      var testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((2.5 2.5, 2.5 4.5, 4.5 4.5, 4.5 2.5, 2.5 2.5))') as a,ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))') as b, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as c, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as d")
      testtable.createOrReplaceTempView("testtable")
      var overlaps = sparkSession.sql("select ST_Overlaps(a,b) from testtable")
      var notoverlaps = sparkSession.sql("select ST_Overlaps(c,d) from testtable")
      assert(overlaps.take(1)(0).get(0) == true)
      assert(notoverlaps.take(1)(0).get(0) == false)
    }
    it("Passed ST_Disjoint") {
      var testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))') as a,ST_GeomFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))') as b, ST_GeomFromWKT('POLYGON((1 9, 6 6, 6 4, 1 2,1 9))') as c, ST_GeomFromWKT('POLYGON((2 5, 4 5, 4 1, 2 1, 2 5))') as d")
      testtable.createOrReplaceTempView("testtable")
      var disjoint = sparkSession.sql("select ST_Disjoint(a,b) from testtable")
      var notdisjoint = sparkSession.sql("select ST_Disjoint(c,d) from testtable")
      assert(disjoint.take(1)(0).get(0) == true)
      assert(notdisjoint.take(1)(0).get(0) == false)
    }

    it("Passed ST_OrderingEquals") {
      var testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') as a, ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') as b, ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 0 -2, 2 0))') as c, ST_GeomFromWKT('POLYGON((0 2, -2 0, 2 0, 0 2))') as d")
      testtable.createOrReplaceTempView("testtable")
      var orderEquals = sparkSession.sql("select ST_OrderingEquals(a,b) from testtable")
      var notOrderEqualsDiffGeom =
        sparkSession.sql("select ST_OrderingEquals(a,c) from testtable")
      var notOrderEqualsDiffOrder =
        sparkSession.sql("select ST_OrderingEquals(a,d) from testtable")
      assert(orderEquals.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notOrderEqualsDiffGeom.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notOrderEqualsDiffOrder.take(1)(0).get(0).asInstanceOf[Boolean])
    }

    it("Passed ST_EqualsExact") {
      val result = sparkSession
        .sql("""
          SELECT
            ST_EqualsExact(ST_Point(0.0, 0.0), ST_Point(0.03, 0.04), 0.051),
            ST_EqualsExact(ST_Point(0.0, 0.0), ST_Point(0.03, 0.04), 0.049),
            ST_EqualsExact(
              ST_GeomFromWKT('LINESTRING(0 0, 1 1)'),
              ST_GeomFromWKT('LINESTRING(1 1, 0 0)'),
              0.0),
            ST_EqualsExact(NULL, ST_Point(0.0, 0.0), 0.0)
        """)
        .first()
      assert(result.getBoolean(0))
      assert(!result.getBoolean(1))
      assert(!result.getBoolean(2))
      assert(result.isNullAt(3))
    }

    it("Passed ST_EqualsIdentical") {
      val result = sparkSession
        .sql("""
          SELECT
            ST_EqualsIdentical(
              ST_GeomFromWKT('POINT Z (1 2 3)'),
              ST_GeomFromWKT('POINT Z (1 2 3)')),
            ST_EqualsIdentical(
              ST_GeomFromWKT('POINT Z (1 2 3)'),
              ST_GeomFromWKT('POINT Z (1 2 4)')),
            ST_EqualsIdentical(
              ST_GeomFromWKT('POINT Z (1 2 3)'),
              ST_GeomFromWKT('POINT M (1 2 3)')),
            ST_EqualsIdentical(
              ST_GeomFromWKT('GEOMETRYCOLLECTION (POINT (0 0), POINT (1 1))'),
              ST_GeomFromWKT('GEOMETRYCOLLECTION (POINT (1 1), POINT (0 0))')),
            ST_EqualsIdentical(NULL, ST_Point(0.0, 0.0))
        """)
        .first()
      assert(result.getBoolean(0))
      assert(!result.getBoolean(1))
      assert(!result.getBoolean(2))
      assert(!result.getBoolean(3))
      assert(result.isNullAt(4))
    }

    it("Passed ST_EqualsIdentical on stored columns and nested constructors") {
      import sparkSession.implicits._
      val line = "LINESTRING (0 0, 1 1, 2 2, 3 3, 4 4)"
      val lineZM = "LINESTRING ZM (0 0 0 0, 1 1 1 1, 2 2 2 2, 3 3 3 3, 4 4 4 4)"
      val polygon =
        "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (5 5, 5 6, 6 6, 6 5, 5 5))"
      val multiPolygon = "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 1 1)))"
      val collection =
        s"GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1), $multiPolygon))"
      // (left WKT, left SRID, right WKT, right SRID, expected result)
      val cases: Seq[(String, Int, String, Int, Option[Boolean])] = Seq(
        // Byte-identical inputs in every dimension layout
        (line, 0, line, 0, Some(true)),
        ("LINESTRING Z (0 0 1, 1 1 2)", 0, "LINESTRING Z (0 0 1, 1 1 2)", 0, Some(true)),
        ("LINESTRING M (0 0 1, 1 1 2)", 0, "LINESTRING M (0 0 1, 1 1 2)", 0, Some(true)),
        (lineZM, 4326, lineZM, 4326, Some(true)),
        ("POINT EMPTY", 0, "POINT EMPTY", 0, Some(true)),
        // Identical geometries with different SRIDs
        ("POINT (1 2)", 4326, "POINT (1 2)", 3857, Some(true)),
        (lineZM, 4326, lineZM, 0, Some(true)),
        (polygon, 4326, polygon, 3857, Some(true)),
        (collection, 0, collection, 4326, Some(true)),
        // NaN ordinates and signed zeros
        ("LINESTRING Z (0 0 1, 1 1 NaN)", 0, "LINESTRING Z (0 0 1, 1 1 NaN)", 0, Some(true)),
        ("LINESTRING Z (0 0 1, 1 1 NaN)", 0, "LINESTRING Z (0 0 1, 1 1 2)", 0, Some(false)),
        ("POINT ZM (-0 0 -0 1)", 0, "POINT ZM (0 -0 0 1)", 0, Some(true)),
        // First, middle, and last coordinate mismatches
        (line, 0, "LINESTRING (9 0, 1 1, 2 2, 3 3, 4 4)", 0, Some(false)),
        (line, 0, "LINESTRING (0 0, 1 1, 2 9, 3 3, 4 4)", 0, Some(false)),
        (line, 0, "LINESTRING (0 0, 1 1, 2 2, 3 3, 4 9)", 0, Some(false)),
        (
          lineZM,
          0,
          "LINESTRING ZM (9 0 0 0, 1 1 1 1, 2 2 2 2, 3 3 3 3, 4 4 4 4)",
          0,
          Some(false)),
        (
          lineZM,
          0,
          "LINESTRING ZM (0 0 0 0, 1 1 1 1, 2 2 9 2, 3 3 3 3, 4 4 4 4)",
          0,
          Some(false)),
        (
          lineZM,
          0,
          "LINESTRING ZM (0 0 0 0, 1 1 1 1, 2 2 2 2, 3 3 3 3, 4 4 4 9)",
          0,
          Some(false)),
        // Polygons with holes
        (polygon, 0, polygon, 0, Some(true)),
        (
          polygon,
          0,
          "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 6, 6 6, 6 5, 5 5), (1 1, 1 2, 2 2, 2 1, 1 1))",
          0,
          Some(false)),
        (
          polygon,
          0,
          "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (5 5, 5 6, 6 7, 6 5, 5 5))",
          0,
          Some(false)),
        (polygon, 0, "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", 0, Some(false)),
        // Nested collections
        (collection, 0, collection, 0, Some(true)),
        (
          collection,
          0,
          s"GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION ($multiPolygon, LINESTRING (0 0, 1 1)))",
          0,
          Some(false)),
        (
          collection,
          0,
          "GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1), " +
            "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 2 2, 1 1)))))",
          0,
          Some(false)),
        (
          collection,
          0,
          s"GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1), $multiPolygon)",
          0,
          Some(false)),
        // Dimension layouts must match exactly
        ("POINT Z (1 2 3)", 0, "POINT M (1 2 3)", 0, Some(false)),
        ("POINT (1 2)", 0, "POINT Z (1 2 0)", 0, Some(false)),
        ("LINESTRING Z (0 0 1, 1 1 2)", 0, "LINESTRING ZM (0 0 1 0, 1 1 2 0)", 0, Some(false)),
        ("POINT EMPTY", 0, "LINESTRING EMPTY", 0, Some(false)),
        // NULL inputs
        (null, 0, "POINT (1 2)", 0, None),
        ("POINT (1 2)", 0, null, 0, None),
        (null, 0, null, 0, None))

      val stored = cases.zipWithIndex
        .map { case ((lw, ls, rw, rs, _), id) => (id, lw, ls, rw, rs) }
        .toDF("id", "lw", "ls", "rw", "rs")
        .selectExpr(
          "id",
          "lw",
          "ls",
          "rw",
          "rs",
          "ST_GeomFromWKT(lw, ls) AS a",
          "ST_GeomFromWKT(rw, rs) AS b")
        .cache()
      try {
        val rows = stored
          .selectExpr(
            "id",
            "ST_EqualsIdentical(a, b)",
            "ST_EqualsIdentical(ST_GeomFromWKT(lw, ls), ST_GeomFromWKT(rw, rs))")
          .collect()
        assert(rows.length == cases.length)
        rows.foreach { row =>
          val id = row.getInt(0)
          (1 until row.length).foreach { i =>
            val actual = if (row.isNullAt(i)) None else Some(row.getBoolean(i))
            assert(actual == cases(id)._5, s"column $i of case ${cases(id)}")
          }
        }
      } finally {
        stored.unpersist()
      }
    }

    it("Passed ST_EqualsIdentical on identical geometries with different serialized bytes") {
      val factory = new GeometryFactory()
      val payloadNaN = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
      def lineZM(last: Coordinate, srid: Int = 0): Geometry = {
        val geom = factory.createLineString(
          Array(new CoordinateXYZM(0, 0, 0, 0), new CoordinateXYZM(1, 1, 1, 1), last))
        geom.setSRID(srid)
        geom
      }
      def wkt(text: String, srid: Int): Geometry = {
        val geom = new WKTReader().read(text)
        geom.setSRID(srid)
        geom
      }
      val polygon = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))"
      val collection = s"GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION ($polygon))"
      // (left, right, expected result); every pair is encoded with different bytes
      val cases: Seq[(Geometry, Geometry, Boolean)] = Seq(
        (
          lineZM(new CoordinateXYZM(2, 2, Double.NaN, Double.NaN)),
          lineZM(new CoordinateXYZM(2, 2, payloadNaN, payloadNaN)),
          true),
        (
          lineZM(new CoordinateXYZM(0.0, 0.0, 0.0, 0.0)),
          lineZM(new CoordinateXYZM(-0.0, -0.0, -0.0, -0.0)),
          true),
        (
          lineZM(new CoordinateXYZM(2, 2, 2, 2), 4326),
          lineZM(new CoordinateXYZM(2, 2, 2, 2), 3857),
          true),
        (wkt(polygon, 4326), wkt(polygon, 0), true),
        (wkt(collection, 0), wkt(collection, 4326), true),
        (
          lineZM(new CoordinateXYZM(2, 2, 2, payloadNaN)),
          lineZM(new CoordinateXYZM(2, 2, 2, 2)),
          false))
      cases.foreach { case (left, right, _) =>
        assert(
          !Arrays.equals(GeometrySerializer.serialize(left), GeometrySerializer.serialize(right)))
      }

      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("a", GeometryUDT()),
          StructField("b", GeometryUDT())))
      val rows = cases.zipWithIndex.map { case ((left, right, _), id) => Row(id, left, right) }
      val df = sparkSession.createDataFrame(Arrays.asList(rows: _*), schema)
      val results = df.selectExpr("id", "ST_EqualsIdentical(a, b)").collect()
      assert(results.length == cases.length)
      results.foreach { row =>
        assert(row.getBoolean(1) == cases(row.getInt(0))._3, s"case ${row.getInt(0)}")
      }
    }

    it("ST_EqualsIdentical skips deserialization only for byte-identical serialized inputs") {
      val factory = new GeometryFactory()
      val coordinates =
        Array[Coordinate](new CoordinateXYZM(0, 0, 1, 2), new CoordinateXYZM(1, 1, 3, 4))
      val bytes = GeometrySerializer.serialize(factory.createLineString(coordinates))
      val lineWithSrid = factory.createLineString(coordinates)
      lineWithSrid.setSRID(4326)
      val bytesWithSrid = GeometrySerializer.serialize(lineWithSrid)
      def stored(value: Array[Byte]) = Literal.create(value, GeometryUDT())
      val constructed =
        ST_GeomFromWKT(Seq(Literal("LINESTRING ZM (0 0 1 2, 1 1 3 4)"), Literal(0)))

      val mocked = mockStatic(classOf[GeometrySerializer], CALLS_REAL_METHODS)
      try {
        assert(
          ST_EqualsIdentical(Seq(stored(bytes), stored(bytes.clone()))).eval(EmptyRow) == true)
        mocked.verify(() => GeometrySerializer.deserialize(any(classOf[Array[Byte]])), never())

        assert(
          ST_EqualsIdentical(Seq(stored(bytes), stored(bytesWithSrid))).eval(EmptyRow) == true)
        mocked.verify(() => GeometrySerializer.deserialize(any(classOf[Array[Byte]])), times(2))

        assert(ST_EqualsIdentical(Seq(stored(bytes), stored(null))).eval(EmptyRow) == null)
        assert(ST_EqualsIdentical(Seq(stored(null), stored(bytes))).eval(EmptyRow) == null)
        assert(ST_EqualsIdentical(Seq(stored(null), stored(null))).eval(EmptyRow) == null)

        // Nested constructors are compared without serializing their results
        mocked.clearInvocations()
        assert(ST_EqualsIdentical(Seq(constructed, constructed)).eval(EmptyRow) == true)
        assert(ST_EqualsIdentical(Seq(stored(bytes), constructed)).eval(EmptyRow) == true)
        mocked.verify(() => GeometrySerializer.serialize(any(classOf[Geometry])), never())
        mocked.verify(() => GeometrySerializer.deserialize(any(classOf[Array[Byte]])), times(1))
      } finally {
        mocked.close()
      }
    }

    it("Passed edge cases of ST_Contains and ST_Covers") {
      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') AS a, ST_GeomFromWKT('POINT(2 0)') AS b")
      testtable.createOrReplaceTempView("testtable")
      val contains = sparkSession.sql("select ST_Contains(a, b) from testtable").take(1)(0)
      val covers = sparkSession.sql("select ST_Covers(a, b) from testtable").take(1)(0)
      assert(!contains.get(0).asInstanceOf[Boolean])
      assert(covers.get(0).asInstanceOf[Boolean])
    }

    it("Passed edge cases of ST_Within and ST_CoveredBy") {
      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') AS a, ST_GeomFromWKT('POINT(2 0)') AS b")
      testtable.createOrReplaceTempView("testtable")
      val within = sparkSession.sql("select ST_Within(b, a) from testtable").take(1)(0)
      val coveredBy = sparkSession.sql("select ST_CoveredBy(b, a) from testtable").take(1)(0)
      assert(!within.get(0).asInstanceOf[Boolean])
      assert(coveredBy.get(0).asInstanceOf[Boolean])
    }

    it("Passed ST_DWithin") {
      val testTable = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (0 0)') as origin, ST_GeomFromWKT('POINT (0 4.1)') as p1")
      testTable.createOrReplaceTempView("testTable")
      val actual = sparkSession
        .sql("SELECT ST_DWithin(origin, p1, 4.2) from testTable")
        .first()(0)
        .asInstanceOf[java.lang.Boolean]
      assert(actual)
    }

    it("Passed ST_DWithin with useSphere") {
      val testTable = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (-122.335167 47.608013)') as seattle, ST_GeomFromWKT('POINT (-73.935242 40.730610)') as ny")
      testTable.createOrReplaceTempView("testTable")
      val actual = sparkSession
        .sql("SELECT ST_DWithin(seattle, ny, 4000000, true) from testTable")
        .first()(0)
        .asInstanceOf[java.lang.Boolean]
      assert(actual)
    }

    it("Passed ST_DWithin with empty geometries") {
      val row = sparkSession
        .sql("""SELECT
            |ST_DWithin(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POLYGON EMPTY'), 1000.0, true),
            |ST_DWithin(ST_GeomFromWKT('POLYGON EMPTY'), ST_GeomFromWKT('POINT (0 0)'), 1000.0, true),
            |ST_DWithin(ST_GeomFromWKT('POINT EMPTY'), ST_GeomFromWKT('POINT EMPTY'), 1000.0, true),
            |ST_DWithin(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POLYGON EMPTY'), 1000.0),
            |ST_DWithin(ST_GeomFromWKT('POINT (-0.5 -0.5)'), ST_GeomFromWKT('POINT EMPTY'), 1.0, false)
            |""".stripMargin)
        .first()
      (0 until row.length).foreach(i => assert(!row.getBoolean(i), s"column $i"))
    }

    Seq(
      ST_Contains,
      ST_Intersects,
      ST_Within,
      ST_Covers,
      ST_CoveredBy,
      ST_Crosses,
      ST_Overlaps,
      ST_Touches,
      ST_Equals,
      ST_EqualsIdentical,
      ST_Disjoint,
      ST_OrderingEquals).foreach { predicate =>
      it(s"Passed null handling in $predicate") {
        val point =
          ST_Point(Literal.create(0.0) :: Literal.create(0.0) :: Literal.create(0.0) :: Nil)
        val missing = Literal.create(null)

        assert(predicate(point :: point :: Nil).eval(EmptyRow) != null)
        assert(predicate(point :: missing :: Nil).eval(EmptyRow) == null)
        assert(predicate(missing :: point :: Nil).eval(EmptyRow) == null)
        assert(predicate(missing :: missing :: Nil).eval(EmptyRow) == null)
      }
    }

    it("Passed null handling in ST_EqualsExact") {
      val point =
        ST_Point(Literal.create(0.0) :: Literal.create(0.0) :: Literal.create(0.0) :: Nil)
      val missing = Literal.create(null)
      val tolerance = Literal.create(0.0)

      assert(ST_EqualsExact(point :: point :: tolerance :: Nil).eval(EmptyRow) != null)
      assert(ST_EqualsExact(point :: missing :: tolerance :: Nil).eval(EmptyRow) == null)
      assert(ST_EqualsExact(missing :: point :: tolerance :: Nil).eval(EmptyRow) == null)
      assert(ST_EqualsExact(point :: point :: missing :: Nil).eval(EmptyRow) == null)
    }
  }
}
