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
package org.apache.sedona.sql.geography

import org.apache.sedona.common.S2Geography.{Geography, WKBGeography}
import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.sedona_sql.expressions.{st_constructors, st_functions, st_predicates}
import org.junit.Assert.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.locationtech.jts.geom.Point
import org.locationtech.jts.io.WKTReader

/**
 * Spark SQL integration tests for Geography ST functions. Representative functions per
 * architecture level: L1 (ST_NPoints), L2 (ST_Distance, ST_Length), L3 (ST_Contains).
 */
class GeographyFunctionTest extends TestBaseScala {

  import sparkSession.implicits._

  // ─── Constructors ──────────────────────────────────────────────────────

  describe("Constructors") {

    it("ST_GeogFromWKT returns WKBGeography") {
      val row = sparkSession
        .sql("SELECT ST_GeogFromWKT('POINT (1 2)', 4326) AS geog")
        .first()
      val geog = row.get(0).asInstanceOf[Geography]
      assertTrue(geog.isInstanceOf[WKBGeography])
      assertEquals(4326, geog.getSRID)
      assertEquals("POINT (1 2)", geog.toString)
    }

    it("ST_GeogFromEWKT with SRID") {
      val row = sparkSession
        .sql("SELECT ST_GeogFromEWKT('SRID=4269;POINT (1 2)') AS geog")
        .first()
      val geog = row.get(0).asInstanceOf[Geography]
      assertEquals(4269, geog.getSRID)
    }

    it("ST_GeogFromWKB round-trip") {
      val row = sparkSession
        .sql("SELECT ST_GeogFromWKB(ST_AsBinary(ST_GeomFromWKT('POINT (30 10)'))) AS geog")
        .first()
      val geog = row.get(0).asInstanceOf[Geography]
      assertTrue(geog.isInstanceOf[WKBGeography])
      assertEquals("POINT (30 10)", geog.toString)
    }

    it("ST_GeomToGeography and ST_GeogToGeometry round-trip") {
      val row = sparkSession
        .sql("""
          SELECT ST_AsText(ST_GeogToGeometry(
            ST_GeomToGeography(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))
          )) AS wkt
        """)
        .first()
      val wkt = row.getString(0)
      assertTrue(wkt.contains("POLYGON"))
    }

    it("ST_GeomToGeography preserves exact coordinates and empty polygons") {
      val row = sparkSession
        .sql("""
          SELECT
            ST_GeomToGeography(ST_GeomFromWKT('POINT (1 2)', 4326)) AS point,
            ST_GeomToGeography(ST_GeomFromWKT('POLYGON EMPTY', 4326)) AS empty_polygon
        """)
        .first()
      val point = row.getAs[Geography](0)
      val emptyPolygon = row.getAs[Geography](1)
      assertEquals("SRID=4326; POINT (1 2)", point.toEWKT)
      assertEquals("SRID=4326; POLYGON EMPTY", emptyPolygon.toEWKT)
    }
  }

  // ─── Level 1: ST_NPoints, ST_Centroid, ST_NumGeometries, ST_GeometryType, ST_AsText ─

  describe("Level 1: Structural") {

    it("ST_NPoints") {
      val row = sparkSession
        .sql("SELECT ST_NPoints(ST_GeogFromWKT('LINESTRING (0 0, 1 1, 2 2)', 4326)) AS n")
        .first()
      assertEquals(3, row.getInt(0))
    }

    it("ST_Centroid square polygon") {
      val row = sparkSession
        .sql(
          "SELECT ST_Centroid(ST_GeogFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 4326)) AS c")
        .first()
      val centroid = row.get(0).asInstanceOf[Geography]
      val point = new WKTReader().read(centroid.toString).asInstanceOf[Point]
      // Spherical area-weighted centroid of a 2x2° square at origin is ~(1, 1) with an
      // O(d^2/R^2) spherical correction; 5e-3° is well within that band.
      assertEquals(1.0, point.getX, 5e-3)
      assertEquals(1.0, point.getY, 5e-3)
    }

    it("ST_NumGeometries single") {
      val row = sparkSession
        .sql("SELECT ST_NumGeometries(ST_GeogFromWKT('POINT (1 2)', 4326)) AS n")
        .first()
      assertEquals(1, row.getInt(0))
    }

    it("ST_NumGeometries multipoint") {
      val row = sparkSession
        .sql(
          "SELECT ST_NumGeometries(ST_GeogFromWKT('MULTIPOINT ((0 0), (1 1), (2 2))', 4326)) AS n")
        .first()
      assertEquals(3, row.getInt(0))
    }

    it("ST_GeometryType point") {
      val row = sparkSession
        .sql("SELECT ST_GeometryType(ST_GeogFromWKT('POINT (1 2)', 4326)) AS t")
        .first()
      assertEquals("ST_Point", row.getString(0))
    }

    it("ST_GeometryType polygon") {
      val row = sparkSession
        .sql("SELECT ST_GeometryType(ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)) AS t")
        .first()
      assertEquals("ST_Polygon", row.getString(0))
    }

    it("ST_AsText") {
      val row = sparkSession
        .sql("SELECT ST_AsText(ST_GeogFromWKT('POINT (1 2)', 4326)) AS wkt")
        .first()
      val wkt = row.getString(0)
      val point = new WKTReader().read(wkt).asInstanceOf[Point]
      // S2 round-trip may introduce sub-nanometer floating-point drift; use a loose tolerance.
      assertEquals(1.0, point.getX, 1e-9)
      assertEquals(2.0, point.getY, 1e-9)
    }

    it("ST_AsEWKT uses the stored WKB representation") {
      val row = sparkSession
        .sql("SELECT ST_AsEWKT(ST_GeogFromWKT('POINT (-122.4194 37.7749)', 4326)) AS ewkt")
        .first()
      assertEquals("SRID=4326; POINT (-122.4194 37.7749)", row.getString(0))
    }

    it("ST_X and ST_Y") {
      val row = sparkSession
        .sql("""
          SELECT
            ST_X(ST_GeogFromWKT('POINT (-73.9857 40.7484)', 4326)) AS x,
            ST_Y(ST_GeogFromWKT('POINT (-73.9857 40.7484)', 4326)) AS y
        """)
        .first()
      assertEquals(-73.9857, row.getDouble(0), 1e-12)
      assertEquals(40.7484, row.getDouble(1), 1e-12)
    }

    it("ST_X and ST_Y return null for non-point geography") {
      val row = sparkSession
        .sql("""
          SELECT
            ST_X(ST_GeogFromWKT('LINESTRING (0 0, 1 1)', 4326)) AS x,
            ST_Y(ST_GeogFromWKT('LINESTRING (0 0, 1 1)', 4326)) AS y
        """)
        .first()
      assertTrue(row.isNullAt(0))
      assertTrue(row.isNullAt(1))
    }

    it("ST_ConvexHull uses spherical Geography semantics") {
      val pointHull = sparkSession
        .sql("SELECT ST_ConvexHull(ST_GeogFromWKT('MULTIPOINT ((1 2), (1 2))', 4326)) AS hull")
      assertTrue(pointHull.schema("hull").dataType.isInstanceOf[GeographyUDT])
      assertEquals("POINT (1 2)", pointHull.first().getAs[Geography](0).toString)

      val lineHull = sparkSession
        .sql("SELECT ST_ConvexHull(ST_GeogFromWKT('LINESTRING (0 0, 0 1, 0 2)', 4326)) AS hull")
        .first()
        .getAs[Geography](0)
      assertEquals(
        "ST_LineString",
        org.apache.sedona.common.geography.Functions.geometryType(lineHull))
      assertEquals(4326, lineHull.getSRID)

      val polygonHull = sparkSession
        .sql("""
          SELECT ST_ConvexHull(
            ST_GeogFromWKT(
              'MULTIPOINT ((170 -10), (170 10), (-170 10), (-170 -10))',
              4326
            )
          ) AS hull
        """)
        .first()
        .getAs[Geography](0)
      assertEquals(
        "ST_Polygon",
        org.apache.sedona.common.geography.Functions.geometryType(polygonHull))
      assertTrue(org.apache.sedona.common.geography.Functions.area(polygonHull) < 1e14)

      val emptyLineHull = sparkSession
        .sql("SELECT ST_ConvexHull(ST_GeogFromWKT('LINESTRING EMPTY', 4326)) AS hull")
        .first()
        .getAs[Geography](0)
      assertEquals("LINESTRING EMPTY", emptyLineHull.toString)
      assertEquals(4326, emptyLineHull.getSRID)
    }

    it("ST_Collect accepts Geography arrays and scalar arguments") {
      val arrayResult = sparkSession.sql("""
        SELECT ST_Collect(array(
          ST_GeogFromWKT('POINT (1 2)', 4326),
          ST_GeogFromWKT(NULL, 4326),
          ST_GeogFromWKT('POINT (3 4)', 4326)
        )) AS collected
      """)
      assertTrue(arrayResult.schema("collected").dataType.isInstanceOf[GeographyUDT])
      assertEquals("MULTIPOINT ((1 2), (3 4))", arrayResult.first().getAs[Geography](0).toString)

      val mixedShapeResult = sparkSession
        .sql("""
          SELECT ST_Collect(
            ST_GeogFromWKT('POINT (1 2)', 4326),
            ST_GeogFromWKT('LINESTRING (0 0, 1 1)', 4326)
          ) AS collected
        """)
        .first()
        .getAs[Geography](0)
      assertEquals(
        "ST_GeometryCollection",
        org.apache.sedona.common.geography.Functions.geometryType(mixedShapeResult))
      assertEquals(4326, mixedShapeResult.getSRID)

      val withEmptyLine = sparkSession
        .sql("""
          SELECT ST_Collect(array(
            ST_GeogFromWKT('LINESTRING EMPTY', 4326),
            ST_GeogFromWKT('LINESTRING (0 0, 1 1)', 4326)
          )) AS collected
        """)
        .first()
        .getAs[Geography](0)
      assertEquals(2, org.apache.sedona.common.geography.Functions.numGeometries(withEmptyLine))
    }

    it("ST_Collect rejects mixed scalar and array arguments") {
      val error = intercept[org.apache.spark.sql.AnalysisException] {
        sparkSession
          .sql("""
            SELECT ST_Collect(
              ST_GeogFromWKT('POINT (1 2)', 4326),
              array(ST_GeogFromWKT('POINT (3 4)', 4326))
            )
          """)
          .collect()
      }
      assertTrue(error.getMessage.contains("either one array or one or more scalar values"))

      val nestedArrayError = intercept[org.apache.spark.sql.AnalysisException] {
        sparkSession
          .sql("""
            SELECT ST_Collect(
              array(array(ST_GeogFromWKT('POINT (1 2)', 4326)))
            )
          """)
          .collect()
      }
      assertTrue(nestedArrayError.getMessage.contains("expects Geometry or Geography values"))
    }

    it("computes the grouped Geography hull from ARRAY_AGG") {
      val result = sparkSession
        .sql("""
          WITH dropoffs AS (
            SELECT * FROM VALUES
              (1, ST_GeogFromWKT('POINT (0 0)', 4326)),
              (1, ST_GeogFromWKT('POINT (1 0)', 4326)),
              (1, ST_GeogFromWKT('POINT (0 1)', 4326))
            AS dropoffs(customer_id, geog)
          )
          SELECT
            ST_GeometryType(ST_ConvexHull(ST_Collect(ARRAY_AGG(geog)))) AS hull_type,
            ST_Area(ST_ConvexHull(ST_Collect(ARRAY_AGG(geog)))) AS area
          FROM dropoffs
          GROUP BY customer_id
        """)
        .first()

      assertEquals("ST_Polygon", result.getString(0))
      assertTrue(result.getDouble(1) > 6e9)
    }

    it("ST_Collect_Agg accepts Geography and feeds ST_ConvexHull") {
      val aggregate = sparkSession.sql("""
        WITH dropoffs AS (
          SELECT * FROM VALUES
            (1, ST_GeogFromWKT('POINT (0 0)', 4326)),
            (1, ST_GeogFromWKT('POINT (1 0)', 4326)),
            (1, ST_GeogFromWKT('POINT (0 1)', 4326))
          AS dropoffs(customer_id, geog)
        )
        SELECT
          ST_Collect_Agg(geog) AS collected,
          ST_Area(ST_ConvexHull(ST_Collect_Agg(geog))) AS hull_area
        FROM dropoffs
        GROUP BY customer_id
      """)

      assertTrue(aggregate.schema("collected").dataType.isInstanceOf[GeographyUDT])
      val row = aggregate.first()
      val collected = row.getAs[Geography]("collected")
      assertEquals(3, org.apache.sedona.common.geography.Functions.numGeometries(collected))
      assertEquals(4326, collected.getSRID)
      assertTrue(row.getAs[Double]("hull_area") > 6e9)

      val allNull = sparkSession.sql("""
        SELECT ST_Collect_Agg(geog) AS collected
        FROM (
          SELECT ST_GeogFromWKT(NULL, 4326) AS geog
          UNION ALL
          SELECT ST_GeogFromWKT(NULL, 4326) AS geog
        )
      """)
      assertTrue(allNull.schema("collected").dataType.isInstanceOf[GeographyUDT])
      assertTrue(allNull.first().isNullAt(0))
    }

    it("ST_Collect_Agg rejects mixed Geography SRIDs") {
      val error = intercept[Exception] {
        sparkSession
          .sql("""
            SELECT ST_Collect_Agg(geog)
            FROM (
              SELECT ST_GeogFromWKT('POINT (0 0)', 4326) AS geog
              UNION ALL
              SELECT ST_GeogFromWKT('POINT (1 1)', 3857) AS geog
            )
          """)
          .collect()
      }

      var cause: Throwable = error
      var hasExpectedMessage = false
      while (cause != null) {
        hasExpectedMessage ||= Option(cause.getMessage)
          .exists(_.contains("same SRID"))
        cause = cause.getCause
      }
      assertTrue(hasExpectedMessage)
    }

    it("ST_MakeLine creates a geography measured in meters") {
      val row = sparkSession
        .sql("""
          SELECT
            ST_AsText(line) AS wkt,
            ST_GeometryType(line) AS geom_type,
            ST_Length(line) AS length
          FROM (
            SELECT ST_MakeLine(
              ST_GeogFromWKT('POINT (0 0)', 4326),
              ST_GeogFromWKT('POINT (1 0)', 4326)
            ) AS line
          )
        """)
        .first()
      assertEquals("LINESTRING (0 0, 1 0)", row.getString(0))
      assertEquals("ST_LineString", row.getString(1))
      assertEquals(111195.10, row.getDouble(2), 1.0)
    }

    it("ST_MakeLine preserves coincident points and deduplicates LineString seams") {
      val coincident = sparkSession
        .sql("""
          WITH source AS (
            SELECT ST_GeogFromWKT('POINT (12 34)', 4326) AS point
          ),
          made AS (
            SELECT point, ST_MakeLine(point, point) AS line
            FROM source
          )
          SELECT
            ST_AsText(line) AS wkt,
            ST_AsEWKT(line) AS ewkt,
            ST_Centroid(line) AS centroid,
            ST_Envelope(line, false) AS envelope,
            ST_Envelope(line, true) AS split_envelope,
            ST_NPoints(line) AS npoints,
            ST_GeometryType(line) AS geom_type,
            ST_NumGeometries(line) AS ngeoms,
            ST_Length(line) AS length,
            ST_Distance(line, point) AS distance,
            ST_AsText(
              ST_MakeLine(line, ST_GeogFromWKT('POINT (13 34)', 4326))
            ) AS extended,
            line AS geography
          FROM made
        """)
        .first()
      assertEquals("LINESTRING (12 34, 12 34)", coincident.getString(0))
      assertEquals("SRID=4326; LINESTRING (12 34, 12 34)", coincident.getString(1))
      val centroid = coincident.getAs[Geography](2)
      assertEquals(4326, centroid.getSRID)
      assertEquals("POINT (12 34)", centroid.toString)
      Seq(3, 4).foreach { index =>
        val envelope = coincident.getAs[Geography](index)
        assertEquals(4326, envelope.getSRID)
        assertEquals("POINT (12 34)", envelope.toString)
      }
      assertEquals(2, coincident.getInt(5))
      assertEquals("ST_LineString", coincident.getString(6))
      assertEquals(1, coincident.getInt(7))
      assertEquals(0.0, coincident.getDouble(8), 0.0)
      assertEquals(0.0, coincident.getDouble(9), 0.0)
      assertEquals("LINESTRING (12 34, 12 34, 13 34)", coincident.getString(10))
      val geography = coincident.getAs[Geography](11)
      assertEquals("LINESTRING (12 34, 12 34)", geography.toString)
      assertEquals("SRID=4326; LINESTRING (12 34, 12 34)", geography.toEWKT)

      val repeated = sparkSession
        .sql("""
          SELECT
            ST_AsText(line) AS wkt,
            ST_AsEWKT(line) AS ewkt,
            ST_NPoints(line) AS npoints
          FROM (
            SELECT ST_MakeLine(
              ST_GeogFromWKT('LINESTRING (0 0, 1 0)', 4326),
              ST_GeogFromWKT('LINESTRING (1 0, 2 0)', 4326)
            ) AS line
          )
        """)
        .first()
      assertEquals("LINESTRING (0 0, 1 0, 2 0)", repeated.getString(0))
      assertEquals("SRID=4326; LINESTRING (0 0, 1 0, 2 0)", repeated.getString(1))
      assertEquals(3, repeated.getInt(2))
    }

    it("ST_MakeLine skips empty geography inputs") {
      val row = sparkSession
        .sql("""
          SELECT
            ST_AsText(ST_MakeLine(
              ST_GeogFromWKT('POINT EMPTY', 3857),
              ST_GeogFromWKT('POINT (12 34)', 4326)
            )) AS empty_first,
            ST_AsText(ST_MakeLine(
              ST_GeogFromWKT('POINT (12 34)', 4326),
              ST_GeogFromWKT('LINESTRING EMPTY', 3857)
            )) AS empty_second,
            ST_AsEWKT(ST_MakeLine(
              ST_GeogFromWKT('POINT EMPTY', 3857),
              ST_GeogFromWKT('LINESTRING EMPTY', 4326)
            )) AS both_empty
        """)
        .first()

      assertEquals("LINESTRING (12 34, 12 34)", row.getString(0))
      assertEquals("LINESTRING (12 34, 12 34)", row.getString(1))
      assertEquals("SRID=3857; LINESTRING EMPTY", row.getString(2))
    }

    it("ST_Intersection returns Geography and feeds spherical ST_Area") {
      val result = sparkSession.sql("""
        SELECT
          overlap,
          ST_GeometryType(overlap) AS overlap_type,
          ST_Area(overlap) AS overlap_area
        FROM (
          SELECT ST_Intersection(
            ST_GeogFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 3857),
            ST_GeogFromWKT('POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))', 4326)
          ) AS overlap
        )
      """)

      assertTrue(result.schema("overlap").dataType.isInstanceOf[GeographyUDT])
      val row = result.first()
      val overlap = row.getAs[Geography](0)
      assertEquals(3857, overlap.getSRID)
      assertEquals("ST_Polygon", row.getString(1))
      assertEquals(3.071055126726233e11, row.getDouble(2), 1e5)
    }

    it("ST_Intersection uses closed-set boundary semantics") {
      val row = sparkSession
        .sql("""
          SELECT
            ST_AsText(ST_Intersection(
              ST_GeogFromWKT('LINESTRING (0 -5, 0 5)', 4326),
              ST_GeogFromWKT('LINESTRING (-5 0, 5 0)', 4326)
            )) AS crossing,
            ST_AsText(ST_Intersection(
              ST_GeogFromWKT('LINESTRING (0 0, 1 0)', 4326),
              ST_GeogFromWKT('LINESTRING (0 1, 1 1)', 4326)
            )) AS disjoint,
            ST_GeometryType(ST_Intersection(
              ST_GeogFromWKT('LINESTRING (0 0, 0 20)', 4326),
              ST_GeogFromWKT('LINESTRING (0 5, 0 15)', 4326)
            )) AS partial_overlap_type,
            ST_Length(ST_Intersection(
              ST_GeogFromWKT('LINESTRING (0 0, 0 20)', 4326),
              ST_GeogFromWKT('LINESTRING (0 5, 0 15)', 4326)
            )) AS partial_overlap_length,
            ST_Intersection(
              ST_GeogFromWKT(NULL, 4326),
              ST_GeogFromWKT('POINT (0 0)', 4326)
            ) AS null_result
        """)
        .first()

      assertEquals("POINT (0 0)", row.getString(0))
      assertEquals("LINESTRING EMPTY", row.getString(1))
      assertEquals("ST_LineString", row.getString(2))
      assertTrue(row.getDouble(3) > 1000000.0)
      assertTrue(row.isNullAt(4))
    }
  }

  // ─── Level 2: ST_Length, ST_Area, ST_Distance ──────────────────────────

  describe("Level 2: Geodesic metrics") {

    it("ST_Length along the equator") {
      val row = sparkSession
        .sql("SELECT ST_Length(ST_GeogFromWKT('LINESTRING (0 0, 1 0)', 4326)) AS l")
        .first()
      val len = row.getDouble(0)
      // Sphere of radius 6371008 m: 1° along a great circle is ~111,195 m.
      assertEquals(111195.10, len, 1.0)
    }

    it("ST_Length of a point returns 0") {
      val row = sparkSession
        .sql("SELECT ST_Length(ST_GeogFromWKT('POINT (1 2)', 4326)) AS l")
        .first()
      assertEquals(0.0, row.getDouble(0), 0.0)
    }

    it("ST_Area unit box at equator") {
      val row = sparkSession
        .sql("SELECT ST_Area(ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)) AS a")
        .first()
      val area = row.getDouble(0)
      // Spherical area of a 1°×1° box near the equator on a sphere of radius 6371008.0 m.
      // Tolerance 1e7 m² (~0.08%) absorbs floating-point drift while staying tight enough to
      // catch a model swap.
      assertEquals(1.2364e10, area, 1e7)
    }

    it("ST_Area of a point returns 0") {
      val row = sparkSession
        .sql("SELECT ST_Area(ST_GeogFromWKT('POINT (1 2)', 4326)) AS a")
        .first()
      assertEquals(0.0, row.getDouble(0), 0.0)
    }

    it("ST_Distance between two points") {
      val row = sparkSession
        .sql("""
          SELECT ST_Distance(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (1 1)', 4326)
          ) AS dist
        """)
        .first()
      val dist = row.getDouble(0)
      assertTrue(s"Expected ~157km, got $dist", dist > 155000 && dist < 160000)
    }

    it("ST_Distance null handling") {
      val row = sparkSession
        .sql("SELECT ST_Distance(ST_GeogFromWKT('POINT (0 0)', 4326), null) AS dist")
        .first()
      assertTrue(row.isNullAt(0))
    }
  }

  // ─── Level 3: ST_Contains ──────────────────────────────────────────────

  describe("Level 3: S2 predicates") {

    it("ST_Contains point in polygon") {
      val row = sparkSession
        .sql("""
          SELECT ST_Contains(
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326),
            ST_GeogFromWKT('POINT (0.5 0.5)', 4326)
          ) AS result
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_Contains point outside polygon") {
      val row = sparkSession
        .sql("""
          SELECT ST_Contains(
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326),
            ST_GeogFromWKT('POINT (2 2)', 4326)
          ) AS result
        """)
        .first()
      assertTrue(!row.getBoolean(0))
    }

    it("ST_GeomToGeography uses polygon ring roles regardless of winding") {
      val row = sparkSession
        .sql("""
          WITH polygons AS (
            SELECT
              ST_GeomToGeography(ST_GeomFromWKT(
                'POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))', 4326
              )) AS clockwise,
              ST_GeomToGeography(ST_GeomFromWKT(
                'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326
              )) AS counterclockwise
          ),
          points AS (
            SELECT
              ST_GeogFromWKT('POINT (0.5 0.5)', 4326) AS inside,
              ST_GeogFromWKT('POINT (5 5)', 4326) AS outside
          )
          SELECT
            ST_Contains(clockwise, inside) AS cw_inside,
            ST_Contains(clockwise, outside) AS cw_outside,
            ST_Contains(counterclockwise, inside) AS ccw_inside,
            ST_Contains(counterclockwise, outside) AS ccw_outside,
            ST_AsText(clockwise) AS cw_text,
            ST_AsText(counterclockwise) AS ccw_text
          FROM polygons CROSS JOIN points
        """)
        .first()

      assertTrue(row.getBoolean(0))
      assertFalse(row.getBoolean(1))
      assertTrue(row.getBoolean(2))
      assertFalse(row.getBoolean(3))
      assertEquals("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", row.getString(4))
      assertEquals("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", row.getString(5))
    }

    it("ST_DWithin true when within threshold") {
      val row = sparkSession
        .sql("""
          SELECT ST_DWithin(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (0 1)', 4326),
            200000.0) AS r
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_Equals same point") {
      val row = sparkSession
        .sql("""
          SELECT ST_Equals(
            ST_GeogFromWKT('POINT (1 2)', 4326),
            ST_GeogFromWKT('POINT (1 2)', 4326)
          ) AS result
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_DWithin false when outside threshold") {
      val row = sparkSession
        .sql("""
          SELECT ST_DWithin(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (0 1)', 4326),
            100000.0) AS r
        """)
        .first()
      assertTrue(!row.getBoolean(0))
    }

    it("ST_Equals different points") {
      val row = sparkSession
        .sql("""
          SELECT ST_Equals(
            ST_GeogFromWKT('POINT (1 2)', 4326),
            ST_GeogFromWKT('POINT (3 4)', 4326)
          ) AS result
        """)
        .first()
      assertTrue(!row.getBoolean(0))
    }

    it("ST_DWithin null handling") {
      // null as second arg
      val r1 = sparkSession
        .sql("SELECT ST_DWithin(ST_GeogFromWKT('POINT (0 0)', 4326), null, 1.0) AS r")
        .first()
      assertTrue(r1.isNullAt(0))
      // null as first arg
      val r2 = sparkSession
        .sql("SELECT ST_DWithin(null, ST_GeogFromWKT('POINT (0 0)', 4326), 1.0) AS r")
        .first()
      assertTrue(r2.isNullAt(0))
      // null distance
      val r3 = sparkSession
        .sql("""
          SELECT ST_DWithin(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (0 1)', 4326),
            CAST(null AS DOUBLE)) AS r
        """)
        .first()
      assertTrue(r3.isNullAt(0))
    }

    it("ST_DWithin accepts INT distance literal") {
      // Catalyst should coerce INT -> DOUBLE for the 3-arg Geography overload.
      val row = sparkSession
        .sql("""
          SELECT ST_DWithin(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (0 1)', 4326),
            200000) AS r
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_DWithin accepts FLOAT distance literal") {
      // CAST to FLOAT forces a narrower type than DOUBLE; Catalyst should widen it.
      val row = sparkSession
        .sql("""
          SELECT ST_DWithin(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (0 1)', 4326),
            CAST(200000.5 AS FLOAT)) AS r
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_Within point in polygon") {
      val row = sparkSession
        .sql("""
          SELECT ST_Within(
            ST_GeogFromWKT('POINT (0.5 0.5)', 4326),
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)
          ) AS r
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_Equals same polygon") {
      val row = sparkSession
        .sql("""
          SELECT ST_Equals(
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326),
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)
          ) AS result
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_Within point outside polygon") {
      val row = sparkSession
        .sql("""
          SELECT ST_Within(
            ST_GeogFromWKT('POINT (2 2)', 4326),
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)
          ) AS r
        """)
        .first()
      assertTrue(!row.getBoolean(0))
    }

    it("ST_Equals different polygons") {
      val row = sparkSession
        .sql("""
          SELECT ST_Equals(
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326),
            ST_GeogFromWKT('POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))', 4326)
          ) AS result
        """)
        .first()
      assertTrue(!row.getBoolean(0))
    }

    it("ST_Within point on polygon boundary returns a Boolean (semantics implementation-defined)") {
      // S2 boolean ownership of an edge depends on vertex orientation, so the result for points
      // exactly on the boundary is intentionally not asserted. We only verify the call completes
      // and returns a non-null Boolean — the docs steer users toward ST_Buffer for predictable
      // boundary handling.
      val row = sparkSession
        .sql("""
          SELECT ST_Within(
            ST_GeogFromWKT('POINT (0 0.5)', 4326),
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)
          ) AS r
        """)
        .first()
      assertTrue(!row.isNullAt(0))
    }

    it("ST_Within null handling") {
      val r1 = sparkSession
        .sql("SELECT ST_Within(ST_GeogFromWKT('POINT (0 0)', 4326), null) AS r")
        .first()
      assertTrue(r1.isNullAt(0))
      val r2 = sparkSession
        .sql("SELECT ST_Within(null, ST_GeogFromWKT('POINT (0 0)', 4326)) AS r")
        .first()
      assertTrue(r2.isNullAt(0))
    }

    it("ST_Intersects overlapping polygons") {
      val row = sparkSession
        .sql("""
          SELECT ST_Intersects(
            ST_GeogFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 4326),
            ST_GeogFromWKT('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))', 4326)
          ) AS result
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_Intersects disjoint polygons") {
      val row = sparkSession
        .sql("""
          SELECT ST_Intersects(
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326),
            ST_GeogFromWKT('POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))', 4326)
          ) AS result
        """)
        .first()
      assertTrue(!row.getBoolean(0))
    }
  }

  // ─── Level 4: ST_Buffer ────────────────────────────────────────────────

  describe("Level 4: Spherical buffer") {

    it("ST_Buffer of a point produces a polygon containing nearby points") {
      val row = sparkSession
        .sql("""
          SELECT
            ST_GeometryType(ST_Buffer(ST_GeogFromWKT('POINT(0 0)', 4326), 1000)) AS gtype,
            ST_Contains(
              ST_Buffer(ST_GeogFromWKT('POINT(0 0)', 4326), 1000),
              ST_GeogFromWKT('POINT(0.005 0.005)', 4326)
            ) AS hits
        """)
        .first()
      assertEquals("ST_Polygon", row.getString(0))
      assertTrue("buffer should contain a point ~785m away", row.getBoolean(1))
    }

    it("ST_Buffer of a polygon contains the original interior") {
      val hits = sparkSession
        .sql("""
          SELECT ST_Contains(
            ST_Buffer(
              ST_GeogFromWKT('POLYGON((0 0, 0.01 0, 0.01 0.01, 0 0.01, 0 0))', 4326),
              200
            ),
            ST_GeogFromWKT('POINT(0.005 0.005)', 4326)
          ) AS r
        """)
        .first()
        .getBoolean(0)
      assertTrue(hits)
    }

    it("ST_Buffer with parameters string is honored") {
      // quad_segs=2 produces an octagonal-ish buffer; quad_segs=64 is much smoother.
      val row = sparkSession
        .sql("""
          SELECT
            ST_NPoints(ST_Buffer(ST_GeogFromWKT('POINT(0 0)', 4326), 1000, 'quad_segs=2')) AS coarse,
            ST_NPoints(ST_Buffer(ST_GeogFromWKT('POINT(0 0)', 4326), 1000, 'quad_segs=64')) AS fine
        """)
        .first()
      assertTrue(
        s"fine (${row.getInt(1)}) should have more vertices than coarse (${row.getInt(0)})",
        row.getInt(1) > row.getInt(0))
    }

    it("ST_Buffer result survives the GeographyUDT round-trip") {
      val df = sparkSession
        .sql("SELECT ST_Buffer(ST_GeogFromWKT('POINT(0 0)', 4326), 500) AS buf")
      val geog = df.first().get(0).asInstanceOf[Geography]
      assertTrue(geog.isInstanceOf[WKBGeography])
      assertEquals(4326, geog.getSRID)
    }

    it("ST_Buffer with useSpheroid is rejected for Geography inputs") {
      val ex = intercept[Throwable] {
        sparkSession
          .sql("SELECT ST_Buffer(ST_GeogFromWKT('POINT(0 0)', 4326), 1000.0, true) AS b")
          .first()
      }
      // The actual cause may sit one or two layers down inside InferredExpressionException.
      val msg = Iterator
        .iterate[Throwable](ex)(t => if (t == null) null else t.getCause)
        .takeWhile(_ != null)
        .map(_.getMessage)
        .mkString(" | ")
      assert(
        msg.contains("useSpheroid") && msg.contains("Geography"),
        s"expected useSpheroid/Geography in message; got: $msg")
    }

    it("ST_Buffer via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POINT(0 0)' AS wkt")
        .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
        .select(st_functions.ST_Buffer(col("g"), lit(1000.0)).as("buf"))
      val geog = df.first().get(0).asInstanceOf[Geography]
      assertTrue(geog.isInstanceOf[WKBGeography])
      assertTrue(geog.toString.startsWith("POLYGON"))
    }
  }

  // ─── DataFrame API ─────────────────────────────────────────────────────

  describe("DataFrame API") {

    it("ST_Distance via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POINT (0 0)' AS wkt_a, 'POINT (1 1)' AS wkt_b")
        .select(
          st_constructors.ST_GeogFromWKT(col("wkt_a"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("wkt_b"), lit(4326)).as("b"))
        .select(st_functions.ST_Distance(col("a"), col("b")).as("dist"))
      val dist = df.first().getDouble(0)
      assertTrue(s"Expected ~157km, got $dist", dist > 155000 && dist < 160000)
    }

    it("ST_NPoints via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'LINESTRING (0 0, 1 1, 2 2)' AS wkt")
        .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("geog"))
        .select(st_functions.ST_NPoints(col("geog")).as("n"))
      assertEquals(3, df.first().getInt(0))
    }

    it("ST_MakeLine via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POINT (0 0)' AS wkt_a, 'POINT (1 0)' AS wkt_b")
        .select(
          st_constructors.ST_GeogFromWKT(col("wkt_a"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("wkt_b"), lit(4326)).as("b"))
        .select(st_functions.ST_MakeLine(col("a"), col("b")).as("line"))
      val line = df.first().get(0).asInstanceOf[Geography]
      assertTrue(line.isInstanceOf[WKBGeography])
      assertEquals("LINESTRING (0 0, 1 0)", line.toString)
    }

    it("ST_Intersection via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'LINESTRING (0 -5, 0 5)' AS wkt_a, 'LINESTRING (-5 0, 5 0)' AS wkt_b")
        .select(
          st_constructors.ST_GeogFromWKT(col("wkt_a"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("wkt_b"), lit(4326)).as("b"))
        .select(st_functions.ST_Intersection(col("a"), col("b")).as("intersection"))

      assertTrue(df.schema("intersection").dataType.isInstanceOf[GeographyUDT])
      assertEquals("POINT (0 0)", df.first().getAs[Geography](0).toString)
    }

    it("ST_Contains via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS poly, 'POINT (0.5 0.5)' AS pt")
        .select(
          st_constructors.ST_GeogFromWKT(col("poly"), lit(4326)).as("poly"),
          st_constructors.ST_GeogFromWKT(col("pt"), lit(4326)).as("pt"))
        .select(st_predicates.ST_Contains(col("poly"), col("pt")).as("result"))
      assertTrue(df.first().getBoolean(0))
    }

    it("ST_DWithin via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POINT (0 0)' AS a, 'POINT (0 1)' AS b")
        .select(
          st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
        .select(st_predicates.ST_DWithin(col("a"), col("b"), lit(200000.0)).as("r"))
      assertTrue(df.first().getBoolean(0))
    }

    it("ST_Within via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POINT (0.5 0.5)' AS pt, 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS poly")
        .select(
          st_constructors.ST_GeogFromWKT(col("pt"), lit(4326)).as("pt"),
          st_constructors.ST_GeogFromWKT(col("poly"), lit(4326)).as("poly"))
        .select(st_predicates.ST_Within(col("pt"), col("poly")).as("r"))
      assertTrue(df.first().getBoolean(0))
    }

    it("ST_Equals via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS a, 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS b")
        .select(
          st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
        .select(st_predicates.ST_Equals(col("a"), col("b")).as("result"))
      assertTrue(df.first().getBoolean(0))
    }
  }

  // ─── Serialization round-trip ──────────────────────────────────────────

  describe("Serialization round-trip") {

    it("Geography survives DataFrame collect") {
      val df = sparkSession
        .sql("SELECT ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326) AS geog")
      val geog = df.first().get(0).asInstanceOf[Geography]
      assertTrue(geog.isInstanceOf[WKBGeography])
      assertEquals(4326, geog.getSRID)
      assertTrue(geog.toString.contains("POLYGON"))
    }

    it("Geography survives multiple function chain") {
      val row = sparkSession
        .sql("""
          SELECT ST_Distance(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (1 0)', 4326)
          ) AS dist,
          ST_NPoints(ST_GeogFromWKT('LINESTRING (0 0, 1 1, 2 2)', 4326)) AS npts
        """)
        .first()
      assertTrue(row.getDouble(0) > 0)
      assertEquals(3, row.getInt(1))
    }
  }
}
