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
import org.apache.spark.sql.sedona_sql.expressions.{st_constructors, st_functions, st_predicates}
import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.WKTReader

/**
 * Spark SQL integration tests for Geography ST functions. Tests one representative function per
 * architecture level: L1 (ST_NPoints), L2 (ST_Distance), L3 (ST_Contains).
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
  }

  // ─── Level 1: ST_NPoints, ST_NumGeometries, ST_GeometryType, ST_AsText ──

  describe("Level 1: Structural") {

    it("ST_NPoints") {
      val row = sparkSession
        .sql("SELECT ST_NPoints(ST_GeogFromWKT('LINESTRING (0 0, 1 1, 2 2)', 4326)) AS n")
        .first()
      assertEquals(3, row.getInt(0))
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
  }

  // ─── Level 2: ST_Area, ST_Distance ─────────────────────────────────────

  describe("Level 2: Geodesic metrics") {

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

    it("ST_Contains via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS poly, 'POINT (0.5 0.5)' AS pt")
        .select(
          st_constructors.ST_GeogFromWKT(col("poly"), lit(4326)).as("poly"),
          st_constructors.ST_GeogFromWKT(col("pt"), lit(4326)).as("pt"))
        .select(st_predicates.ST_Contains(col("poly"), col("pt")).as("result"))
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
