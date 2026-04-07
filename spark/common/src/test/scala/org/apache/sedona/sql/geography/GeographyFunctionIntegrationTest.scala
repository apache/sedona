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
import org.locationtech.jts.geom.Geometry

/**
 * Comprehensive Spark SQL integration tests for all Geography ST functions.
 *
 * Tests cover:
 *   - Constructors: ST_GeogFromWKB, ST_GeogFromWKT, ST_GeogFromEWKT, ST_GeomToGeography,
 *     ST_GeogToGeometry
 *   - Level 0+1 (Structural): ST_Envelope, ST_AsEWKT, ST_AsText, ST_NPoints, ST_GeometryType,
 *     ST_NumGeometries, ST_Centroid
 *   - Level 2 (Geodesic metrics): ST_Distance, ST_Area, ST_Length
 *   - Level 3 (S2 predicates): ST_Contains, ST_Intersects, ST_Equals, ST_MaxDistance,
 *     ST_ClosestPoint
 *   - Serialization round-trip: WKBGeography through Spark DataFrame write/read
 */
class GeographyFunctionIntegrationTest extends TestBaseScala {

  import sparkSession.implicits._

  // ─── Helper: create a Geography column from WKT ────────────────────────

  private def geogFromWKT(wkt: String, srid: Int = 4326) =
    sparkSession
      .sql(s"SELECT '$wkt' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(srid)).as("geog"))

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
      // Create WKB from a Geometry, then read as Geography
      val row = sparkSession
        .sql(
          "SELECT ST_GeogFromWKB(ST_AsBinary(ST_GeomFromWKT('POINT (30 10)'))) AS geog")
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

  // ─── Level 0+1: Structural functions ───────────────────────────────────

  describe("Level 0+1: Structural") {

    it("ST_AsEWKT with SRID") {
      val row = sparkSession
        .sql("SELECT ST_AsEWKT(ST_GeogFromWKT('POINT (1 2)', 4326)) AS ewkt")
        .first()
      assertEquals("SRID=4326; POINT (1 2)", row.getString(0))
    }

    it("ST_AsText") {
      val row = sparkSession
        .sql("SELECT ST_AsText(ST_GeogFromWKT('LINESTRING (0 0, 1 1, 2 2)', 4326)) AS wkt")
        .first()
      assertEquals("LINESTRING (0 0, 1 1, 2 2)", row.getString(0))
    }

    it("ST_NPoints") {
      val row = sparkSession
        .sql("SELECT ST_NPoints(ST_GeogFromWKT('LINESTRING (0 0, 1 1, 2 2)', 4326)) AS n")
        .first()
      assertEquals(3, row.getInt(0))
    }

    it("ST_GeometryType") {
      val row = sparkSession
        .sql("SELECT ST_GeometryType(ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)) AS t")
        .first()
      assertTrue(row.getString(0).contains("Polygon"))
    }

    it("ST_NumGeometries") {
      val row = sparkSession
        .sql("SELECT ST_NumGeometries(ST_GeogFromWKT('POINT (1 2)', 4326)) AS n")
        .first()
      assertEquals(1, row.getInt(0))
    }

    it("ST_Centroid") {
      val row = sparkSession
        .sql("SELECT ST_AsText(ST_Centroid(ST_GeogFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 4326))) AS c")
        .first()
      val wkt = row.getString(0)
      assertTrue(wkt.contains("POINT"))
      assertTrue(wkt.contains("1")) // centroid near (1, 1)
    }

    it("ST_Envelope") {
      val row = sparkSession
        .sql("SELECT ST_Envelope(ST_GeogFromWKT('LINESTRING (0 0, 3 4)', 4326), false) AS env")
        .first()
      val env = row.get(0).asInstanceOf[Geography]
      assertNotNull(env)
    }
  }

  // ─── Level 2: Geodesic metrics ─────────────────────────────────────────

  describe("Level 2: Geodesic metrics") {

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
      // S2 spherical distance ~157 km
      assertTrue(s"Expected ~157km, got $dist", dist > 155000 && dist < 160000)
    }

    it("ST_Distance between point and polygon") {
      val row = sparkSession
        .sql("""
          SELECT ST_Distance(
            ST_GeogFromWKT('POINT (2 2)', 4326),
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)
          ) AS dist
        """)
        .first()
      val dist = row.getDouble(0)
      assertTrue(s"Distance should be > 0, got $dist", dist > 0)
    }

    it("ST_Distance null handling") {
      val row = sparkSession
        .sql("SELECT ST_Distance(ST_GeogFromWKT('POINT (0 0)', 4326), null) AS dist")
        .first()
      assertTrue(row.isNullAt(0))
    }

    it("ST_Area of polygon") {
      val row = sparkSession
        .sql("""
          SELECT ST_Area(
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)
          ) AS area
        """)
        .first()
      val area = row.getDouble(0)
      // ~1 degree x 1 degree box near equator: ~12,300 km^2 = ~1.23e10 m^2
      assertTrue(s"Expected area > 1e10 m^2, got $area", area > 1e10)
    }

    it("ST_Area of point is zero") {
      val row = sparkSession
        .sql("SELECT ST_Area(ST_GeogFromWKT('POINT (0 0)', 4326)) AS area")
        .first()
      assertEquals(0.0, row.getDouble(0), 1e-10)
    }

    it("ST_Length of linestring") {
      val row = sparkSession
        .sql("""
          SELECT ST_Length(
            ST_GeogFromWKT('LINESTRING (0 0, 1 1)', 4326)
          ) AS len
        """)
        .first()
      val len = row.getDouble(0)
      // ~157 km
      assertTrue(s"Expected ~157km, got $len", len > 155000 && len < 160000)
    }

    it("ST_Length of polygon is zero") {
      val row = sparkSession
        .sql("""
          SELECT ST_Length(
            ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)
          ) AS len
        """)
        .first()
      assertEquals(0.0, row.getDouble(0), 1e-10)
    }
  }

  // ─── Level 3: S2 predicates and distance functions ─────────────────────

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

    it("ST_Equals same geography") {
      val row = sparkSession
        .sql("""
          SELECT ST_Equals(
            ST_GeogFromWKT('POINT (1 1)', 4326),
            ST_GeogFromWKT('POINT (1 1)', 4326)
          ) AS result
        """)
        .first()
      assertTrue(row.getBoolean(0))
    }

    it("ST_Equals different geography") {
      val row = sparkSession
        .sql("""
          SELECT ST_Equals(
            ST_GeogFromWKT('POINT (1 1)', 4326),
            ST_GeogFromWKT('POINT (2 2)', 4326)
          ) AS result
        """)
        .first()
      assertTrue(!row.getBoolean(0))
    }

    it("ST_MaxDistance between two points") {
      val row = sparkSession
        .sql("""
          SELECT ST_MaxDistance(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (1 1)', 4326)
          ) AS dist
        """)
        .first()
      val dist = row.getDouble(0)
      assertTrue(s"Expected ~157km, got $dist", dist > 155000 && dist < 160000)
    }

    it("ST_ClosestPoint from line to point") {
      val row = sparkSession
        .sql("""
          SELECT ST_AsText(ST_ClosestPoint(
            ST_GeogFromWKT('LINESTRING (0 0, 2 0)', 4326),
            ST_GeogFromWKT('POINT (1 1)', 4326)
          )) AS wkt
        """)
        .first()
      val wkt = row.getString(0)
      assertTrue(wkt.contains("POINT"))
    }
  }

  // ─── DataFrame API tests ──────────────────────────────────────────────

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

    it("ST_Area via DataFrame API") {
      val df = geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
        .select(st_functions.ST_Area(col("geog")).as("area"))
      val area = df.first().getDouble(0)
      assertTrue(s"Expected area > 1e10, got $area", area > 1e10)
    }

    it("ST_Length via DataFrame API") {
      val df = geogFromWKT("LINESTRING (0 0, 1 1)")
        .select(st_functions.ST_Length(col("geog")).as("len"))
      val len = df.first().getDouble(0)
      assertTrue(s"Expected ~157km, got $len", len > 155000 && len < 160000)
    }

    it("ST_NPoints via DataFrame API") {
      val df = geogFromWKT("LINESTRING (0 0, 1 1, 2 2)")
        .select(st_functions.ST_NPoints(col("geog")).as("n"))
      assertEquals(3, df.first().getInt(0))
    }

    it("ST_MaxDistance via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POINT (0 0)' AS wkt_a, 'POINT (1 1)' AS wkt_b")
        .select(
          st_constructors.ST_GeogFromWKT(col("wkt_a"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("wkt_b"), lit(4326)).as("b"))
        .select(st_functions.ST_MaxDistance(col("a"), col("b")).as("dist"))
      val dist = df.first().getDouble(0)
      assertTrue(s"Expected ~157km, got $dist", dist > 155000 && dist < 160000)
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

    it("ST_Intersects via DataFrame API") {
      val df = sparkSession
        .sql(
          "SELECT 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))' AS a, 'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))' AS b")
        .select(
          st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
        .select(st_predicates.ST_Intersects(col("a"), col("b")).as("result"))
      assertTrue(df.first().getBoolean(0))
    }

    it("ST_Equals via DataFrame API") {
      val df = sparkSession
        .sql("SELECT 'POINT (1 1)' AS wkt")
        .select(
          st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("a"),
          st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("b"))
        .select(st_predicates.ST_Equals(col("a"), col("b")).as("result"))
      assertTrue(df.first().getBoolean(0))
    }
  }

  // ─── WKB serialization round-trip through Spark ────────────────────────

  describe("Serialization round-trip") {

    it("Geography survives DataFrame collect") {
      val df = sparkSession
        .sql("SELECT ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326) AS geog")
      val geog = df.first().get(0).asInstanceOf[Geography]
      assertTrue(geog.isInstanceOf[WKBGeography])
      assertEquals(4326, geog.getSRID)
      assertTrue(geog.toString.contains("POLYGON"))
    }

    it("Geography survives filter + collect") {
      val df = sparkSession
        .sql("""
          SELECT ST_GeogFromWKT(wkt, 4326) AS geog FROM (
            SELECT 'POINT (0 0)' AS wkt
            UNION ALL
            SELECT 'POINT (1 1)' AS wkt
            UNION ALL
            SELECT 'POINT (2 2)' AS wkt
          )
        """)
      assertEquals(3, df.count())
      df.collect().foreach { row =>
        val geog = row.get(0).asInstanceOf[Geography]
        assertTrue(geog.isInstanceOf[WKBGeography])
        assertTrue(geog.toString.startsWith("POINT"))
      }
    }

    it("Geography survives multiple function chain") {
      val row = sparkSession
        .sql("""
          SELECT ST_Distance(
            ST_GeogFromWKT('POINT (0 0)', 4326),
            ST_GeogFromWKT('POINT (1 0)', 4326)
          ) AS dist,
          ST_Area(ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)) AS area,
          ST_Length(ST_GeogFromWKT('LINESTRING (0 0, 0 1)', 4326)) AS len
        """)
        .first()
      assertTrue(row.getDouble(0) > 0) // distance > 0
      assertTrue(row.getDouble(1) > 0) // area > 0
      assertTrue(row.getDouble(2) > 0) // length > 0
    }
  }
}
