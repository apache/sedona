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

  // ─── Level 1: ST_NPoints ───────────────────────────────────────────────

  describe("Level 1: Structural") {

    it("ST_NPoints") {
      val row = sparkSession
        .sql("SELECT ST_NPoints(ST_GeogFromWKT('LINESTRING (0 0, 1 1, 2 2)', 4326)) AS n")
        .first()
      assertEquals(3, row.getInt(0))
    }
  }

  // ─── Level 2: ST_Distance ──────────────────────────────────────────────

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
