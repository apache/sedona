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
package org.apache.sedona.viz.sql

import org.apache.sedona.viz.utils.Pixel

class PixelizeTest extends VizTestBase {

  /** Pixelizes a WKT geometry on a 10 x 10 grid over (0, 0, 10, 10) and returns pixel (x, y)s. */
  private def pixelsOf(wkt: String): Seq[(Int, Int)] = {
    val row = sparkSession
      .sql(
        s"SELECT ST_Pixelize(ST_GeomFromWKT('$wkt'), 10, 10, ST_PolygonFromEnvelope(0, 0, 10, 10))")
      .collect()(0)
    row.getSeq[Pixel](0).map(p => (p.getX.toInt, p.getY.toInt))
  }

  describe("ST_Pixelize") {

    it("returns the containing pixel for a polygon smaller than a pixel (GH-1272)") {
      assert(pixelsOf("POLYGON ((3.1 3.1, 3.8 3.1, 3.8 3.8, 3.1 3.8, 3.1 3.1))") == Seq((3, 3)))
    }

    it("returns the containing pixel for a line shorter than a pixel") {
      assert(pixelsOf("LINESTRING (3.1 3.1, 3.8 3.8)") == Seq((3, 3)))
    }

    it("returns the same pixel for a sub-pixel polygon as for its vertex") {
      // Both inside and outside the boundary, a polygon that collapses to one pixel lands
      // exactly where a point at its first vertex does.
      Seq(
        ("POLYGON ((3.1 3.1, 3.8 3.1, 3.8 3.8, 3.1 3.8, 3.1 3.1))", "POINT (3.1 3.1)"),
        ("POLYGON ((0.1 9.1, 0.8 9.1, 0.8 9.8, 0.1 9.8, 0.1 9.1))", "POINT (0.1 9.1)"),
        ("POLYGON ((12.1 4.1, 12.8 4.1, 12.8 4.8, 12.1 4.8, 12.1 4.1))", "POINT (12.1 4.1)"))
        .foreach { case (polygon, vertex) =>
          assert(pixelsOf(polygon) == pixelsOf(vertex), polygon)
        }
    }

    it("returns one pixel per member of a multi-geometry in different pixels") {
      val polygons =
        "MULTIPOLYGON (((3.1 3.1, 3.8 3.1, 3.8 3.8, 3.1 3.8, 3.1 3.1)), " +
          "((7.1 7.1, 7.8 7.1, 7.8 7.8, 7.1 7.8, 7.1 7.1)))"
      assert(pixelsOf(polygons) == Seq((3, 3), (7, 7)))
      assert(
        pixelsOf("MULTILINESTRING ((3.1 3.1, 3.8 3.8), (7.1 7.1, 7.8 7.8))") == Seq(
          (3, 3),
          (7, 7)))
      assert(pixelsOf("MULTIPOINT ((3.5 3.5), (7.5 7.5))") == Seq((3, 3), (7, 7)))
    }

    it("pixelizes each member of a geometry collection") {
      val collection = "GEOMETRYCOLLECTION (POINT (1.5 1.5), " +
        "POLYGON ((3.1 3.1, 3.8 3.1, 3.8 3.8, 3.1 3.8, 3.1 3.1)), LINESTRING (7.1 7.1, 7.8 7.8))"
      assert(pixelsOf(collection) == Seq((1, 1), (3, 3), (7, 7)))
    }

    it("returns no pixels for empty geometries instead of failing") {
      Seq(
        "POINT EMPTY",
        "LINESTRING EMPTY",
        "POLYGON EMPTY",
        "MULTIPOINT EMPTY",
        "MULTILINESTRING EMPTY",
        "MULTIPOLYGON EMPTY",
        "GEOMETRYCOLLECTION EMPTY").foreach { wkt =>
        assert(pixelsOf(wkt).isEmpty, wkt)
      }
    }

    it("skips empty members of a collection") {
      assert(pixelsOf("GEOMETRYCOLLECTION (POLYGON EMPTY, POINT (3.5 3.5))") == Seq((3, 3)))
    }

    it("still traces the outline of a polygon spanning several pixels") {
      // The one-pixel fallback only applies when tracing draws nothing, so a larger polygon
      // keeps its traced outline: the 16 boundary pixels of the 5 x 5 block from (1, 1) to (5, 5).
      val outline = pixelsOf("POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1))")
      val expected = for {
        x <- 1 to 5
        y <- 1 to 5
        if x == 1 || x == 5 || y == 1 || y == 5
      } yield (x, y)
      assert(outline.size == 16)
      assert(outline.toSet == expected.toSet)
    }

    it("still traces a line spanning several pixels") {
      // Bresenham draws each segment's start pixel but not its end pixel.
      assert(pixelsOf("LINESTRING (1.5 1.5, 4.5 1.5)") == Seq((1, 1), (2, 1), (3, 1)))
    }

    it("pixelizes the reproduction from GH-1272 inside a query") {
      val count = sparkSession
        .sql("""
          |SELECT pixel FROM (
          |  SELECT ST_GeomFromWKT('POLYGON ((3.1 3.1, 3.8 3.1, 3.8 3.8, 3.1 3.8, 3.1 3.1))') AS geo
          |) LATERAL VIEW EXPLODE(ST_Pixelize(geo, 10, 10, ST_PolygonFromEnvelope(0, 0, 10, 10))) AS pixel
        """.stripMargin)
        .count()
      assert(count == 1)
    }
  }
}
