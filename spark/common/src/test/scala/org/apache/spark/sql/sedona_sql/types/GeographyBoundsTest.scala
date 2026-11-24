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
package org.apache.spark.sql.sedona_sql.types

import scala.util.{Random, Try}

import com.google.common.geometry.{S1Interval, S2LatLngRect}
import org.apache.sedona.common.S2Geography.{WKBReader => GeographyReader}
import org.locationtech.jts.io.{ByteOrderValues, WKBWriter, WKTReader}
import org.scalatest.funsuite.AnyFunSuite

class GeographyBoundsTest extends AnyFunSuite {
  private def wkb(wkt: String, byteOrder: Int = ByteOrderValues.LITTLE_ENDIAN): Array[Byte] =
    new WKBWriter(2, byteOrder).write(new WKTReader().read(wkt))

  private def add(bounds: GeographyBounds, bytes: Array[Byte], offset: Int = 0): Unit =
    bounds.add(bytes, offset)

  /** The box of the geographies of `wkts`: west, east, south, north. Null without one. */
  private def box(wkts: String*): Seq[Double] = {
    val bounds = new GeographyBounds
    wkts.foreach(wkt => add(bounds, wkb(wkt)))
    Option(bounds.box()).map(_.toSeq).orNull
  }

  private def contains(box: Seq[Double], x: Double, y: Double): Boolean = {
    val Seq(west, east, south, north) = box
    val inX = if (west <= east) x >= west && x <= east else x >= west || x <= east
    inX && y >= south && y <= north
  }

  private def near(actual: Double, expected: Double): Boolean =
    math.abs(actual - expected) < 1e-9

  test("has no box without a geography, and none for empty geographies") {
    assert(box() == null)
    assert(box("POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY") == null)
    assert(box("GEOMETRYCOLLECTION EMPTY", "MULTIPOINT EMPTY") == null)
  }

  test("gives the box of points in their own coordinates") {
    assert(box("POINT (10.1 20.2)") == Seq(10.1, 10.1, 20.2, 20.2))
    assert(
      box("POINT (10.1 20.2)", "POINT (-30.3 5)", "POINT (0 -40.4)") ==
        Seq(-30.3, 10.1, -40.4, 20.2))
    assert(box("POINT (-10 1)", "POINT (-30 2)") == Seq(-30.0, -10.0, 1.0, 2.0))
    assert(box("POINT (1e-10 1)", "POINT (-1e-10 2)") == Seq(-1e-10, 1e-10, 1.0, 2.0))
  }

  test("reads a point in either byte order, from an offset, and with more coordinates") {
    val bounds = new GeographyBounds
    add(bounds, wkb("POINT (1 2)", ByteOrderValues.BIG_ENDIAN))
    add(bounds, Array[Byte](9, 9, 9, 9) ++ wkb("POINT (3 4)"), offset = 4)
    // ISO WKB of POINT ZM (5 6 7 8)
    val pointZM = new WKBWriter(4, ByteOrderValues.LITTLE_ENDIAN)
      .write(new WKTReader().read("POINT ZM (5 6 7 8)"))
    val iso = PublicWkbHeaderAdapter.toIsoWkb(pointZM)
    add(bounds, iso)
    assert(bounds.box().toSeq == Seq(1.0, 5.0, 2.0, 6.0))
  }

  test("takes the shorter way around the sphere for points on both sides of the antimeridian") {
    assert(box("POINT (179 1)", "POINT (-179 2)") == Seq(179.0, -179.0, 1.0, 2.0))
    assert(
      box("POINT (170 1)", "POINT (-160 2)", "POINT (175 3)", "POINT (-175 0)") ==
        Seq(170.0, -160.0, 0.0, 3.0))
    // The way through the prime meridian is the shorter one here
    assert(box("POINT (60 1)", "POINT (-60 2)") == Seq(-60.0, 60.0, 1.0, 2.0))
  }

  test("has a point on the antimeridian in the box as longitude 180 and as -180") {
    Seq(Seq("POINT (180 5)"), Seq("POINT (-180 5)"), Seq("POINT (-180 5)", "POINT (175 6)"))
      .foreach { wkts =>
        val found = box(wkts: _*)
        assert(contains(found, 180, 5) && contains(found, -180, 5), wkts)
        assert(!contains(found, 0, 5) && !contains(found, 170, 5), wkts)
      }
  }

  test("follows the edges of a line") {
    val found = box("LINESTRING (0 50, 90 50)")
    val north = math.toDegrees(math.atan(math.tan(math.toRadians(50)) / math.cos(math.Pi / 4)))
    assert(found(3) >= north && found(3) < north + 1e-6)
    assert(found(0) <= 0 && near(found(0), 0) && found(1) >= 90 && near(found(1), 90))
    assert(found(2) <= 50 && near(found(2), 50))
  }

  test("crosses the antimeridian for a line across it") {
    val found = box("LINESTRING (170 10, -170 20)")
    assert(found(0) > found(1))
    assert(found(0) <= 170 && near(found(0), 170) && found(1) >= -170 && near(found(1), -170))
  }

  test("reaches the pole and all longitudes for a polygon around a pole") {
    val north = box("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))")
    assert(north(0) == -180 && north(1) == 180 && north(3) == 90)
    assert(north(2) <= 80 && near(north(2), 80))
    val south = box("POLYGON ((0 -80, -90 -80, 180 -80, 90 -80, 0 -80))")
    assert(south(0) == -180 && south(1) == 180 && south(2) == -90)
    assert(south(3) >= -80 && near(south(3), -80))
  }

  test("has the vertices of lines and polygons in the box as they are written") {
    val wkts = Seq(
      "LINESTRING (10.1 20.2, 30.3 40.4)",
      "POLYGON ((-5.5 -5.5, 7.7 -5.5, 7.7 8.8, -5.5 -5.5))",
      "MULTIPOINT ((100.1 -60.6), (-120.2 70.7))")
    val found = box(wkts: _*)
    Seq((10.1, 20.2), (30.3, 40.4), (-5.5, -5.5), (7.7, 8.8), (100.1, -60.6), (-120.2, 70.7))
      .foreach { case (x, y) => assert(contains(found, x, y), s"($x $y)") }
  }

  test("joins the points with the other geographies") {
    val found = box("POINT (-20 -30)", "LINESTRING (10 20, 30 40)")
    assert(contains(found, -20, -30) && contains(found, 30, 40))
    assert(near(found(0), -20) && near(found(1), 30) && near(found(2), -30) && near(found(3), 40))
  }

  test("starts again after a reset") {
    val bounds = new GeographyBounds
    add(bounds, wkb("POINT (1 2)"))
    add(bounds, wkb("LINESTRING (170 10, -170 20)"))
    bounds.reset()
    assert(bounds.box() == null)
    add(bounds, wkb("POINT (3 4)"))
    assert(bounds.box().toSeq == Seq(3.0, 3.0, 4.0, 4.0))
  }

  test("has no box once a geography cannot be read, until a reset") {
    val bounds = new GeographyBounds
    add(bounds, wkb("POINT (1 2)"))
    // A line that states two points and has the coordinates of none
    bounds.add(wkb("LINESTRING (0 0, 1 1)").take(9), 0)
    add(bounds, wkb("POINT (3 4)"))
    assert(bounds.box() == null)
    bounds.reset()
    add(bounds, wkb("POINT (3 4)"))
    assert(bounds.box().toSeq == Seq(3.0, 3.0, 4.0, 4.0))
  }

  /**
   * The bounds that Sedona's reading of `wkt` into its S2 form gives, which is what the functions
   * and the predicates of a geography work on. None for a geography that it does not read.
   */
  private def reference(wkt: String): Option[S2LatLngRect] =
    Try(new GeographyReader().read(wkb(wkt)).region().getRectBound).toOption

  /**
   * Asserts that the box of `wkt` has all of its reference, and no more than `slack` degrees on
   * any side.
   *
   * @return
   *   whether there is a reference for `wkt`
   */
  private def assertCovers(wkt: String, slack: Double): Boolean = reference(wkt).exists { rect =>
    val found = box(wkt)
    assert(found != null, wkt)
    val Seq(west, east, south, north) = found
    assert(
      south <= rect.latLo().degrees() && north >= rect.latHi().degrees(),
      s"$found $rect $wkt")
    assert(
      rect.latLo().degrees() - south <= slack && north - rect.latHi().degrees() <= slack,
      wkt)
    val longitudes =
      if (west == -180 && east == 180) S1Interval.full()
      else new S1Interval(math.toRadians(west), math.toRadians(east))
    assert(longitudes.contains(rect.lng()), s"$found $rect $wkt")
    assert(
      math.toDegrees(longitudes.getLength - rect.lng().getLength) <= 2 * slack,
      s"$found $rect $wkt")
    true
  }

  private def wrap(longitude: Double): Double =
    if (longitude > 180) longitude - 360 else if (longitude < -180) longitude + 360 else longitude

  private def text(vertices: Seq[(Double, Double)]): String =
    vertices.map { case (x, y) => s"${wrap(x)} $y" }.mkString("(", ", ", ")")

  /** A ring around a centre, with its vertices in the order of their direction from it. */
  private def ring(random: Random, radius: Double): Seq[(Double, Double)] = {
    val longitude = random.nextDouble() * 360 - 180
    val latitude = (random.nextDouble() * 2 - 1) * (89 - radius)
    val count = 3 + random.nextInt(12)
    val open = (0 until count).map { i =>
      val angle = 2 * math.Pi * (i + random.nextDouble() * 0.8) / count
      val distance = radius * (0.3 + 0.7 * random.nextDouble())
      (longitude + distance * math.cos(angle), latitude + distance * math.sin(angle))
    }
    val vertices = if (random.nextBoolean()) open else open.reverse
    vertices :+ vertices.head
  }

  test("has the bounds that the S2 form of lines gives, wherever they are") {
    val random = new Random(7)
    val compared = (0 until 300).count { _ =>
      val vertices = Seq.fill(2 + random.nextInt(6))(
        (random.nextDouble() * 360 - 180, random.nextDouble() * 178 - 89))
      assertCovers(s"LINESTRING ${text(vertices)}", slack = 1e-9)
    }
    assert(compared == 300)
  }

  test("has the bounds that the S2 form of polygons gives, in either direction of the ring") {
    val random = new Random(11)
    Seq(1.0, 10.0, 40.0, 80.0).foreach { radius =>
      val compared = (0 until 150).count { _ =>
        assertCovers(s"POLYGON (${text(ring(random, radius))})", slack = 1e-9)
      }
      // Sedona may not read a ring that crosses itself on the sphere
      assert(compared > 120, s"radius $radius: $compared")
    }
  }

  test("has the bounds of a polygon whose ring is around more than half of the sphere") {
    // A band from 60 degrees south to 60 degrees north over 300 degrees of longitude. The inside
    // is the smaller side of the ring: both poles and the longitudes beyond 150.
    val band = "POLYGON ((-150 -60, -90 -60, -30 -60, 30 -60, 90 -60, 150 -60, " +
      "150 60, 90 60, 30 60, -30 60, -90 60, -150 60, -150 -60))"
    assert(box(band) == Seq(-180.0, 180.0, -90.0, 90.0))
    assertCovers(band, slack = 1e-9)
    // An edge takes the shorter way between its vertices, so this ring is across the
    // antimeridian and 60 degrees of longitude wide
    val narrow = "POLYGON ((-150 -80, 150 -80, 150 80, -150 80, -150 -80))"
    assert(box(narrow).head > box(narrow)(1))
    assertCovers(narrow, slack = 1e-9)
    // A ring along the tenth parallel, around the northern side
    assertCovers("POLYGON ((0 10, 90 10, 180 10, -90 10, 0 10))", slack = 1e-9)
    assertCovers("POLYGON ((0 10, -90 10, 180 10, 90 10, 0 10))", slack = 1e-9)
  }

  test("is within the first ring of a polygon with holes, and has every member of a collection") {
    // A hole takes nothing from the bounds here
    assertCovers("POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 10, 10 10, 10 5, 5 5))", 1e-9)
    assertCovers(
      "MULTIPOLYGON (((0 0, 20 0, 20 20, 0 0)), ((170 -10, -170 -10, -170 5, 170 -10)))",
      1e-9)
    assertCovers(
      "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 50 60), " +
        "GEOMETRYCOLLECTION (POLYGON ((-5 -5, 7 -5, 7 8, -5 -5))))",
      1e-9)
    assertCovers("MULTILINESTRING ((0 50, 90 50), (-100 -20, -110 -30))", 1e-9)
  }
}
