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

import java.nio.{ByteBuffer, ByteOrder}

import scala.util.{Random, Try}

import com.google.common.geometry.{S1Interval, S2LatLng, S2LatLngRect}
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

  /** The WKB of a point with any coordinates, also ones that are not numbers. */
  private def pointWkb(x: Double, y: Double): Array[Byte] =
    ByteBuffer
      .allocate(21)
      .order(ByteOrder.LITTLE_ENDIAN)
      .put(1.toByte)
      .putInt(1)
      .putDouble(x)
      .putDouble(y)
      .array()

  /**
   * The WKB of a line or a polygon with the vertices of `rings` as they are given: a ring is not
   * closed for the caller, and a coordinate may be no number.
   */
  private def shapeWkb(typeCode: Int, rings: Seq[(Double, Double)]*): Array[Byte] = {
    val size = 5 + (if (typeCode == 3) 4 else 0) + rings.map(4 + _.size * 16).sum
    val buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)
    buffer.put(1.toByte).putInt(typeCode)
    if (typeCode == 3) buffer.putInt(rings.size)
    rings.foreach { ring =>
      buffer.putInt(ring.size)
      ring.foreach { case (x, y) => buffer.putDouble(x).putDouble(y) }
    }
    buffer.array()
  }

  private def boxOf(wkbs: Array[Byte]*): Seq[Double] = {
    val bounds = new GeographyBounds
    wkbs.foreach(add(bounds, _))
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

  test("has points on both longitudes of the antimeridian in the box with other geographies") {
    val line = "LINESTRING (10 1, 20 2)"
    Seq(
      Seq("POINT (-180 55)", "POINT (180 80)", line),
      Seq(line, "POINT (180 80)", "POINT (-180 55)"),
      Seq("POINT (-180 55)", "POINT (180 80)"),
      Seq("MULTIPOINT ((180 80), (-180 55))", "POLYGON ((10 1, 20 1, 20 2, 10 1))")).foreach {
      wkts =>
        val found = box(wkts: _*)
        assert(contains(found, 180, 80) && contains(found, -180, 55), wkts)
    }
    val found = box("POINT (-180 55)", "POINT (180 80)", line)
    assert(contains(found, 10, 1) && contains(found, 20, 2))
    // From the line to the antimeridian, the shorter way
    assert(contains(found, 100, 30) && !contains(found, 0, 30) && !contains(found, -90, 30))
    // A point with no latitude has its longitude there as well
    val nan = Double.NaN
    val partial = boxOf(pointWkb(-180, nan), pointWkb(180, nan), wkb(line))
    assert(contains(partial, 180, 1) && contains(partial, -180, 2))
  }

  test("has every point and vertex in the box, also on the antimeridian and at a pole") {
    val random = new Random(7)
    val special =
      Seq(-180.0, 180.0, 0.0, -0.0, Math.nextDown(180.0), Math.nextUp(-180.0), 90.0)
    def longitude(): Double =
      if (random.nextInt(3) == 0) special(random.nextInt(special.size))
      else random.nextDouble() * 360 - 180
    def latitude(): Double = random.nextInt(8) match {
      case 0 => 90
      case 1 => -90
      case _ => random.nextDouble() * 180 - 90
    }
    (1 to 2000).foreach { _ =>
      // Every fourth time the points are on the antimeridian alone, with either longitude
      val antimeridian = random.nextInt(4) == 0
      val points = Seq.fill(random.nextInt(5))(
        (if (antimeridian) special(random.nextInt(2)) else longitude(), latitude()))
      val line = Seq.fill(if (random.nextBoolean()) 2 + random.nextInt(3) else 0)(
        (longitude(), random.nextDouble() * 160 - 80))
      val wkbs = points.map { case (x, y) =>
        pointWkb(x, y)
      } ++
        (if (line.isEmpty) Nil else Seq(shapeWkb(2, line)))
      if (wkbs.nonEmpty) {
        val found = boxOf(random.shuffle(wkbs): _*)
        (points ++ line).foreach { case (x, y) =>
          assert(contains(found, x, y), s"($x $y) of $points and $line in $found")
        }
      }
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

  test(
    "is the whole sphere for a ring that halves the sphere, whose inside Sedona reads both ways") {
    // The four vertices are on one great circle, so the two sides of the ring are of one size.
    // Sedona's functions do not agree on the side that is the inside of such a ring.
    val sphere = Seq(-180.0, 180.0, -90.0, 90.0)
    assert(box("POLYGON ((0 0, 90 10, 180 0, -90 -10, 0 0))") == sphere)
    assert(box("POLYGON ((0 0, -90 -10, 180 0, 90 10, 0 0))") == sphere)
    // The equator
    assert(box("POLYGON ((0 0, 90 0, 180 0, -90 0, 0 0))") == sphere)
  }

  test("has the edge that closes a ring whose last vertex is not its first") {
    // Sedona's own writer of the WKB of a geography leaves the last vertex of a ring out. The
    // edge between the two vertices on the 50th parallel reaches atan(tan(50) / cos(60)).
    val north = math.toDegrees(math.atan(math.tan(math.toRadians(50)) / 0.5))
    val open = boxOf(shapeWkb(3, Seq((-60.0, 50.0), (0.0, -10.0), (60.0, 50.0))))
    assert(open(3) >= north && open(3) < north + 1e-6)
    assert(open == box("POLYGON ((-60 50, 0 -10, 60 50, -60 50))"))
    // A ring around the pole, which takes the other way to its bounds
    val pole = boxOf(shapeWkb(3, Seq((0.0, 80.0), (90.0, 80.0), (180.0, 80.0), (-90.0, 80.0))))
    assert(pole == box("POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))"))
    assert(pole(3) == 90)
    // The line of the same vertices has no such edge
    val line = boxOf(shapeWkb(2, Seq((-60.0, 50.0), (0.0, -10.0), (60.0, 50.0))))
    assert(line(3) < 51)
  }

  test("takes a coordinate that is a number from a point whose other coordinate is none") {
    val nan = Double.NaN
    assert(boxOf(pointWkb(75, nan), pointWkb(10, 20)) == Seq(10.0, 75.0, 20.0, 20.0))
    assert(boxOf(pointWkb(nan, 30), pointWkb(10, 20)) == Seq(10.0, 10.0, 20.0, 30.0))
    assert(boxOf(pointWkb(75, nan), pointWkb(nan, 30)) == Seq(75.0, 75.0, 30.0, 30.0))
    // A box has both dimensions
    assert(boxOf(pointWkb(75, nan)) == null)
    assert(boxOf(pointWkb(nan, 30)) == null)
    // With a line, whose bounds S2 gives
    val withLine = boxOf(pointWkb(75, nan), wkb("LINESTRING (10 20, 30 40)"))
    assert(near(withLine(0), 10) && near(withLine(1), 75))
    assert(near(withLine(2), 20) && near(withLine(3), 40))
    // A vertex of a line, which takes no part in the edges of the line
    val vertex = boxOf(shapeWkb(2, Seq((10.0, 20.0), (75.0, nan), (30.0, 40.0))))
    assert(near(vertex(0), 10) && near(vertex(1), 75))
    assert(near(vertex(2), 20) && near(vertex(3), 40))
  }

  private def meet(a: Seq[Double], b: Seq[Double]): Boolean = {
    val forth = GeographyBounds.intersects(a.toArray, b.toArray)
    assert(GeographyBounds.intersects(b.toArray, a.toArray) == forth)
    forth
  }

  test("tells whether two boxes have a point in common") {
    val box = Seq(10.0, 20.0, 30.0, 40.0)
    assert(meet(box, Seq(15.0, 16.0, 35.0, 36.0)))
    assert(meet(box, Seq(0.0, 50.0, 0.0, 50.0)))
    assert(meet(box, Seq(19.0, 25.0, 39.0, 45.0)))
    // A bound in common is a point in common
    assert(meet(box, Seq(20.0, 25.0, 40.0, 45.0)))
    assert(!meet(box, Seq(21.0, 25.0, 30.0, 40.0)))
    assert(!meet(box, Seq(10.0, 20.0, 41.0, 45.0)))
    assert(!meet(box, Seq(-20.0, -10.0, 30.0, 40.0)))
  }

  test("tells whether boxes across the antimeridian have a point in common") {
    val across = Seq(170.0, -170.0, 0.0, 10.0)
    assert(meet(across, Seq(175.0, 176.0, 1.0, 2.0)))
    assert(meet(across, Seq(-176.0, -175.0, 1.0, 2.0)))
    assert(meet(across, Seq(160.0, 170.0, 1.0, 2.0)))
    assert(meet(across, Seq(175.0, -175.0, 1.0, 2.0)))
    assert(meet(across, Seq(-175.0, 175.0, 1.0, 2.0)))
    assert(!meet(across, Seq(-169.0, 169.0, 1.0, 2.0)))
    assert(!meet(across, Seq(0.0, 10.0, 1.0, 2.0)))
    assert(!meet(across, Seq(175.0, 176.0, 11.0, 12.0)))
    // Longitude 180 and longitude -180 are one meridian
    assert(meet(Seq(170.0, 180.0, 0.0, 10.0), Seq(-180.0, -170.0, 0.0, 10.0)))
    assert(meet(Seq(180.0, 180.0, 0.0, 10.0), Seq(-180.0, -180.0, 5.0, 6.0)))
    // All longitudes
    assert(meet(Seq(-180.0, 180.0, 0.0, 10.0), Seq(175.0, -175.0, 1.0, 2.0)))
    assert(meet(Seq(-180.0, 180.0, 0.0, 10.0), Seq(-180.0, 180.0, 10.0, 20.0)))
  }

  test("grows a box by a distance on the sphere") {
    val degree = math.toRadians(1)
    val grown = GeographyBounds.expand(Array(10.0, 20.0, 30.0, 40.0), degree).toSeq
    assert(contains(grown, 10, 29.01) && contains(grown, 20, 40.99))
    assert(!contains(grown, 10, 28.9) && !contains(grown, 20, 41.1))
    // A degree of distance is more than a degree of longitude away from the equator
    assert(grown(0) < 9 && grown(0) > 8 && grown(1) > 21 && grown(1) < 22)
    // Nothing is added for no distance
    assert(
      GeographyBounds
        .expand(Array(10.0, 20.0, 30.0, 40.0), 0)
        .toSeq
        .zip(Seq(10.0, 20.0, 30.0, 40.0))
        .forall { case (found, expected) => near(found, expected) })
    // Across the antimeridian, over a pole, and around the sphere
    val across = GeographyBounds.expand(Array(179.5, 179.9, 0.0, 1.0), degree).toSeq
    assert(across(0) > across(1) && contains(across, -179.5, 0.5) && !contains(across, -178, 0.5))
    val polar = GeographyBounds.expand(Array(10.0, 20.0, 88.0, 89.5), degree).toSeq
    assert(polar == Seq(-180.0, 180.0, polar(2), 90.0) && polar(2) < 87.01 && polar(2) > 86.9)
    assert(
      GeographyBounds.expand(Array(10.0, 20.0, 30.0, 40.0), 4).toSeq ==
        Seq(-180.0, 180.0, -90.0, 90.0))
  }

  test("has every point within a distance of a geography in the box grown by that distance") {
    val random = new Random(23)
    (1 to 2000).foreach { _ =>
      val center =
        S2LatLng.fromDegrees(random.nextDouble() * 178 - 89, random.nextDouble() * 360 - 180)
      val box = boxOf(pointWkb(center.lngDegrees(), center.latDegrees()))
      val radius = math.pow(10, random.nextDouble() * 4 - 4) * math.Pi
      val grown = GeographyBounds.expand(box.toArray, radius).toSeq
      // A point on the circle of that radius around the center
      val bearing = random.nextDouble() * 2 * math.Pi
      val (lat, lng) = (center.latRadians(), center.lngRadians())
      val reachedLat =
        math.asin(
          math.sin(lat) * math.cos(radius) + math.cos(lat) * math.sin(radius) * math.cos(bearing))
      val reachedLng = lng + math.atan2(
        math.sin(bearing) * math.sin(radius) * math.cos(lat),
        math.cos(radius) - math.sin(lat) * math.sin(reachedLat))
      val reached = S2LatLng.fromRadians(reachedLat, reachedLng).normalized()
      assert(
        contains(grown, reached.lngDegrees(), reached.latDegrees()),
        s"$reached, ${math.toDegrees(radius)} degrees from $center, in $grown")
    }
  }
}
