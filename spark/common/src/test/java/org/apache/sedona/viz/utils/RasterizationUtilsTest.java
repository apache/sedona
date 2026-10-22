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
package org.apache.sedona.viz.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;

/** Pixel coordinates of polygons on a 10 x 10 grid over (0, 0, 10, 10). */
public class RasterizationUtilsTest {

  private static final Envelope BOUNDARY = new Envelope(0, 10, 0, 10);

  private static final String EXTERIOR = "(0.5 0.5, 9.5 0.5, 9.5 9.5, 0.5 9.5, 0.5 0.5)";
  private static final String HOLE = "(4.5 4.5, 6.5 4.5, 6.5 6.5, 4.5 6.5, 4.5 4.5)";
  private static final String SECOND_HOLE = "(1.5 1.5, 2.5 1.5, 2.5 2.5, 1.5 2.5, 1.5 1.5)";

  private static Polygon polygon(String wkt) throws ParseException {
    return (Polygon) new WKTReader().read(wkt);
  }

  /** The outline pixels as sorted "x,y" strings, keeping duplicates. */
  private static List<String> outline(Polygon polygon) {
    return pixels(RasterizationUtils.FindPixelCoordinates(10, 10, BOUNDARY, polygon, false));
  }

  /** The filled pixels as sorted "x,y" strings, keeping duplicates. */
  private static List<String> fill(Polygon polygon) {
    return pixels(RasterizationUtils.FindPixelCoordinates(10, 10, BOUNDARY, polygon, false, 1.0));
  }

  private static List<String> pixels(List<Tuple2<Pixel, Double>> pixels) {
    List<String> result = new ArrayList<>();
    for (Tuple2<Pixel, Double> pixel : pixels) {
      result.add((int) pixel._1().getX() + "," + (int) pixel._1().getY());
    }
    Collections.sort(result);
    return result;
  }

  /** Every pixel (x, y) with lo <= x, y <= hi, as "x,y" strings. */
  private static Set<String> block(int lo, int hi) {
    Set<String> result = new TreeSet<>();
    for (int x = lo; x <= hi; x++) {
      for (int y = lo; y <= hi; y++) {
        result.add(x + "," + y);
      }
    }
    return result;
  }

  /** Every pixel (x, y) with x0 <= x <= x1 and y0 <= y <= y1, as "x,y" strings. */
  private static Set<String> rectangle(int x0, int x1, int y0, int y1) {
    Set<String> result = new TreeSet<>();
    for (int x = x0; x <= x1; x++) {
      for (int y = y0; y <= y1; y++) {
        result.add(x + "," + y);
      }
    }
    return result;
  }

  private static List<String> concat(List<String> first, List<String> second) {
    List<String> result = new ArrayList<>(first);
    result.addAll(second);
    Collections.sort(result);
    return result;
  }

  @Test
  public void outlineOfPolygonWithoutHolesIsItsExteriorRing() throws ParseException {
    Set<String> expected = block(0, 9);
    expected.removeAll(block(1, 8));
    List<String> pixels = outline(polygon("POLYGON (" + EXTERIOR + ")"));
    assertEquals(36, pixels.size());
    assertEquals(expected, new TreeSet<>(pixels));
  }

  @Test
  public void outlineOfPolygonWithHoleTracesEachRingSeparately() throws ParseException {
    List<String> exterior = outline(polygon("POLYGON (" + EXTERIOR + ")"));
    List<String> hole = outline(polygon("POLYGON (" + HOLE + ")"));
    List<String> pixels = outline(polygon("POLYGON (" + EXTERIOR + ", " + HOLE + ")"));

    assertEquals(concat(exterior, hole), pixels);
    assertEquals(44, pixels.size());
    // No segment joining the exterior ring's last vertex to the hole's first vertex
    assertFalse(pixels.contains("1,1"));
    assertFalse(pixels.contains("2,2"));
    assertFalse(pixels.contains("3,3"));
  }

  @Test
  public void outlineOfPolygonWithSeveralHolesTracesEachRingSeparately() throws ParseException {
    List<String> exterior = outline(polygon("POLYGON (" + EXTERIOR + ")"));
    List<String> hole = outline(polygon("POLYGON (" + HOLE + ")"));
    List<String> secondHole = outline(polygon("POLYGON (" + SECOND_HOLE + ")"));

    List<String> pixels =
        outline(polygon("POLYGON (" + EXTERIOR + ", " + HOLE + ", " + SECOND_HOLE + ")"));

    assertEquals(concat(concat(exterior, hole), secondHole), pixels);
  }

  @Test
  public void fillOfPolygonWithoutHolesCoversItsInterior() throws ParseException {
    assertEquals(new ArrayList<>(block(1, 8)), fill(polygon("POLYGON (" + EXTERIOR + ")")));
  }

  @Test
  public void fillOfPolygonWithHoleLeavesTheHoleOut() throws ParseException {
    Set<String> expected = block(1, 8);
    expected.removeAll(block(4, 6));

    List<String> pixels = fill(polygon("POLYGON (" + EXTERIOR + ", " + HOLE + ")"));

    assertEquals(new ArrayList<>(expected), pixels);
    // The interior on the line between the exterior ring and the hole is still filled
    assertTrue(pixels.contains("1,1"));
    assertTrue(pixels.contains("2,2"));
    assertTrue(pixels.contains("3,3"));
  }

  @Test
  public void fillOfPolygonWithSeveralHolesLeavesEachHoleOut() throws ParseException {
    Set<String> expected = block(1, 8);
    expected.removeAll(block(4, 6));
    expected.removeAll(block(1, 2));

    List<String> pixels =
        fill(polygon("POLYGON (" + EXTERIOR + ", " + HOLE + ", " + SECOND_HOLE + ")"));

    assertEquals(new ArrayList<>(expected), pixels);
  }

  @Test
  public void fillOfPolygonWithHoleSnappedOntoTheExteriorRing() throws ParseException {
    // In pixels the hole runs from (0, 2) to (4, 4), so its left edge lies on the exterior ring
    Set<String> expected = block(1, 8);
    expected.removeAll(rectangle(1, 4, 2, 4));

    List<String> pixels =
        fill(polygon("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (0.5 2, 4 2, 4 4, 0.5 4, 0.5 2))"));

    assertEquals(new ArrayList<>(expected), pixels);
  }

  @Test
  public void fillOfPolygonWithHolesSnappedOntoEachOther() throws ParseException {
    // In pixels the holes run from x = 2 to 3 and from x = 3 to 5, sharing the edge x = 3
    Set<String> expected = block(1, 8);
    expected.removeAll(rectangle(2, 5, 2, 3));

    List<String> pixels =
        fill(
            polygon(
                "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0),"
                    + " (2.1 2.1, 3.5 2.1, 3.5 3.5, 2.1 3.5, 2.1 2.1),"
                    + " (3.6 2.1, 5.5 2.1, 5.5 3.5, 3.6 3.5, 3.6 2.1))"));

    assertEquals(new ArrayList<>(expected), pixels);
  }

  @Test
  public void emptyPolygonHasNoPixels() throws ParseException {
    Polygon empty = polygon("POLYGON EMPTY");
    assertTrue(outline(empty).isEmpty());
    assertTrue(fill(empty).isEmpty());
  }
}
