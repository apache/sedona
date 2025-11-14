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
package org.apache.sedona.common.S2Geography;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.geometry.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class PointGeographyTest {
  @Test
  public void testEncodeTag() throws IOException {
    // 1) Create an empty geography
    PointGeography geog = new PointGeography();
    assertEquals(Geography.GeographyKind.POINT, geog.kind);
    assertEquals(0, geog.numShapes());
    // Java returns -1 for no shapes; if yours returns 0, adjust accordingly
    assertEquals(-1, geog.dimension());
    assertTrue(geog.getPoints().isEmpty());

    // 2) Encode into a ByteArrayOutputStream
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, new EncodeOptions());
    byte[] data = baos.toByteArray();
    assertEquals(8, data.length);

    // 2) Create a single-point geography at lat=45°, lng=-64°
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    PointGeography geog2 = new PointGeography(pt);
    assertEquals(1, geog2.numShapes());
    assertEquals(0, geog2.dimension());
    List<S2Point> originalPts = geog2.getPoints();
    assertEquals(1, originalPts.size());
    assertEquals(pt, originalPts.get(0));

    // 3) EncodeTagged
    ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    geog2.encodeTagged(baos2, new EncodeOptions());
    byte[] data2 = baos2.toByteArray();
    // should be >4 bytes (header+payload)
    assertTrue(data2.length > 4);
  }

  @Test
  public void testEmptyPointEncodeDecode() throws IOException {
    // 1) Create an empty geography
    PointGeography geog = new PointGeography();
    TestHelper.assertRoundTrip(geog, new EncodeOptions());
  }

  @Test
  public void testEncodedPoint() throws IOException {
    // 1) Create a single-point geography at lat=45°, lng=-64°
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    PointGeography geog = new PointGeography(pt);
    TestHelper.assertRoundTrip(geog, new EncodeOptions());
  }

  @Test
  public void testEncodedSnappedPoint() throws IOException {
    // 1) Build the original point and its snapped-to-cell-center version
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    S2CellId cellId = S2CellId.fromPoint(pt);
    S2Point ptSnapped = cellId.toPoint();

    // 2) EncodeTagged in COMPACT mode
    PointGeography geog = new PointGeography(ptSnapped);
    EncodeOptions opts = new EncodeOptions();
    opts.setCodingHint(EncodeOptions.CodingHint.COMPACT);
    TestHelper.assertRoundTrip(geog, opts);
  }

  @Test
  public void testEncodedListPoints() throws IOException {
    // 1) Build two points
    S2Point pt1 = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point pt2 = S2LatLng.fromDegrees(70, -40).toPoint();

    // 2) Encode both points
    PointGeography geog = new PointGeography(List.of(pt1, pt2));
    EncodeOptions opts = new EncodeOptions();
    opts.setCodingHint(EncodeOptions.CodingHint.COMPACT);
    TestHelper.assertRoundTrip(geog, opts);
  }

  @Test
  public void testPointCoveringEnabled() throws IOException {
    // single point geography
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    PointGeography geo = new PointGeography(pt);
    EncodeOptions opts = new EncodeOptions();
    opts.setIncludeCovering(true);

    // should write a non-zero coveringSize
    TestHelper.assertCovering(geo, opts);
  }

  @Test
  public void testPointCoveringDisabled() throws IOException {
    // single point geography
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    PointGeography geo = new PointGeography(pt);
    EncodeOptions opts = new EncodeOptions();
    opts.setIncludeCovering(false);

    // should write coveringSize == 0
    TestHelper.assertCovering(geo, opts);
  }

  @Test
  public void testSmallPointUnionCovering() throws IOException {
    // fewer than 10 points: each point should produce one cell
    List<S2Point> pts = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      pts.add(S2LatLng.fromDegrees(i * 10, i * 5).toPoint());
    }
    PointGeography geo = new PointGeography(pts);
    List<S2CellId> cells = new ArrayList<>();
    geo.getCellUnionBound(cells);
    assertEquals("Should cover each point individually", pts.size(), cells.size());
    // ensure each cell's center matches the original point upon decoding
    for (int i = 0; i < pts.size(); i++) {
      S2CellId center = new S2CellId(cells.get(i).id());
      assertEquals("Cell center should round-trip point", S2CellId.fromPoint(pts.get(i)), center);
    }
  }

  @Test
  public void testLargePointUnionCovering() {
    // 1) Build 100 distinct points
    List<S2Point> pts = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      double lat = i * 0.5 - 25;
      double lng = (i * 3.6) - 180;
      pts.add(S2LatLng.fromDegrees(lat, lng).toPoint());
    }

    // 2) Create your geography
    PointGeography geo = new PointGeography(pts);

    // 3) Ask it for its cell-union bound
    List<S2CellId> cover = new ArrayList<>();
    geo.getCellUnionBound(cover);

    // 4) Check the size is non-zero (or == some expected value)
    assertTrue("Covering size should be > 0", cover.size() > 0);

    // 5) Verify *every* input point lies in at least one covering cell
    for (S2Point p : pts) {
      boolean covered = false;
      for (S2CellId cid : cover) {
        S2Cell cell = new S2Cell(cid);
        if (cell.contains(p)) {
          covered = true;
          break;
        }
      }
      assertTrue("Point " + p + " was not covered by any cell", covered);
    }
  }
}
