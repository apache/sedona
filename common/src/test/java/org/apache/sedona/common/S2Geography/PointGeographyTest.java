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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
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
    assertEquals(S2Geography.GeographyKind.POINT, geog.kind);
    assertEquals(0, geog.numShapes());
    // Java returns -1 for no shapes; if yours returns 0, adjust accordingly
    assertEquals(-1, geog.dimension());
    assertTrue(geog.getPoints().isEmpty());

    // 2) Encode into a ByteArrayOutputStream
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, new EncodeOptions());
    byte[] data = baos.toByteArray();
    assertEquals(5, data.length);

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
    assertTrue(data2.length > 5);
  }

  @Test
  public void testEmptyPointEncodeDecode() throws IOException {
    // 1) Create an empty geography
    PointGeography geog = new PointGeography();
    assertEquals(S2Geography.GeographyKind.POINT, geog.kind);
    assertEquals(0, geog.numShapes());
    // Java returns -1 for no shapes; if yours returns 0, adjust accordingly
    assertEquals(-1, geog.dimension());
    assertTrue(geog.getPoints().isEmpty());

    // 2) EncodeTagged
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, new EncodeOptions());
    byte[] data = baos.toByteArray();

    // 3) DecodeTagged
    ByteArrayInputStream din = new ByteArrayInputStream(data);
    // 3b) Now delegate to the dispatch method that takes (DataInputStream, EncodeTag)
    S2Geography decoded = geog.decodeTagged(din);

    assertTrue(decoded instanceof PointGeography);
    PointGeography round = (PointGeography) decoded;
    assertTrue(round.getPoints().isEmpty());

    // 4) region() should be an empty cap
    S2Region region = round.region();
    assertTrue(region instanceof S2Cap);
    assertTrue(((S2Cap) region).isEmpty());
  }

  @Test
  public void testEncodedPoint() throws IOException {
    // 1) Create a single-point geography at lat=45°, lng=-64°
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    System.out.println(pt.toString());
    PointGeography geog = new PointGeography(pt);
    assertEquals(1, geog.numShapes());
    assertEquals(0, geog.dimension());
    List<S2Point> originalPts = geog.getPoints();
    assertEquals(1, originalPts.size());
    assertEquals(pt, originalPts.get(0));

    // 2) EncodeTagged
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, new EncodeOptions());
    byte[] data = baos.toByteArray();

    // 3) DecodeTagged
    ByteArrayInputStream din = new ByteArrayInputStream(data);
    PointGeography decoded = (PointGeography) geog.decodeTagged(din);
    assertTrue(decoded instanceof PointGeography);

    // 4) Verify everything round-tripped
    assertEquals(1, decoded.numShapes());
    assertEquals(0, decoded.dimension());
    // 4) Now round-trip the entire geography
    System.out.println(decoded.getPoints().toString());

    // It should still be a single‐point geography
    assertEquals(1, decoded.getPoints().size());

    // 1) Get the point and turn it into WKT with 6 decimal places:
    S2Point p = ((PointGeography) decoded).getPoints().get(0);
    S2LatLng ll = new S2LatLng(p);
    String wkt =
        String.format(
            "POINT (%.6f %.6f)",
            ll.lng().degrees(), // longitude first
            ll.lat().degrees() // then latitude
            );
    System.out.println(wkt);
    assertEquals("POINT (-64.000000 45.000000)", wkt);
    // 5) region() should contain exactly that point
    S2Region region = decoded.region();
    // Single-point region can be represented as a tiny cap containing only pt
    assertTrue(region.contains(pt));
  }

  @Test
  public void testEncodedSnappedPoint() throws IOException {
    // 1) Build the original point and its snapped-to-cell-center version
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    S2CellId cellId = S2CellId.fromPoint(pt);
    S2Point ptSnapped = cellId.toPoint();
    System.out.println(ptSnapped.toString());

    // 2) EncodeTagged in COMPACT mode
    PointGeography geog = new PointGeography(ptSnapped);
    EncodeOptions opts = new EncodeOptions();
    opts.setCodingHint(EncodeOptions.CodingHint.COMPACT);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();

    // Exactly 5-byte header + 8-byte cell-id
    assertEquals(13, data.length);

    // 3) Peek at the tag + covering
    Input in1 = new Input(data);

    // Read and decode the 4-byte header
    EncodeTag tag = EncodeTag.decode(in1);
    assertEquals(S2Geography.GeographyKind.CELL_CENTER, tag.getKind());
    assertEquals(1, tag.getCoveringSize() & 0xFF);

    // Read the single cell in the covering
    List<S2CellId> cover = new ArrayList<>();
    tag.decodeCovering(in1, cover);
    assertEquals(1, cover.size());
    // Covering must contain the original unsnapped cell
    assertEquals(cellId, cover.get(0));

    // 4) Now round-trip the entire geography
    PointGeography round = (PointGeography) geog.decodeTagged(new ByteArrayInputStream(data));
    System.out.println(round.getPoints().toString());

    // It should still be a single‐point geography
    assertEquals(1, round.getPoints().size());
    // And the point should be exactly the snapped cell‐center
    assertEquals(ptSnapped, round.getPoints().get(0));
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

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();

    // 3) Decode round-trip
    PointGeography round =
        (PointGeography) geog.decodeTagged(new UnsafeInput(new ByteArrayInputStream(data)));

    // 4) Assert round-trip matches
    assertEquals(2, round.getPoints().size());
    assertEquals(geog.getPoints(), round.getPoints());
    assertEquals(pt1, round.getPoints().get(0));
    assertEquals(pt2, round.getPoints().get(1));
  }
}
