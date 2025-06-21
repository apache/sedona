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

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polyline;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class PolylineGeographyTest {
  @Test
  public void testEncodedPolyline() throws IOException {
    // Create two points
    S2Point ptStart = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point ptEnd = S2LatLng.fromDegrees(0, 0).toPoint();

    // Prepare encoder output
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // Build a single polyline and wrap in geography
    List<S2Point> points = new ArrayList<>();
    points.add(ptStart);
    points.add(ptEnd);
    S2Polyline polyline = new S2Polyline(points);
    PolylineGeography geog = new PolylineGeography(polyline);

    // Encode the geography with tagging
    geog.encodeTagged(baos, new EncodeOptions());

    // Decode from the bytes
    byte[] encodedBytes = baos.toByteArray();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(encodedBytes));
    S2Geography roundtrip = S2Geography.decodeTagged(dis);

    // Verify kind
    assertEquals(S2Geography.GeographyKind.POLYLINE, roundtrip.kind);
    System.out.println(roundtrip.toString());
    S2Polyline pl = (S2Polyline) roundtrip.shape(0);
    StringBuilder sb = new StringBuilder("LINESTRING (");
    for (int i = 0; i < pl.numVertices(); i++) {
      S2Point p = pl.vertex(i);
      S2LatLng ll = new S2LatLng(p);
      if (i > 0) sb.append(", ");
      // WKT is “lon lat”
      sb.append(String.format("%.0f %.0f", ll.lng().degrees(), ll.lat().degrees()));
    }
    sb.append(")");
    System.out.println(sb.toString());
    // Verify WKT representation (assuming toWkt() is implemented)
    assertEquals("LINESTRING (-64 45, 0 0)", sb.toString());

    // Downcast and inspect internal polylines
    assertTrue(roundtrip instanceof PolylineGeography);
    PolylineGeography rtTyped = (PolylineGeography) roundtrip;
    assertEquals(1, rtTyped.getPolylines().size());
    S2Polyline decodedPolyline = rtTyped.getPolylines().get(0);

    // Compare geometry equality
    assertTrue(decodedPolyline.equals(polyline));
  }

  @Test
  public void testEncodedMultiPolyline() throws IOException {
    // create multiple polylines
    S2Point a = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point b = S2LatLng.fromDegrees(0, 0).toPoint();
    S2Point c = S2LatLng.fromDegrees(-30, 20).toPoint();
    S2Point d = S2LatLng.fromDegrees(10, -10).toPoint();

    S2Polyline poly1 = new S2Polyline(List.of(a, b));
    S2Polyline poly2 = new S2Polyline(List.of(c, d));

    // 2) Wrap both in a single geography
    PolylineGeography geog = new PolylineGeography(List.of(poly1, poly2));

    // 3) Encode to bytes
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, new EncodeOptions());

    // 4) Decode back
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    S2Geography decoded = S2Geography.decodeTagged(dis);

    // 5) Verify it’s a PolylineGeography with two members
    assertTrue(decoded instanceof PolylineGeography);
    PolylineGeography pg = (PolylineGeography) decoded;
    assertEquals(2, pg.getPolylines().size());

    // 6) Check each one matches the original
    assertTrue(pg.getPolylines().get(0).equals(poly1));
    assertTrue(pg.getPolylines().get(1).equals(poly2));
  }
}
