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

import com.esotericsoftware.kryo.io.UnsafeInput;
import com.google.common.geometry.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class TestHelper {

  public static void assertRoundTrip(S2Geography original, EncodeOptions opts) throws IOException {
    // 1) Encode to bytes
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    original.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();

    // 2) Decode back
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    S2Geography decoded = original.decodeTagged(in);

    // 3) Compare kind, shapes, dimension
    assertEquals("Kind should round-trip", original.kind, decoded.kind);
    assertEquals("Shape count should round-trip", original.numShapes(), decoded.numShapes());
    assertEquals("Dimension should round-trip", original.dimension(), decoded.dimension());

    // 4) Geometry-specific checks + region containment of each vertex
    if (original instanceof PointGeography && decoded instanceof PointGeography) {
      List<S2Point> ptsOrig = ((PointGeography) original).getPoints();
      List<S2Point> ptsDec = ((PointGeography) decoded).getPoints();
      assertEquals("Point list size", ptsOrig.size(), ptsDec.size());
      assertEquals("Point coordinates", ptsOrig, ptsDec);
      ptsOrig.forEach(
          p -> assertTrue("Region should contain point " + p, decoded.region().contains(p)));

    } else if (original instanceof PolylineGeography && decoded instanceof PolylineGeography) {
      List<S2Polyline> a = ((PolylineGeography) original).getPolylines();
      List<S2Polyline> b = ((PolylineGeography) decoded).getPolylines();
      assertEquals("Polyline list size mismatch", a.size(), b.size());
      for (int i = 0; i < a.size(); i++) {
        S2Polyline pOrig = a.get(i);
        S2Polyline pDec = b.get(i);
        assertEquals(
            "Vertex count mismatch in polyline[" + i + "]",
            pOrig.numVertices(),
            pDec.numVertices());
        for (int v = 0; v < pOrig.numVertices(); v++) {
          assertEquals(
              "Vertex coordinate mismatch at polyline[" + i + "] vertex[" + v + "]",
              pOrig.vertex(v),
              pDec.vertex(v));
        }
      }

    } else if (original instanceof PolygonGeography && decoded instanceof PolygonGeography) {
      PolygonGeography a = (PolygonGeography) original;
      PolygonGeography b = (PolygonGeography) decoded;

      S2Polygon pgOrig = a.polygon;
      S2Polygon pgDec = b.polygon;
      assertEquals(
          "Loop count mismatch in polygon[" + 1 + "]", pgOrig.numLoops(), pgDec.numLoops());
      S2Loop loopOrig = pgOrig.loop(0);
      S2Loop loopDec = pgDec.loop(0);
      assertEquals(
          "Vertex count mismatch in loop[" + 1 + "] of polygon[" + 1 + "]",
          loopOrig.numVertices(),
          loopDec.numVertices());
      for (int v = 0; v < loopOrig.numVertices(); v++) {
        assertEquals(
            "Vertex mismatch at polygon[" + 1 + "] loop[" + 1 + "] vertex[" + v + "]",
            loopOrig.vertex(v),
            loopDec.vertex(v));
      }
    }
  }

  /**
   * Asserts that the EncodeTag for the given geography honors the includeCovering option; if
   * includeCovering==true, coveringSize should be >0, otherwise it must be zero.
   */
  public static void assertCovering(S2Geography original, EncodeOptions opts) throws IOException {
    // encode and read only the tag
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    original.encodeTagged(baos, opts);
    UnsafeInput in = new UnsafeInput(new ByteArrayInputStream(baos.toByteArray()));
    EncodeTag tag = EncodeTag.decode(in);
    int cov = tag.getCoveringSize() & 0xFF;
    if (opts.isIncludeCovering()) {
      assertTrue("Expected coveringSize>0 when includeCovering=true, got " + cov, cov > 0);
    } else {
      assertEquals("Expected coveringSize==0 when includeCovering=false", 0, cov);
    }
  }

  /** Converts an S2Point into a 6-decimal-place WKT POINT string. */
  public static String toPointWkt(S2Point p) {
    S2LatLng ll = new S2LatLng(p);
    return String.format("POINT (%.6f %.6f)", ll.lng().degrees(), ll.lat().degrees());
  }
  /** Converts an S2Polyline into a 0-decimal-place WKT LINESTRING string. */
  public static String toPolylineWkt(S2Polyline pl) {
    StringBuilder sb = new StringBuilder("LINESTRING (");
    for (int i = 0; i < pl.numVertices(); i++) {
      S2LatLng ll = new S2LatLng(pl.vertex(i));
      if (i > 0) sb.append(", ");
      sb.append(String.format("%.0f %.0f", ll.lng().degrees(), ll.lat().degrees()));
    }
    sb.append(")");
    return sb.toString();
  }
  /** Converts an S2Polygon (single-loop) into a 0-decimal-place WKT POLYGON string. */
  public static String toPolygonWkt(S2Polygon polygon) {
    // Assumes a single outer loop
    S2Loop loop = polygon.loop(0);
    StringBuilder sb = new StringBuilder("POLYGON ((");
    int n = loop.numVertices();
    for (int i = 0; i < n; i++) {
      S2LatLng ll = new S2LatLng(loop.vertex(i));
      if (i > 0) sb.append(", ");
      sb.append(String.format("%.0f %.0f", ll.lng().degrees(), ll.lat().degrees()));
    }
    // close the ring by repeating the first vertex
    S2LatLng first = new S2LatLng(loop.vertex(0));
    sb.append(", ")
        .append(String.format("%.0f %.0f", first.lng().degrees(), first.lat().degrees()));
    sb.append("))");
    return sb.toString();
  }
}
