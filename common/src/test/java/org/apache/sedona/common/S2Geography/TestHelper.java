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

import static org.apache.sedona.common.S2Geography.Accessors.S2_isEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.esotericsoftware.kryo.io.UnsafeInput;
import com.google.common.geometry.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.locationtech.jts.io.ParseException;

public class TestHelper {

  private static final double EPS = 1e-6;

  public static void assertRoundTrip(Geography original, EncodeOptions opts) throws IOException {
    int srid = original.getSRID();
    assertTrue("SRID must be non-negative", srid >= 0);
    if (srid == 0) {
      // If SRID is not set, we set it to a default value for testing purposes
      original.setSRID(4326);
    }
    // 1) Encode to bytes
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    original.encodeTagged(baos, opts);
    byte[] data = baos.toByteArray();

    // 2) Decode back
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    Geography decoded = Geography.decodeTagged(in);

    assertEquals(original.getSRID(), decoded.getSRID()); // Ensure SRID matches
    original.setSRID(srid); // Restore original SRID

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
  public static void assertCovering(Geography original, EncodeOptions opts) throws IOException {
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

  public static void checkWKBGeography(String wkbHex, String expectedWKT) throws ParseException {
    WKBReader wkbReader = new WKBReader();
    byte[] wkb = WKBReader.hexToBytes(wkbHex);
    Geography geoWKB = wkbReader.read(wkb);

    WKTReader wktReader = new WKTReader();
    Geography geoWKT = wktReader.read(expectedWKT);

    boolean isEqual = compareTo(geoWKT, geoWKT) == 0;
    if (!isEqual) {
      System.out.println(geoWKB);
      System.out.println(geoWKT);
    }
    assertTrue(isEqual);
  }

  public static int compareTo(Geography geo1, Geography geo2) {
    int compare = geo1.kind.getKind() - geo2.kind.getKind();
    if (compare != 0) {
      return compare;
    }

    compare = Integer.compare(geo1.numShapes(), geo2.numShapes());
    if (compare != 0) return compare;

    if (geo1 instanceof SinglePointGeography && geo2 instanceof SinglePointGeography) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      assertPointEquals(
          ((SinglePointGeography) geo1).getPoints().get(0),
          ((SinglePointGeography) geo2).getPoints().get(0));
    } else if (geo1 instanceof PointGeography && geo2 instanceof PointGeography) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      checkPointShape((PointGeography) geo1, (PointGeography) geo2);
    } else if (geo1 instanceof SinglePolylineGeography && geo2 instanceof SinglePolylineGeography) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      checkPolylineShape((PolylineGeography) geo1, (PolylineGeography) geo2);
    } else if (geo1 instanceof PolylineGeography && geo2 instanceof PolylineGeography) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      checkPolylineShape((PolylineGeography) geo1, (PolylineGeography) geo2);
    } else if (geo1 instanceof PolygonGeography && geo2 instanceof PolygonGeography) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      checkPolygonShape((PolygonGeography) geo1, (PolygonGeography) geo2);
    } else if (geo1 instanceof MultiPolygonGeography && geo2 instanceof MultiPolygonGeography) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      assertEquals(geo1.numShapes(), geo2.numShapes());
      for (int i = 0; i < geo1.numShapes(); i++) {
        PolygonGeography poly1 = (PolygonGeography) ((MultiPolygonGeography) geo1).features.get(i);
        PolygonGeography poly2 = (PolygonGeography) ((MultiPolygonGeography) geo1).features.get(i);
        checkPolygonShape(poly1, poly2);
      }
    } else if (geo1 instanceof GeographyCollection && geo2 instanceof GeographyCollection) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      assertEquals(geo1.numShapes(), geo2.numShapes());
      for (int i = 0; i < geo1.numShapes(); i++) {
        Geography g1 = (Geography) ((GeographyCollection) geo1).features.get(i);
        Geography g2 = (Geography) ((GeographyCollection) geo2).features.get(i);
        compareTo(g1, g2);
      }
    } else if (geo1 instanceof MultiPolygonGeography && geo2 instanceof MultiPolygonGeography) {
      if (S2_isEmpty(geo1) && S2_isEmpty(geo2)) return 0;
      assertEquals(geo1.numShapes(), geo2.numShapes());
      for (int i = 0; i < geo1.numShapes(); i++) {
        Geography g1 = (Geography) ((MultiPolygonGeography) geo1).features.get(i);
        Geography g2 = (Geography) ((MultiPolygonGeography) geo2).features.get(i);
        compareTo(g1, g2);
      }
    }
    return 0;
  }

  private static void checkPointShape(PointGeography point1, PointGeography point2) {
    List<S2Point> pOrig = point1.getPoints();
    List<S2Point> pDec = point2.getPoints();
    assertEquals("Point count", pOrig.size(), pDec.size());
    for (int i = 0; i < pOrig.size(); i++) {
      assertPointEquals(pOrig.get(i), pDec.get(i));
    }
  }

  private static void checkPolylineShape(PolylineGeography line1, PolylineGeography line2) {
    assertEquals("Line count", line1.getPolylines().size(), line2.getPolylines().size());
    List<S2Polyline> lineOrig = line1.getPolylines();
    List<S2Polyline> lineDec = line2.getPolylines();
    for (int i = 0; i < lineOrig.size(); i++) {
      List<S2Point> point1 = lineOrig.get(i).vertices();
      List<S2Point> point2 = lineDec.get(i).vertices();
      assertEquals("Vertex count", point1.size(), point2.size());
      for (int v = 0; v < point1.size(); v++) {
        assertPointEquals(point1.get(v), point2.get(v));
      }
    }
  }

  private static void checkPolygonShape(PolygonGeography poly1, PolygonGeography poly2) {
    // may have multiple rings
    assertEquals("Ring count", poly1.polygon.numLoops(), poly2.polygon.numLoops());

    for (int li = 0; li < poly1.polygon.numLoops(); li++) {
      S2Loop loop1 = poly1.polygon.loop(li);
      S2Loop loop2 = poly2.polygon.loop(li);

      assertEquals(loop1.numVertices(), loop2.numVertices());
      // number of vertices in this loop
      assertEquals(
          String.format("Vertex count in loop[%d]", li), loop1.numVertices(), loop2.numVertices());

      // compare each vertex
      for (int vi = 0; vi < loop1.numVertices(); vi++) {
        S2Point v1 = loop1.vertex(vi);
        S2Point v2 = loop2.vertex(vi);
        assertPointEquals(v1, v2);
      }
    }
  }

  private static void assertPointEquals(S2Point p1, S2Point p2) {
    S2LatLng ll1 = S2LatLng.fromPoint(p1);
    S2LatLng ll2 = S2LatLng.fromPoint(p2);
    assertEquals("Latitude", ll1.latDegrees(), ll2.latDegrees(), EPS);
    assertEquals("Longitude", ll1.lngDegrees(), ll2.lngDegrees(), EPS);
  }
}
