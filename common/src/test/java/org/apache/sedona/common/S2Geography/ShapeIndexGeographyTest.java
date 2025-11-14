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

import static junit.framework.TestCase.assertEquals;

import com.google.common.geometry.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Test;

public class ShapeIndexGeographyTest {
  @Test
  public void testEncodedShapeIndexGeographyRoundTrip() throws IOException {
    // 1) Build three S2Point landmarks.
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point ptMid = S2LatLng.fromDegrees(45, 0).toPoint();
    S2Point ptEnd = S2LatLng.fromDegrees(0, 0).toPoint();

    PointGeography pointGeography = new PointGeography(pt);
    S2Polyline polyline = new S2Polyline(Arrays.asList(pt, ptEnd));
    PolylineGeography lineGeog = new PolylineGeography(polyline);

    S2Loop loop = new S2Loop(Arrays.asList(pt, ptMid, ptEnd));
    S2Polygon polygon = new S2Polygon(loop);
    PolygonGeography polygonGeog = new PolygonGeography(polygon);

    // 3) Index them in a ShapeIndexGeography
    ShapeIndexGeography geog = new ShapeIndexGeography();
    geog.addIndex(pointGeography);
    geog.addIndex(lineGeog);
    geog.addIndex(polygonGeog);

    // 4) Encode
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geog.encodeTagged(baos, new EncodeOptions());

    byte[] data = baos.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    Geography roundtrip = geog.decodeTagged(in);

    // 6) Verify the kind and count
    assertEquals(Geography.GeographyKind.ENCODED_SHAPE_INDEX, roundtrip.kind);
    assertEquals(3, roundtrip.numShapes());

    // 6) Cast back to shape-index geography
    EncodedShapeIndexGeography idxGeo = (EncodedShapeIndexGeography) roundtrip;

    S2Shape.MutableEdge mutableEdge = new S2Shape.MutableEdge();

    // ---- Point shape ----
    S2Shape ptShape = idxGeo.shape(0);
    assertEquals(1, ptShape.numEdges());
    // A single “edge” for a point: v0 should equal the original point
    ptShape.getEdge(0, mutableEdge);
    assertEquals(pt, mutableEdge.a);

    // ---- Line shape ----
    S2Shape lineShape = idxGeo.shape(1);
    assertEquals(1, lineShape.numEdges());
    lineShape.getEdge(0, mutableEdge);
    assertEquals(pt, mutableEdge.a);
    assertEquals(ptEnd, mutableEdge.b);

    // ---- Polygon shape ----
    S2Shape polyShape = idxGeo.shape(2);
    assertEquals(3, polyShape.numEdges());
    polyShape.getEdge(0, mutableEdge);
    assertEquals(pt, mutableEdge.a);
    assertEquals(ptMid, mutableEdge.b);

    polyShape.getEdge(1, mutableEdge);
    assertEquals(ptMid, mutableEdge.a);
    assertEquals(ptEnd, mutableEdge.b);

    polyShape.getEdge(2, mutableEdge);
    assertEquals(ptEnd, mutableEdge.a);
    assertEquals(pt, mutableEdge.b);
  }
}
