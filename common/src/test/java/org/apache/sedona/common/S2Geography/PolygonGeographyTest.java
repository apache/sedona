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

import com.google.common.geometry.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class PolygonGeographyTest {
  @Test
  public void testEncodedPolygon() throws IOException {
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point pt_mid = S2LatLng.fromDegrees(45, 0).toPoint();
    S2Point pt_end = S2LatLng.fromDegrees(0, 0).toPoint();
    // Build a single polygon and wrap in geography
    List<S2Point> points = new ArrayList<>();
    points.add(pt);
    points.add(pt_mid);
    points.add(pt_end);
    points.add(pt);
    S2Loop polyline = new S2Loop(points);
    S2Polygon poly = new S2Polygon(polyline);
    PolygonGeography geo = new PolygonGeography(poly);

    TestHelper.assertRoundTrip(geo, new EncodeOptions());
  }

  @Test
  public void testEncodeMultiPolygon() throws IOException {
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point pt_mid = S2LatLng.fromDegrees(45, 0).toPoint();
    S2Point pt_end = S2LatLng.fromDegrees(0, 0).toPoint();
    // Build a single polygon and wrap in geography
    List<S2Point> points = new ArrayList<>();
    points.add(pt);
    points.add(pt_mid);
    points.add(pt_end);
    points.add(pt);
    S2Loop polyline = new S2Loop(points);
    S2Polygon poly = new S2Polygon(polyline);

    S2Point pt2 = S2LatLng.fromDegrees(30, 10).toPoint();
    S2Point pt_mid2 = S2LatLng.fromDegrees(35, 20).toPoint();
    S2Point pt_end2 = S2LatLng.fromDegrees(32, 15).toPoint();
    // Build a single polygon and wrap in geography
    List<S2Point> points2 = new ArrayList<>();
    points2.add(pt2);
    points2.add(pt_mid2);
    points2.add(pt_end2);
    points2.add(pt2);
    S2Loop polyline2 = new S2Loop(points2);
    S2Polygon poly2 = new S2Polygon(polyline2);

    MultiPolygonGeography geo =
        new MultiPolygonGeography(Geography.GeographyKind.MULTIPOLYGON, List.of(poly2, poly));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    geo.encodeTagged(baos, new EncodeOptions());
    byte[] data = baos.toByteArray();

    ByteArrayInputStream in = new ByteArrayInputStream(data);
    MultiPolygonGeography decoded = (MultiPolygonGeography) Geography.decodeTagged(in);
    assertEquals(0, TestHelper.compareTo(geo, decoded));
  }
}
