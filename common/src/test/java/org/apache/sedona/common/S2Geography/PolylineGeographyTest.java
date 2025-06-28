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

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polyline;
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
    // Build a single polyline and wrap in geography
    List<S2Point> points = new ArrayList<>();
    points.add(ptStart);
    points.add(ptEnd);
    S2Polyline polyline = new S2Polyline(points);
    PolylineGeography geog = new PolylineGeography(polyline);
    TestHelper.assertRoundTrip(geog, new EncodeOptions());
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
    TestHelper.assertRoundTrip(geog, new EncodeOptions());
  }

  @Test
  public void testEncodedMultiPolylineHint() throws IOException {
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
    EncodeOptions encodeOptions = new EncodeOptions();
    encodeOptions.setCodingHint(EncodeOptions.CodingHint.COMPACT);
    TestHelper.assertRoundTrip(geog, encodeOptions);
  }
}
