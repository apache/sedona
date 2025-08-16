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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.geometry.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class GeographyCollectionTest {
  @Test
  public void testEmptyCollection() {
    GeographyCollection empty = new GeographyCollection();
    assertEquals("Empty collection should have zero shapes", 0, empty.numShapes());
    assertEquals("Empty collection dimension", -1, empty.dimension());
    List<S2CellId> cover = new ArrayList<>();
    empty.getCellUnionBound(cover);
    assertTrue("Covering of empty should be empty", cover.isEmpty());
  }

  @Test
  public void testSingleFeature() {
    // Single point geography
    S2Point p = S2LatLng.fromDegrees(10, 20).toPoint();
    PointGeography pointGeo = new PointGeography(Arrays.asList(p));
    List<Geography> features = Arrays.<Geography>asList(pointGeo);
    GeographyCollection coll = new GeographyCollection(features);

    assertEquals("Collection numShapes", 1, coll.numShapes());
    assertEquals("Collection dimension for single point", 0, coll.dimension());
    // shape(0) should return first shape
    assertNotNull("shape(0) not null", coll.shape(0));
    // region should contain the point
    S2Region region = coll.region();
    assertTrue("Region should contain point", region.contains(p));
  }

  @Test
  public void testMultipleFeaturesAndMixedDimension() {
    // Two point features (dimension 0)
    S2Point p1 = S2LatLng.fromDegrees(0, 0).toPoint();
    S2Point p2 = S2LatLng.fromDegrees(1, 1).toPoint();
    PointGeography pg1 = new PointGeography(Arrays.asList(p1));
    PointGeography pg2 = new PointGeography(Arrays.asList(p2));

    // One polyline feature (dimension 1)
    List<S2Point> linePts = new ArrayList<>();
    S2Point ptStart = S2LatLng.fromDegrees(2, 2).toPoint();
    S2Point ptEnd = S2LatLng.fromDegrees(3, 3).toPoint();
    linePts.add(ptStart);
    linePts.add(ptEnd);
    S2Polyline polyline = new S2Polyline(linePts);
    PolylineGeography polyGeo = new PolylineGeography(polyline);

    List<Geography> features = Arrays.<Geography>asList(pg1, pg2, polyGeo);
    GeographyCollection coll = new GeographyCollection(features);

    assertEquals("Collection numShapes", 3, coll.numShapes());
    assertEquals("Mixed dimensions should return -1", -1, coll.dimension());
    // test proper dispatch of shape ids
    assertNotNull(coll.shape(2));
  }

  @Test // todo: issue with buffer underflow of encode decode geography kind differences
  public void testEncodeDecodeRoundTrip() throws IOException {
    // prepare collection with two point geos
    S2Point p1 = S2LatLng.fromDegrees(-10, -20).toPoint();
    S2Point p2 = S2LatLng.fromDegrees(30, 40).toPoint();
    PointGeography pg1 = new PointGeography(Arrays.asList(p1));
    PointGeography pg2 = new PointGeography(Arrays.asList(p2));
    GeographyCollection original = new GeographyCollection(List.<Geography>of(pg1, pg2));

    // encode/decode round trip via TestHelper
    EncodeOptions opts = new EncodeOptions();
    TestHelper.assertRoundTrip(original, opts);
  }

  @Test
  public void testGetCellUnionBoundNonEmpty() {
    // collection of two distinct points
    S2Point a = S2LatLng.fromDegrees(0, 0).toPoint();
    S2Point b = S2LatLng.fromDegrees(10, 10).toPoint();
    PointGeography pg1 = new PointGeography(List.of(a));
    PointGeography pg2 = new PointGeography(List.of(b));
    GeographyCollection coll = new GeographyCollection(Arrays.<Geography>asList(pg1, pg2));

    List<S2CellId> cover = new ArrayList<>();
    coll.getCellUnionBound(cover);
    assertTrue("Covering should not be empty for non-empty collection", cover.size() > 0);
    // ensure each cell covers at least one input point
    for (S2Point p : Arrays.asList(a, b)) {
      boolean covered =
          cover.stream().anyMatch(cid -> new com.google.common.geometry.S2Cell(cid).contains(p));
      assertTrue("Each point should be covered", covered);
    }
  }
}
