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

import com.google.common.geometry.R2Vector;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import org.junit.Test;

public class ProjectionTest {
  private static final double TOL = 1e-6;

  @Test
  public void testMercatorProjection() {
    // Arrange
    var merc = Projection.pseudoMercator();

    // Act & Assert: Equator (lat=0, lon=0) → (0,0)
    R2Vector equator = merc.project(S2LatLng.fromDegrees(0, 0).toPoint());
    assertEquals(0.0, equator.x(), TOL);
    assertEquals(0.0, equator.y(), TOL);

    // Act & Assert: 45°N should map to positive Y
    R2Vector north45 = merc.project(S2LatLng.fromDegrees(45, 0).toPoint());
    assertTrue("Expected positive y for lat=45°", north45.y() > 0);
  }

  @Test
  public void testOrthographicProjection() {
    // Arrange
    S2LatLng centre = S2LatLng.fromDegrees(10, 20);
    var ortho = new Projection.OrthographicProjection(centre);

    // Act & Assert: Centre → (0,0)
    R2Vector origin = ortho.project(centre.toPoint());
    assertEquals(0.0, origin.x(), TOL);
    assertEquals(0.0, origin.y(), TOL);

    // Act: project another point then unproject back
    S2LatLng sample = S2LatLng.fromDegrees(45, 20);
    R2Vector projected = ortho.project(sample.toPoint());
    S2Point unprojected3d = ortho.unProject(projected);
    S2LatLng roundTripped = S2LatLng.fromPoint(unprojected3d);

    // Assert: round-trip matches original
    assertEquals(sample.lat().degrees(), roundTripped.lat().degrees(), TOL);
    assertEquals(sample.lng().degrees(), roundTripped.lng().degrees(), TOL);
  }
}
