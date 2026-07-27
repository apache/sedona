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
package org.apache.sedona.core.spatialPartitioning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;

public class QuadTreeRTPartitioningTest {

  @Test
  public void testCircleIntersectionDoesNotUsePolygonalBufferApproximation() {
    double radius = 10.0;
    double angle = Math.PI / 32.0;
    Coordinate coordinate = new Coordinate(9.98 * Math.cos(angle), 9.98 * Math.sin(angle));
    Envelope candidate = new Envelope(coordinate);

    assertTrue(QuadTreeRTPartitioning.intersectsCircle(candidate, 0.0, 0.0, radius, true));
    assertFalse(QuadTreeRTPartitioning.intersectsCircle(candidate, 0.0, 0.0, radius, false));

    GeometryFactory factory = new GeometryFactory();
    assertFalse(
        factory
            .createPoint(new Coordinate(0.0, 0.0))
            .buffer(radius)
            .intersects(factory.createPoint(coordinate)));
  }
}
