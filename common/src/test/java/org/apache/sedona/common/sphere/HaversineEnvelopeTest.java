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
package org.apache.sedona.common.sphere;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class HaversineEnvelopeTest {
  private static final int SPHERE_RADIUS = 6357000;
  private static final GeometryFactory factory = new GeometryFactory();

  @Test
  public void testExpandPoint() {
    // The null island
    Envelope env = new Envelope(0, 0, 0, 0);
    Envelope expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);

    // North semi-sphere
    env = new Envelope(60, 60, 40, 40);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertFalse(coverEntireLongitudeRange(expandedEnv));

    env = new Envelope(-60, -60, 70, 70);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertFalse(coverEntireLongitudeRange(expandedEnv));

    // South semi-sphere
    env = new Envelope(60, 60, -40, -40);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertFalse(coverEntireLongitudeRange(expandedEnv));

    env = new Envelope(-60, -60, -70, -70);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertFalse(coverEntireLongitudeRange(expandedEnv));
  }

  @Test
  public void testExpandPointCrossingAntiMeridian() {
    // On the anti-meridian
    Envelope env = new Envelope(-180, -180, 50, 50);
    Envelope expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));

    env = new Envelope(180, 180, -60, -60);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));

    // Expand to cross the anti-meridian
    env = new Envelope(-178, -178, -50, -50);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));

    env = new Envelope(178, 178, 60, 60);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));
  }

  @Test
  public void testExpandPointCoveringThePoles() {
    // Point on the pole
    Envelope env = new Envelope(0, 0, 90, 90);
    Envelope expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheNorthPole(expandedEnv));

    env = new Envelope(-120, -120, -90, -90);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheSouthPole(expandedEnv));

    // Expand to cover the North Pole
    env = new Envelope(-120, -120, 89, 89);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheNorthPole(expandedEnv));

    // Expand to cover the South Pole
    env = new Envelope(160, 160, -89, -89);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheSouthPole(expandedEnv));
  }

  @Test
  public void testExpandPointCoveringTheGlobe() {
    Envelope env = new Envelope(10, 10, 20, 20);
    Envelope expandedEnv =
        Haversine.expandEnvelope(env, 2 * SPHERE_RADIUS * Math.PI, SPHERE_RADIUS);
    Assert.assertTrue(coverTheNorthPole(expandedEnv));
    Assert.assertTrue(coverTheSouthPole(expandedEnv));
  }

  @Test
  public void testExpandEnvelope() {
    Envelope env = new Envelope(-10, 10, -20, 20);
    Envelope expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);

    env = new Envelope(10, 20, 30, 40);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);

    env = new Envelope(-20, 10, 60, 70);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);

    env = new Envelope(10, 20, -40, -30);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);

    env = new Envelope(10, 20, -40, -30);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);

    env = new Envelope(-20, 10, -70, -60);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
  }

  @Test
  public void testExpandEnvelopeCrossingAntiMeridian() {
    Envelope env = new Envelope(-200, 160, -20, 20);
    Envelope expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));

    env = new Envelope(160, 200, -20, 20);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));

    env = new Envelope(-179, -160, -40, 60);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));

    env = new Envelope(160, 179, -40, 60);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverEntireLongitudeRange(expandedEnv));
  }

  @Test
  public void testExpandEnvelopeCoveringPoles() {
    Envelope env = new Envelope(40, 50, 89, 90);
    Envelope expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheNorthPole(expandedEnv));

    env = new Envelope(40, 50, -90, -89);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheSouthPole(expandedEnv));

    env = new Envelope(40, 50, 60, 89);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheNorthPole(expandedEnv));

    env = new Envelope(40, 50, -89, -80);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheSouthPole(expandedEnv));
  }

  @Test
  public void testExpandEnvelopeCoverTheGlobe() {
    Envelope env = new Envelope(-178, 178, -89, 89);
    Envelope expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheNorthPole(expandedEnv));
    Assert.assertTrue(coverTheSouthPole(expandedEnv));

    env = new Envelope(10, 20, -89, 89);
    expandedEnv = Haversine.expandEnvelope(env, 500_000, SPHERE_RADIUS);
    validateExpandedEnv(env, expandedEnv, 500_000);
    Assert.assertTrue(coverTheNorthPole(expandedEnv));
    Assert.assertTrue(coverTheSouthPole(expandedEnv));
  }

  private void validateExpandedEnv(Envelope env, Envelope expandedEnv, double distance) {
    if (env.getMinX() >= -180
        && env.getMaxX() <= 180
        && env.getMinY() >= -90
        && env.getMinY() <= 90) {
      Assert.assertTrue(expandedEnv.contains(env));
    }

    // Validate that points on the edge of the expanded envelope are further than the distance
    if (!coverEntireLongitudeRange(expandedEnv)) {
      // Upper left corner of original envelope
      Coordinate original = new Coordinate(env.getMinX(), env.getMaxY());
      Coordinate expanded = new Coordinate(expandedEnv.getMinX(), env.getMaxY());
      validateDistance(original, expanded, distance);

      // Left middle of original envelope
      original = new Coordinate(env.getMinX(), env.centre().y);
      expanded = new Coordinate(expandedEnv.getMinX(), env.centre().y);
      validateDistance(original, expanded, distance);

      // Lower left corner of original envelope
      original = new Coordinate(env.getMinX(), env.getMinY());
      expanded = new Coordinate(expandedEnv.getMinX(), env.getMinY());
      validateDistance(original, expanded, distance);

      // Upper right of the original envelope
      original = new Coordinate(env.getMaxX(), env.getMaxY());
      expanded = new Coordinate(expandedEnv.getMaxX(), env.getMaxY());
      validateDistance(original, expanded, distance);

      // Right middle of the original envelope
      original = new Coordinate(env.getMaxX(), env.centre().y);
      expanded = new Coordinate(expandedEnv.getMaxX(), env.centre().y);
      validateDistance(original, expanded, distance);

      // Lower right of the original envelope
      original = new Coordinate(env.getMaxX(), env.getMinY());
      expanded = new Coordinate(expandedEnv.getMaxX(), env.getMinY());
      validateDistance(original, expanded, distance);
    }

    if (!coverTheNorthPole(expandedEnv)) {
      // Validate the center of top edge
      Coordinate original = new Coordinate(env.centre().x, env.getMaxY());
      Coordinate expanded = new Coordinate(env.centre().x, expandedEnv.getMaxY());
      validateDistance(original, expanded, distance);
    }

    if (!coverTheSouthPole(expandedEnv)) {
      // Validate the center of bottom edge
      Coordinate original = new Coordinate(env.centre().x, env.getMinY());
      Coordinate expanded = new Coordinate(env.centre().x, expandedEnv.getMinY());
      validateDistance(original, expanded, distance);
    }
  }

  private boolean coverEntireLongitudeRange(Envelope env) {
    return env.getMinX() <= -180 && env.getMaxX() >= 180;
  }

  private boolean coverTheNorthPole(Envelope env) {
    return coverEntireLongitudeRange(env) && env.getMaxY() >= 90;
  }

  private boolean coverTheSouthPole(Envelope env) {
    return coverEntireLongitudeRange(env) && env.getMinY() <= -90;
  }

  private void validateDistance(Coordinate original, Coordinate expanded, double distance) {
    Point ptOriginal = factory.createPoint(original);
    Point ptExpanded = factory.createPoint(expanded);
    double actualSphereDistance = Haversine.distance(ptOriginal, ptExpanded);
    Assert.assertTrue(actualSphereDistance >= distance);
    double actualSpheroidDistance = Spheroid.distance(ptOriginal, ptExpanded);
    Assert.assertTrue(actualSpheroidDistance >= distance);
  }
}
