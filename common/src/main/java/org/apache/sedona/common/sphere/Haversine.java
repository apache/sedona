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

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class Haversine {
  /**
   * Calculate the distance between two points on the earth using the "haversine" formula. This is
   * also known as the great-circle distance This will produce almost identical result to PostGIS
   * ST_DistanceSphere and ST_Distance(useSpheroid=false)
   *
   * @param geom1 The first geometry. Each coordinate is in lon/lat order
   * @param geom2 The second geometry. Each coordinate is in lon/lat order
   * @return
   */
  public static double distance(Geometry geom1, Geometry geom2, double AVG_EARTH_RADIUS) {
    Coordinate coordinate1 =
        geom1.getGeometryType().equals("Point")
            ? geom1.getCoordinate()
            : geom1.getCentroid().getCoordinate();
    Coordinate coordinate2 =
        geom2.getGeometryType().equals("Point")
            ? geom2.getCoordinate()
            : geom2.getCentroid().getCoordinate();
    // Calculate the distance between the two points
    double lon1 = coordinate1.getX();
    double lat1 = coordinate1.getY();
    double lon2 = coordinate2.getX();
    double lat2 = coordinate2.getY();
    double latDistance = toRadians(lat2 - lat1);
    double lngDistance = toRadians(lon2 - lon1);
    double a =
        sin(latDistance / 2) * sin(latDistance / 2)
            + cos(toRadians(lat1))
                * cos(toRadians(lat2))
                * sin(lngDistance / 2)
                * sin(lngDistance / 2);
    double c = 2 * atan2(sqrt(a), sqrt(1 - a));
    return AVG_EARTH_RADIUS * c * 1.0;
  }

  // Calculate the distance between two points on the earth using the "haversine" formula.
  // The radius of the earth is 6371.0 km
  public static double distance(Geometry geom1, Geometry geom2) {
    return distance(geom1, geom2, 6371008.0);
  }

  /**
   * Expand the given envelope on sphere by the given distance in meter.
   *
   * @param envelope the envelope to expand
   * @param distance in meter
   * @param sphereRadius radius of the sphere in meter
   * @return expanded envelope
   */
  public static Envelope expandEnvelope(Envelope envelope, double distance, double sphereRadius) {
    // 10% buffer to get rid of false negatives
    double scaleFactor = 1.1;
    double latDeltaRadian = distance / sphereRadius;
    double latDeltaDegree = Math.toDegrees(latDeltaRadian);
    double newMinY = envelope.getMinY() - latDeltaDegree * scaleFactor;
    double newMaxY = envelope.getMaxY() + latDeltaDegree * scaleFactor;
    if (newMinY <= -90 || newMaxY >= 90) {
      // The expanded envelope covers the pole, so its longitude range should be expanded to [-180,
      // 180]
      return new Envelope(-180, 180, Math.max(newMinY, -90), Math.min(newMaxY, 90));
    }

    double minLatRadian = Math.toRadians(newMinY);
    double maxLatRadian = Math.toRadians(newMaxY);
    double lonDeltaRadian =
        Math.max(
            Math.abs(distance / (sphereRadius * Math.cos(maxLatRadian))),
            Math.abs(distance / (sphereRadius * Math.cos(minLatRadian))));
    double lonDeltaDegree = Math.toDegrees(lonDeltaRadian);
    double newMinX = envelope.getMinX() - lonDeltaDegree * scaleFactor;
    double newMaxX = envelope.getMaxX() + lonDeltaDegree * scaleFactor;
    if (newMinX <= -180 || newMaxX >= 180) {
      // The expanded envelope crosses the anti-meridian. Expanding its longitude range to [-180,
      // 180] is
      // the best thing we can do, since we treat lon-lat as planar coordinates when running spatial
      // join.
      return new Envelope(-180, 180, newMinY, newMaxY);
    } else {
      return new Envelope(newMinX, newMaxX, newMinY, newMaxY);
    }
  }
}
