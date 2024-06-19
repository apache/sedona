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
package org.apache.sedona.common.utils;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class GeometryGeoHashEncoder {
  private static GeometryFactory geometryFactory = new GeometryFactory();

  public static String calculate(Geometry geom, int precision) {
    Envelope gbox = geom.getEnvelope().getEnvelopeInternal();
    // Latitude can take values in [-90, 90]
    // Longitude can take values in [-180, 180]
    if (gbox.getMinX() < -180
        || gbox.getMinY() < -90
        || gbox.getMaxX() > 180
        || gbox.getMaxY() > 90) {
      return null;
    }

    double lon = gbox.getMinX() + (gbox.getMaxX() - gbox.getMinX()) / 2;
    double lat = gbox.getMinY() + (gbox.getMaxY() - gbox.getMinY()) / 2;

    return PointGeoHashEncoder.calculateGeoHash(
        geometryFactory.createPoint(new Coordinate(lon, lat)), precision);
  }
}
