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
package org.apache.sedona.common.geography;

import com.google.common.geometry.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.GeographyCollection;
import org.apache.sedona.common.S2Geography.PolygonGeography;

public class Functions {

  public static Geography getEnvelope(Geography geography) {
    if (geography == null) return null;
    S2LatLngRect rect = geography.region().getRectBound();
    double lngLo = rect.lngLo().degrees();
    double latLo = rect.latLo().degrees();
    double lngHi = rect.lngHi().degrees();
    double latHi = rect.latHi().degrees();

    Geography envelope;
    if (!rect.lng().isInverted()) {
      // No antimeridian crossing → ONE polygon
      envelope = new PolygonGeography(rectToPolygon(lngLo, latLo, lngHi, latHi));
    } else {
      // Crossing → split into two polygons
      S2Polygon left = rectToPolygon(lngLo, latLo, 180.0, latHi);
      S2Polygon right = rectToPolygon(-180.0, latLo, lngHi, latHi);
      envelope =
          new GeographyCollection(Geography.GeographyKind.MULTIPOLYGON, List.of(left, right));
    }

    // Preserve SRID if you track it
    envelope.setSRID(geography.getSRID());
    return envelope;
  }

  /**
   * Build an S2Polygon rectangle (lng/lat in degrees), CCW ring: (lo,lo) → (hi,lo) → (hi,hi) →
   * (lo,hi).
   */
  private static S2Polygon rectToPolygon(double lngLo, double latLo, double lngHi, double latHi) {
    ArrayList<S2Point> v = new ArrayList<>(4);
    v.add(S2LatLng.fromDegrees(latLo, lngLo).toPoint());
    v.add(S2LatLng.fromDegrees(latLo, lngHi).toPoint());
    v.add(S2LatLng.fromDegrees(latHi, lngHi).toPoint());
    v.add(S2LatLng.fromDegrees(latHi, lngLo).toPoint());

    S2Loop loop = new S2Loop(v);
    // Optional: normalize for canonical orientation (keeps the smaller-area side)
    loop.normalize();

    return new S2Polygon(loop);
  }
}
