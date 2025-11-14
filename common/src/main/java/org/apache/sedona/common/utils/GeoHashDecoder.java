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

import com.google.common.geometry.*;
import java.io.IOException;
import java.util.List;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.PolygonGeography;
import org.locationtech.jts.geom.Geometry;

public class GeoHashDecoder {
  private static final int[] bits = new int[] {16, 8, 4, 2, 1};
  private static final String base32 = "0123456789bcdefghjkmnpqrstuvwxyz";

  public static class InvalidGeoHashException extends Exception {
    public InvalidGeoHashException(String message) {
      super(message);
    }
  }

  public static Geometry decode(String geohash, Integer precision) throws InvalidGeoHashException {
    return decodeGeoHashBBox(geohash, precision).getBbox().toPolygon();
  }

  public static Geography decodeGeog(String geohash, Integer precision)
      throws InvalidGeoHashException, IOException {
    BBox box = decodeGeoHashBBox(geohash, precision).getBbox(); // west,east,south,north
    double south = box.startLat, north = box.endLat;
    double west = box.startLon, east = box.endLon;

    // 4 corners (lat, lon) in CCW order: SW → SE → NE → NW
    S2Point SW = S2LatLng.fromDegrees(south, west).toPoint();
    S2Point SE = S2LatLng.fromDegrees(south, east).toPoint();
    S2Point NE = S2LatLng.fromDegrees(north, east).toPoint();
    S2Point NW = S2LatLng.fromDegrees(north, west).toPoint();

    S2Loop shell = new S2Loop(List.of(SW, SE, NE, NW));
    shell.normalize(); // ensure CCW & smaller-area orientation (safe even if already CCW)

    S2Polygon s2poly = new S2Polygon(shell);
    Geography geog = new PolygonGeography(s2poly);
    geog.setSRID(4326);
    return geog;
  }

  private static class LatLon {
    public Double[] lons;

    public Double[] lats;

    public LatLon(Double[] lons, Double[] lats) {
      this.lons = lons;
      this.lats = lats;
    }

    BBox getBbox() {
      return new BBox(lons[0], lons[1], lats[0], lats[1]);
    }
  }

  private static LatLon decodeGeoHashBBox(String geohash, Integer precision)
      throws InvalidGeoHashException {
    LatLon latLon = new LatLon(new Double[] {-180.0, 180.0}, new Double[] {-90.0, 90.0});
    String geoHashLowered = geohash.toLowerCase();
    int geoHashLength = geohash.length();
    int targetPrecision = geoHashLength;
    if (precision != null) {
      if (precision < 0) throw new InvalidGeoHashException("Precision can not be negative");
      else targetPrecision = Math.min(geoHashLength, precision);
    }
    boolean isEven = true;

    for (int i = 0; i < targetPrecision; i++) {
      char c = geoHashLowered.charAt(i);
      byte cd = (byte) base32.indexOf(c);
      if (cd == -1) {
        throw new InvalidGeoHashException(
            String.format("Invalid character '%s' found at index %d", c, i));
      }
      for (int j = 0; j < 5; j++) {
        byte mask = (byte) bits[j];
        int index = (mask & cd) == 0 ? 1 : 0;
        if (isEven) {
          latLon.lons[index] = (latLon.lons[0] + latLon.lons[1]) / 2;
        } else {
          latLon.lats[index] = (latLon.lats[0] + latLon.lats[1]) / 2;
        }
        isEven = !isEven;
      }
    }
    return latLon;
  }
}
