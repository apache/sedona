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

import org.locationtech.jts.geom.Point;

public class PointGeoHashEncoder {
  private static String base32 = "0123456789bcdefghjkmnpqrstuvwxyz";
  private static int[] bits = new int[] {16, 8, 4, 2, 1};

  public static String calculateGeoHash(Point geom, long precision) {
    BBox bbox = new BBox(-180, 180, -90, 90);
    long precisionUpdated = Math.min(precision, 20);
    if (precision <= 0) {
      return "";
    }
    return geoHashAggregate(geom, precisionUpdated, 0, "", true, bbox, 0, 0);
  }

  private static String geoHashAggregate(
      Point point,
      long precision,
      long currentPrecision,
      String geoHash,
      boolean isEven,
      BBox bbox,
      int bit,
      int ch) {
    if (currentPrecision >= precision) {
      return geoHash;
    }

    BBox updatedBbox = null;
    int updatedCh = -1;
    if (isEven) {
      double mid = (bbox.startLon + bbox.endLon) / 2.0;
      if (point.getX() >= mid) {
        updatedBbox = new BBox(bbox);
        updatedBbox.startLon = mid;
        updatedCh = ch | bits[bit];
      } else {
        updatedBbox = new BBox(bbox);
        updatedBbox.endLon = mid;
        updatedCh = ch;
      }
    } else {
      double mid = (bbox.startLat + bbox.endLat) / 2.0;
      if (point.getY() >= mid) {
        updatedBbox = new BBox(bbox);
        updatedBbox.startLat = mid;
        updatedCh = ch | bits[bit];
      } else {
        updatedBbox = new BBox(bbox);
        updatedBbox.endLat = mid;
        updatedCh = ch;
      }
    }
    if (bit < 4) {
      return geoHashAggregate(
          point, precision, currentPrecision, geoHash, !isEven, updatedBbox, bit + 1, updatedCh);
    } else {
      String geoHashUpdated = geoHash + base32.charAt(updatedCh);
      return geoHashAggregate(
          point, precision, currentPrecision + 1, geoHashUpdated, !isEven, updatedBbox, 0, 0);
    }
  }
}
