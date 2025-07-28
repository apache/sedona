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
package org.apache.sedona.common;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.S2Geography.Distance;
import org.apache.sedona.common.S2Geography.PointGeography;
import org.apache.sedona.common.S2Geography.S2Geography;
import org.apache.sedona.common.S2Geography.ShapeIndexGeography;

public class S2Functions {

  public static double distance(S2Geography left, S2Geography right) {
    ShapeIndexGeography indexLeft = new ShapeIndexGeography(left);
    ShapeIndexGeography indexRight = new ShapeIndexGeography(right);
    return Distance.S2_distance((ShapeIndexGeography) indexLeft, (ShapeIndexGeography) indexRight);
  }

  public static double maxDistance(S2Geography left, S2Geography right) {
    ShapeIndexGeography indexLeft = new ShapeIndexGeography(left);
    ShapeIndexGeography indexRight = new ShapeIndexGeography(right);
    return Distance.S2_maxDistance(
        (ShapeIndexGeography) indexLeft, (ShapeIndexGeography) indexRight);
  }

  public static S2Geography cloestPoint(S2Geography left, S2Geography right) throws Exception {
    ShapeIndexGeography indexLeft = new ShapeIndexGeography(left);
    ShapeIndexGeography indexRight = new ShapeIndexGeography(right);
    S2Geography s2Geography =
        new PointGeography(
            Distance.S2_closestPoint(
                (ShapeIndexGeography) indexLeft, (ShapeIndexGeography) indexRight));
    return s2Geography;
  }

  public static S2Geography minimumClearanceLineBetween(S2Geography left, S2Geography right)
      throws Exception {
    ShapeIndexGeography indexLeft = new ShapeIndexGeography(left);
    ShapeIndexGeography indexRight = new ShapeIndexGeography(right);
    Pair<S2Point, S2Point> res =
        Distance.S2_minimumClearanceLineBetween(
            (ShapeIndexGeography) indexLeft, (ShapeIndexGeography) indexRight);
    S2Geography geography = new PointGeography(List.of(res.getLeft(), res.getRight()));
    return geography;
  }

  public static String asWKT(S2Geography geography) {
    return geography.toString();
  }

  public static String resultToText(S2Geography pointGeography) {
    List<S2Point> points = ((PointGeography) pointGeography).getPoints();

    S2LatLng a = new S2LatLng(points.get(0));
    S2LatLng b = new S2LatLng(points.get(1));

    double lng1 = a.lngDegrees();
    double lat1 = a.latDegrees();
    double lng2 = b.lngDegrees();
    double lat2 = b.latDegrees();

    return String.format("LINESTRING (%f %f, %f %f)", lng1, lat1, lng2, lat2);
  }
}
