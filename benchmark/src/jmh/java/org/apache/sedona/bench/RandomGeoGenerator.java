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
package org.apache.sedona.bench;

import java.util.Random;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

/**
 * Generates random Geography and JTS Geometry objects for benchmarking. Produces scattered
 * geometries within a bounded region to simulate realistic spatial data.
 */
public class RandomGeoGenerator {

  private static final GeometryFactory GF = new GeometryFactory();

  /** Generate an array of random Geography points scattered within a bounding box. */
  public static Geography[] randomGeogPoints(int count, long seed) throws ParseException {
    Random rng = new Random(seed);
    Geography[] result = new Geography[count];
    for (int i = 0; i < count; i++) {
      double lon = -180 + rng.nextDouble() * 360;
      double lat = -60 + rng.nextDouble() * 120; // avoid poles
      result[i] = Constructors.geogFromWKT(String.format("POINT (%.8f %.8f)", lon, lat), 4326);
    }
    return result;
  }

  /** Generate an array of random JTS points scattered within a bounding box. */
  public static Geometry[] randomJtsPoints(int count, long seed) {
    Random rng = new Random(seed);
    Geometry[] result = new Geometry[count];
    for (int i = 0; i < count; i++) {
      double lon = -180 + rng.nextDouble() * 360;
      double lat = -60 + rng.nextDouble() * 120;
      result[i] = GF.createPoint(new Coordinate(lon, lat));
    }
    return result;
  }

  /**
   * Generate an array of random Geography polygons. Each polygon is a circular approximation with
   * the given number of vertices, centered at a random location.
   */
  public static Geography[] randomGeogPolygons(int count, int vertices, long seed)
      throws ParseException {
    Random rng = new Random(seed);
    Geography[] result = new Geography[count];
    for (int i = 0; i < count; i++) {
      double centerLon = -170 + rng.nextDouble() * 340;
      double centerLat = -50 + rng.nextDouble() * 100;
      double radius = 0.01 + rng.nextDouble() * 0.5; // 0.01 to 0.51 degrees
      String wkt = buildCirclePolygonWKT(vertices, radius, centerLon, centerLat);
      result[i] = Constructors.geogFromWKT(wkt, 4326);
    }
    return result;
  }

  /** Generate an array of random JTS polygons matching the Geography generation. */
  public static Geometry[] randomJtsPolygons(int count, int vertices, long seed)
      throws org.locationtech.jts.io.ParseException {
    Random rng = new Random(seed);
    org.locationtech.jts.io.WKTReader reader = new org.locationtech.jts.io.WKTReader();
    Geometry[] result = new Geometry[count];
    for (int i = 0; i < count; i++) {
      double centerLon = -170 + rng.nextDouble() * 340;
      double centerLat = -50 + rng.nextDouble() * 100;
      double radius = 0.01 + rng.nextDouble() * 0.5;
      result[i] = reader.read(buildCirclePolygonWKT(vertices, radius, centerLon, centerLat));
    }
    return result;
  }

  /** Generate an array of random Geography linestrings with the given number of vertices. */
  public static Geography[] randomGeogLineStrings(int count, int vertices, long seed)
      throws ParseException {
    Random rng = new Random(seed);
    Geography[] result = new Geography[count];
    for (int i = 0; i < count; i++) {
      double startLon = -170 + rng.nextDouble() * 340;
      double startLat = -50 + rng.nextDouble() * 100;
      String wkt = buildRandomLineWKT(vertices, startLon, startLat, rng);
      result[i] = Constructors.geogFromWKT(wkt, 4326);
    }
    return result;
  }

  private static String buildCirclePolygonWKT(
      int vertices, double radius, double centerLon, double centerLat) {
    StringBuilder sb = new StringBuilder("POLYGON ((");
    for (int i = 0; i <= vertices; i++) {
      if (i > 0) sb.append(", ");
      double angle = 2.0 * Math.PI * (i % vertices) / vertices;
      sb.append(
          String.format(
              "%.8f %.8f",
              centerLon + radius * Math.cos(angle), centerLat + radius * Math.sin(angle)));
    }
    sb.append("))");
    return sb.toString();
  }

  private static String buildRandomLineWKT(
      int vertices, double startLon, double startLat, Random rng) {
    StringBuilder sb = new StringBuilder("LINESTRING (");
    double lon = startLon, lat = startLat;
    for (int i = 0; i < vertices; i++) {
      if (i > 0) sb.append(", ");
      sb.append(String.format("%.8f %.8f", lon, lat));
      lon += (rng.nextDouble() - 0.5) * 0.2;
      lat += (rng.nextDouble() - 0.5) * 0.2;
    }
    sb.append(")");
    return sb.toString();
  }
}
