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
import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.sphere.Haversine;
import org.locationtech.jts.geom.Geometry;

public class Functions {

  private static final double EPSILON = 1e-9;

  private static boolean nearlyEqual(double a, double b) {
    if (Double.isNaN(a) || Double.isNaN(b)) {
      return false;
    }
    return Math.abs(a - b) < EPSILON;
  }

  public static Geography getEnvelope(Geography geography, boolean splitAtAntiMeridian) {
    if (geography == null) return null;
    S2LatLngRect rect = geography.region().getRectBound();
    double lngLo = rect.lngLo().degrees();
    double latLo = rect.latLo().degrees();
    double lngHi = rect.lngHi().degrees();
    double latHi = rect.latHi().degrees();

    if (nearlyEqual(latLo, latHi) && nearlyEqual(lngLo, lngHi)) {
      S2Point point = S2LatLng.fromDegrees(latLo, lngLo).toPoint();
      Geography pointGeo = new SinglePointGeography(point);
      pointGeo.setSRID(geography.getSRID());
      return pointGeo;
    }

    Geography envelope;
    if (splitAtAntiMeridian && rect.lng().isInverted()) {
      S2Polygon left = rectToPolygon(lngLo, latLo, 180.0, latHi);
      S2Polygon right = rectToPolygon(-180.0, latLo, lngHi, latHi);
      envelope =
          new MultiPolygonGeography(Geography.GeographyKind.MULTIPOLYGON, List.of(left, right));
    } else {
      envelope = new PolygonGeography(rectToPolygon(lngLo, latLo, lngHi, latHi));
    }
    envelope.setSRID(geography.getSRID());
    return envelope;
  }

  private static S2Polygon rectToPolygon(double lngLo, double latLo, double lngHi, double latHi) {
    ArrayList<S2Point> v = new ArrayList<>(4);
    v.add(S2LatLng.fromDegrees(latLo, lngLo).toPoint());
    v.add(S2LatLng.fromDegrees(latLo, lngHi).toPoint());
    v.add(S2LatLng.fromDegrees(latHi, lngHi).toPoint());
    v.add(S2LatLng.fromDegrees(latHi, lngLo).toPoint());

    S2Loop loop = new S2Loop(v);
    loop.normalize();

    return new S2Polygon(loop);
  }

  // ─── Level 1: JTS-only structural operations ─────────────────────────────

  /** Return the number of points in a geography. */
  public static int nPoints(Geography g) {
    if (g == null) return 0;
    return toJTS(g).getNumPoints();
  }

  /** Return the number of sub-geometries in a geography (1 for singles). */
  public static int numGeometries(Geography g) {
    if (g == null) return 0;
    return toJTS(g).getNumGeometries();
  }

  /** Return the geometry type string of a geography, prefixed with "ST_". */
  public static String geometryType(Geography g) {
    if (g == null) return null;
    return "ST_" + toJTS(g).getGeometryType();
  }

  /** Return the WKT text representation of a geography. */
  public static String asText(Geography g) {
    if (g == null) return null;
    return toJTS(g).toText();
  }

  // ─── Level 2: JTS + S2 geodesic metrics ──────────────────────────────────

  /**
   * Geometry-to-geometry geodesic distance in meters. Uses S2ClosestEdgeQuery for true minimum
   * distance between any two points on the geometries (not centroid-to-centroid). Consistent with
   * sedona-db's s2_distance implementation.
   */
  public static Double distance(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return null;
    if (g1 instanceof WKBGeography && g2 instanceof WKBGeography) {
      WKBGeography w1 = (WKBGeography) g1;
      WKBGeography w2 = (WKBGeography) g2;
      // Fast path: point-to-point distance without building ShapeIndex
      if (w1.isPoint() && w2.isPoint()) {
        S1Angle angle = new S1Angle(w1.extractPoint(), w2.extractPoint());
        return angle.radians() * Haversine.AVG_EARTH_RADIUS;
      }
      // Fast path: point-to-complex uses PointTarget (avoids building ShapeIndex for point side)
      if (w1.isPoint()) {
        double radians = Distance.S2_distancePointToIndex(w1.extractPoint(), toShapeIndex(w2));
        return radiansToMeters(radians);
      }
      if (w2.isPoint()) {
        double radians = Distance.S2_distancePointToIndex(w2.extractPoint(), toShapeIndex(w1));
        return radiansToMeters(radians);
      }
    }
    // General path via ShapeIndex
    Distance dist = new Distance();
    double radians = dist.S2_distance(toShapeIndex(g1), toShapeIndex(g2));
    return radiansToMeters(radians);
  }

  // ─── Level 3: S2 spherical predicates ────────────────────────────────────

  /** Spherical containment test using S2 boolean operations. */
  public static boolean contains(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return false;
    // A point (dimension 0) cannot contain anything
    if (g1.dimension() == 0) return false;

    Predicates pred = new Predicates();
    return pred.S2_contains(toShapeIndex(g1), toShapeIndex(g2), s2Options());
  }

  /** Return EWKT for geography object */
  public static String asEWKT(Geography geography) {
    return geography.toEWKT();
  }

  // ─── Helpers ───────────────────────────────────────────────────────────────

  private static Geometry toJTS(Geography g) {
    if (g instanceof WKBGeography) return ((WKBGeography) g).getJTSGeometry();
    return Constructors.geogToGeometry(g);
  }

  private static ShapeIndexGeography toShapeIndex(Geography g) {
    if (g instanceof WKBGeography) {
      return ((WKBGeography) g).getShapeIndexGeography();
    }
    return new ShapeIndexGeography(g);
  }

  private static S2BooleanOperation.Options s2Options() {
    return new S2BooleanOperation.Options();
  }

  private static double radiansToMeters(double radians) {
    return radians * Haversine.AVG_EARTH_RADIUS;
  }
}
