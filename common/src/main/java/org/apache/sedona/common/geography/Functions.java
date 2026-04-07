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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.sphere.Spheroid;
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
      // Crossing → split into two polygons
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

  /** Geodesic distance between two geographies in meters (WGS84 spheroid). */
  public static Double distance(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return null;
    return Spheroid.distance(toJTS(g1), toJTS(g2));
  }

  /** Geodesic area of a geography in square meters (WGS84 spheroid). */
  public static double area(Geography g) {
    if (g == null) return 0.0;
    return Spheroid.area(toJTS(g));
  }

  /** Geodesic length of a geography in meters (WGS84 spheroid). */
  public static double length(Geography g) {
    if (g == null) return 0.0;
    return Spheroid.length(toJTS(g));
  }

  // ─── Level 1: JTS-only structural operations ─────────────────────────────

  /** Return WKT text representation of a geography. */
  public static String asText(Geography g) {
    if (g == null) return null;
    return toJTS(g).toText();
  }

  /** Return the number of points in a geography. */
  public static int nPoints(Geography g) {
    if (g == null) return 0;
    return toJTS(g).getNumPoints();
  }

  /** Return the geometry type string (e.g., "ST_Point", "ST_Polygon"). */
  public static String geometryType(Geography g) {
    if (g == null) return null;
    return "ST_" + toJTS(g).getGeometryType();
  }

  /** Return the number of sub-geometries in a geography. */
  public static int numGeometries(Geography g) {
    if (g == null) return 0;
    return toJTS(g).getNumGeometries();
  }

  /** Return the centroid of a geography as a Geography point (planar centroid). */
  public static Geography centroid(Geography g) {
    if (g == null) return null;
    org.locationtech.jts.geom.Geometry centroid = toJTS(g).getCentroid();
    centroid.setSRID(g.getSRID());
    return WKBGeography.fromJTS(centroid);
  }

  // ─── Level 2+: Metric and S2 operations ─────────────────────────────────

  /** Maximum geodesic distance between two geographies in meters. Uses S2 furthest edge query. */
  public static Double maxDistance(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return null;
    Distance dist = new Distance();
    double radians = dist.S2_maxDistance(toShapeIndex(g1), toShapeIndex(g2));
    return radiansToMeters(radians);
  }

  /** Closest point on g1 to g2, returned as a Geography point. Uses S2 closest edge query. */
  public static Geography closestPoint(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return null;
    try {
      Pair<S2Point, S2Point> pair = findClosestPair(g1, g2);
      Geography result = new SinglePointGeography(pair.getLeft());
      result.setSRID(g1.getSRID());
      return WKBGeography.fromS2Geography(result);
    } catch (Exception e) {
      throw new RuntimeException("ST_ClosestPoint failed for geography: " + e.getMessage(), e);
    }
  }

  /**
   * Minimum clearance line between two geographies — the shortest line connecting them. Returns a
   * Geography LineString. Uses S2 closest edge query.
   */
  public static Geography minimumClearanceLine(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return null;
    try {
      Pair<S2Point, S2Point> pair = findClosestPair(g1, g2);
      S2Polyline line = new S2Polyline(List.of(pair.getLeft(), pair.getRight()));
      Geography result = new SinglePolylineGeography(line);
      result.setSRID(g1.getSRID());
      return WKBGeography.fromS2Geography(result);
    } catch (Exception e) {
      throw new RuntimeException(
          "ST_MinimumClearanceLine failed for geography: " + e.getMessage(), e);
    }
  }

  /**
   * Find the closest pair of points between two geographies. Uses S2ClosestEdgeQuery with interiors
   * included to handle point geometries correctly.
   */
  private static Pair<S2Point, S2Point> findClosestPair(Geography g1, Geography g2)
      throws Exception {
    Geography s2g1 = toS2(g1);
    Geography s2g2 = toS2(g2);

    // For point geometries, return the points directly
    boolean g1IsPoint = s2g1 instanceof SinglePointGeography || s2g1 instanceof PointGeography;
    boolean g2IsPoint = s2g2 instanceof SinglePointGeography || s2g2 instanceof PointGeography;

    if (g1IsPoint && g2IsPoint) {
      S2Point p1 = getFirstPoint(s2g1);
      S2Point p2 = getFirstPoint(s2g2);
      return Pair.of(p1, p2);
    }

    if (g1IsPoint) {
      // Closest point on g1 (a point) to g2 is always the point itself
      S2Point p1 = getFirstPoint(s2g1);
      // Find the closest point on g2 to p1 using edge query with interiors
      S2Point p2 = findClosestPointOnTarget(toShapeIndex(g2), p1);
      return Pair.of(p1, p2);
    }

    if (g2IsPoint) {
      S2Point p2 = getFirstPoint(s2g2);
      S2Point p1 = findClosestPointOnTarget(toShapeIndex(g1), p2);
      return Pair.of(p1, p2);
    }

    // Both have edges — use the full edge-pair closest points algorithm
    Distance dist = new Distance();
    return dist.S2_minimumClearanceLineBetween(toShapeIndex(g1), toShapeIndex(g2));
  }

  private static S2Point getFirstPoint(Geography g) {
    if (g instanceof SinglePointGeography) {
      return ((SinglePointGeography) g).getPoints().get(0);
    } else if (g instanceof PointGeography) {
      return ((PointGeography) g).getPoints().get(0);
    }
    throw new IllegalArgumentException("Expected point geography, got: " + g.getClass());
  }

  private static S2Point findClosestPointOnTarget(ShapeIndexGeography target, S2Point queryPoint) {
    S2ClosestEdgeQuery query = S2ClosestEdgeQuery.builder().build(target.shapeIndex);
    S2ClosestEdgeQuery.PointTarget pointTarget = new S2ClosestEdgeQuery.PointTarget(queryPoint);
    java.util.Optional<S2BestEdgesQueryBase.Result> result = query.findClosestEdge(pointTarget);
    if (!result.isPresent()) {
      return queryPoint; // fallback: no edges found
    }
    // The result contains the closest point on the target geometry
    S2BestEdgesQueryBase.Result edge = result.get();
    int shapeId = edge.shapeId();
    int edgeId = edge.edgeId();
    if (edgeId == -1) {
      // Interior match (point inside polygon) — return the query point itself
      return queryPoint;
    }
    S2Shape.MutableEdge mutableEdge = new S2Shape.MutableEdge();
    target.shapeIndex.getShapes().get(shapeId).getEdge(edgeId, mutableEdge);
    // Project the query point onto the edge to find the closest point
    return S2EdgeUtil.getClosestPoint(queryPoint, mutableEdge.getStart(), mutableEdge.getEnd());
  }

  /** Spherical equality test using S2 boolean operations. */
  public static boolean equals(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return false;
    Predicates pred = new Predicates();
    return pred.S2_equals(toShapeIndex(g1), toShapeIndex(g2), s2Options());
  }

  /** Spherical intersection test using S2 boolean operations. */
  public static boolean intersects(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return false;
    Predicates pred = new Predicates();
    return pred.S2_intersects(toShapeIndex(g1), toShapeIndex(g2), s2Options());
  }

  /** Spherical containment test using S2 boolean operations. */
  public static boolean contains(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return false;
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

  private static Geography toS2(Geography g) {
    if (g instanceof WKBGeography) return ((WKBGeography) g).getS2Geography();
    return g;
  }

  private static ShapeIndexGeography toShapeIndex(Geography g) {
    if (g instanceof WKBGeography) {
      return new ShapeIndexGeography(((WKBGeography) g).getS2Geography());
    }
    return new ShapeIndexGeography(g);
  }

  private static S2BooleanOperation.Options s2Options() {
    return new S2BooleanOperation.Options();
  }

  /** Mean Earth radius in meters (WGS84 mean radius). */
  private static final double EARTH_RADIUS_METERS = 6371008.8;

  private static double radiansToMeters(double radians) {
    return radians * EARTH_RADIUS_METERS;
  }
}
