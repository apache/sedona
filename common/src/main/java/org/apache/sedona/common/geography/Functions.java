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

  /**
   * Returns the spherical centroid of a geography as a {@link Geography} point. The centroid is
   * computed on the sphere using S2 area- and length-weighting:
   *
   * <ul>
   *   <li>Polygon / MultiPolygon: area-weighted centroid via {@link S2Polygon#getCentroid()}.
   *   <li>LineString / MultiLineString: length-weighted centroid via {@link
   *       S2Polyline#getCentroid()}.
   *   <li>Point / MultiPoint: mean of the unit vectors.
   *   <li>GeographyCollection: weighted sum of the children's S2 centroids.
   * </ul>
   *
   * <p>Unlike a planar (lon/lat) centroid, this answer is correct for antimeridian-crossing and
   * polar geographies. As with JTS for non-convex shapes, the centroid may lie outside the input
   * geometry. Returns {@code null} if the input is {@code null} or if the centroid is undefined
   * (e.g. an empty geometry, or antipodal points whose unit vectors cancel).
   */
  public static Geography centroid(Geography g) {
    if (g == null) return null;
    Geography typed = (g instanceof WKBGeography) ? ((WKBGeography) g).getS2Geography() : g;
    S2Point weighted = sphericalCentroid(typed);
    if (weighted == null) return null;
    // S2 returns area- (or length-) weighted centroids; project back to the sphere.
    double norm = weighted.norm();
    if (!(norm > 0.0)) return null; // zero or NaN — centroid undefined
    S2Point unit = S2Point.normalize(weighted);
    SinglePointGeography point = new SinglePointGeography(unit);
    point.setSRID(g.getSRID());
    return WKBGeography.fromS2Geography(point);
  }

  /**
   * Returns the S2-weighted centroid of {@code g} (area-weighted for polygons, length-weighted for
   * polylines, sum of unit vectors for points). Result is {@code null} when no centroid
   * contribution exists (empty input, unsupported kind).
   */
  private static S2Point sphericalCentroid(Geography g) {
    if (g instanceof PointGeography) {
      List<S2Point> pts = ((PointGeography) g).getPoints();
      if (pts == null || pts.isEmpty()) return null;
      S2Point sum = pts.get(0);
      for (int i = 1; i < pts.size(); i++) {
        sum = S2Point.add(sum, pts.get(i));
      }
      return sum;
    }
    if (g instanceof PolylineGeography) {
      S2Point sum = null;
      for (S2Polyline p : ((PolylineGeography) g).getPolylines()) {
        sum = addOrInit(sum, p.getCentroid());
      }
      return sum;
    }
    if (g instanceof PolygonGeography) {
      return ((PolygonGeography) g).polygon.getCentroid();
    }
    if (g instanceof MultiPolygonGeography) {
      S2Point sum = null;
      for (Geography feature : ((MultiPolygonGeography) g).getFeatures()) {
        if (feature instanceof PolygonGeography) {
          sum = addOrInit(sum, ((PolygonGeography) feature).polygon.getCentroid());
        }
      }
      return sum;
    }
    if (g instanceof GeographyCollection) {
      S2Point sum = null;
      for (Geography feature : ((GeographyCollection) g).getFeatures()) {
        sum = addOrInit(sum, sphericalCentroid(feature));
      }
      return sum;
    }
    return null;
  }

  private static S2Point addOrInit(S2Point sum, S2Point next) {
    if (next == null) return sum;
    return (sum == null) ? next : S2Point.add(sum, next);
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
