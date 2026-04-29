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

  // ─── Level 2: Geodesic metrics ───────────────────────────────────────────

  /**
   * Spherical length in meters of a geography, calculated on the sphere. Edges are interpreted as
   * great-circle arcs; the summed arc-angle is scaled by {@link Haversine#AVG_EARTH_RADIUS}.
   * Multi-polylines sum the children's lengths; geography collections recurse. Returns {@code 0.0}
   * for point/polygon geographies and for {@code null}.
   */
  public static double length(Geography g) {
    if (g == null) return 0.0;
    Geography typed = (g instanceof WKBGeography) ? ((WKBGeography) g).getS2Geography() : g;
    double radians = sphericalLength(typed);
    return radians * Haversine.AVG_EARTH_RADIUS;
  }

  /** Arc-angle (radians) of {@code g} on the unit sphere; 0 for non-linear kinds. */
  private static double sphericalLength(Geography g) {
    if (g instanceof PolylineGeography) {
      double sum = 0.0;
      for (S2Polyline pl : ((PolylineGeography) g).getPolylines()) {
        sum += pl.getArclengthAngle().radians();
      }
      return sum;
    }
    if (g instanceof GeographyCollection) {
      double sum = 0.0;
      for (Geography feature : ((GeographyCollection) g).getFeatures()) {
        sum += sphericalLength(feature);
      }
      return sum;
    }
    return 0.0;
  }

  /**
   * Spherical area in square meters of a geography, calculated on the sphere. The Earth is modeled
   * as a sphere of radius {@link Haversine#AVG_EARTH_RADIUS}; the polygon's interior is integrated
   * along great-circle edges and scaled by R squared. Multi-polygons sum the children's areas;
   * geography collections recurse. Returns {@code 0.0} for point/line geographies and for {@code
   * null}.
   */
  public static double area(Geography g) {
    if (g == null) return 0.0;
    Geography typed = (g instanceof WKBGeography) ? ((WKBGeography) g).getS2Geography() : g;
    double steradians = sphericalArea(typed);
    // S2 polygons can be wound either CCW (interior is the small side) or CW (interior is the
    // complement on the sphere). Some WKT inputs land in the latter form after parsing, which
    // makes S2 report the entire sphere minus the visible polygon. Always return the smaller
    // of the two regions so the answer is bounded by half the surface of the sphere.
    if (steradians > 2.0 * Math.PI) {
      steradians = 4.0 * Math.PI - steradians;
    }
    return steradians * Haversine.AVG_EARTH_RADIUS * Haversine.AVG_EARTH_RADIUS;
  }

  /** Steradian area of {@code g} on the unit sphere; 0 for non-areal kinds. */
  private static double sphericalArea(Geography g) {
    if (g instanceof PolygonGeography) {
      return ((PolygonGeography) g).polygon.getArea();
    }
    if (g instanceof MultiPolygonGeography) {
      double sum = 0.0;
      for (Geography feature : ((MultiPolygonGeography) g).getFeatures()) {
        if (feature instanceof PolygonGeography) {
          sum += ((PolygonGeography) feature).polygon.getArea();
        }
      }
      return sum;
    }
    if (g instanceof GeographyCollection) {
      double sum = 0.0;
      for (Geography feature : ((GeographyCollection) g).getFeatures()) {
        sum += sphericalArea(feature);
      }
      return sum;
    }
    // Points and polylines have zero area
    return 0.0;
  }

  /**
   * Geometry-to-geometry geodesic distance in meters. Uses S2ClosestEdgeQuery for true minimum
   * distance between any two points on the geometries (not centroid-to-centroid).
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

  /** Spherical equality test using S2 boolean operations. */
  public static boolean equals(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return false;
    Predicates pred = new Predicates();
    return pred.S2_equals(toShapeIndex(g1), toShapeIndex(g2), s2Options());
  }

  /**
   * Spherical intersection test using S2 boolean operations. Takes fast paths for point-to-point
   * and point-to-complex inputs backed by WKBGeography, avoiding ShapeIndex construction on the
   * point side.
   */
  public static boolean intersects(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return false;
    if (g1 instanceof WKBGeography && g2 instanceof WKBGeography) {
      WKBGeography w1 = (WKBGeography) g1;
      WKBGeography w2 = (WKBGeography) g2;
      // Fast path: point-to-point intersects iff the points are equal
      if (w1.isPoint() && w2.isPoint()) {
        return w1.extractPoint().equalsPoint(w2.extractPoint());
      }
      // Fast path: point-to-complex uses PointTarget (avoids building ShapeIndex for point side)
      if (w1.isPoint()) {
        return Predicates.S2_intersectsPointWithIndex(w1.extractPoint(), toShapeIndex(w2));
      }
      if (w2.isPoint()) {
        return Predicates.S2_intersectsPointWithIndex(w2.extractPoint(), toShapeIndex(w1));
      }
    }
    // General path via ShapeIndex
    Predicates pred = new Predicates();
    return pred.S2_intersects(toShapeIndex(g1), toShapeIndex(g2), s2Options());
  }

  /** Return EWKT for geography object */
  public static String asEWKT(Geography geography) {
    return geography.toEWKT();
  }

  // ─── Level 4: spherical buffer ───────────────────────────────────────────

  /**
   * Returns a Geography that represents the metric ε-buffer of {@code g} on the sphere, where
   * {@code radiusMeters} is interpreted as meters along the spheroid. Implementation reuses the
   * existing geometry-side spheroidal buffer (UTM project → JTS planar buffer → unproject), which
   * gives accurate sub-UTM-zone results; for very large geographies the UTM round-trip's accuracy
   * caveats apply (see ST_Buffer's docs).
   */
  public static Geography buffer(Geography g, double radiusMeters) {
    return buffer(g, radiusMeters, "");
  }

  /**
   * Geography is inherently spheroidal, so the {@code useSpheroid} flag (only meaningful for the
   * planar Geometry version of ST_Buffer) is rejected for Geography inputs. This overload exists to
   * give a clear, actionable error when callers try to pass it; without it the resolver would
   * coerce the boolean to a string and fail later inside the buffer-parameters parser with a
   * confusing message.
   */
  public static Geography buffer(Geography g, double radiusMeters, boolean useSpheroid) {
    throw new IllegalArgumentException(
        "ST_Buffer does not accept a useSpheroid argument for Geography inputs (Geography is "
            + "always spheroidal). Use ST_Buffer(geog, distance) or "
            + "ST_Buffer(geog, distance, parameters) instead.");
  }

  /**
   * Same as {@link #buffer(Geography, double)} but allows a JTS-style buffer parameters string
   * ({@code "quad_segs=8 endcap=round join=round mitre_limit=5.0 side=both"}). The string is parsed
   * by the existing geometry-side parser.
   */
  public static Geography buffer(Geography g, double radiusMeters, String parameters) {
    if (g == null) return null;
    Geometry jts = toJTS(g);
    if (jts == null) return null;
    int srid = g.getSRID();
    // Geography is always lon/lat; default to WGS84 when the source has no SRID set.
    jts.setSRID(srid != 0 ? srid : 4326);
    Geometry buffered =
        org.apache.sedona.common.Functions.buffer(jts, radiusMeters, true, parameters);
    if (buffered == null) return null;
    Geography result = Constructors.geomToGeography(buffered);
    result.setSRID(srid);
    return result;
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
