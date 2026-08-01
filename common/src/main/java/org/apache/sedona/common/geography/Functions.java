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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.sphere.Haversine;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

public class Functions {

  private static final double EPSILON = 1e-9;
  private static final S1Angle PROJECT_PERPENDICULAR_ERROR =
      S1Angle.radians((2.0 + 2.0 / Math.sqrt(3.0)) * S2.DBL_ERROR).add(S2.ROBUST_CROSS_PROD_ERROR);
  // A tiny positive snap radius activates S2Builder's forced edge/site processing. Every input
  // vertex is forced below, so vertices cannot move; the radius is many orders below double
  // precision at unit scale. Double.MIN_VALUE underflows in the chord-angle calculations.
  private static final S2BuilderSnapFunctions.IdentitySnapFunction
      INTERSECTION_ALIGNMENT_SNAP_FUNCTION =
          new S2BuilderSnapFunctions.IdentitySnapFunction(
              S1Angle.radians(S2.DBL_EPSILON * S2.DBL_EPSILON));
  // S2Builder considers a forced site up to maxEdgeDeviation (1.1r) plus the snap function's
  // minimum edge/vertex separation. Keep the fast-path detector in sync so it can never skip a
  // vertex that the alignment builder would insert into the other input.
  private static final S1Angle INTERSECTION_EDGE_SITE_RADIUS =
      INTERSECTION_ALIGNMENT_SNAP_FUNCTION
          .snapRadius()
          .mul(1.1)
          .add(INTERSECTION_ALIGNMENT_SNAP_FUNCTION.minEdgeVertexSeparation());
  private static final double INTERSECTION_EDGE_SITE_RADIUS_LENGTH_2 =
      S1ChordAngle.fromS1Angle(INTERSECTION_EDGE_SITE_RADIUS).getLength2();
  // S2 expands conservative rectangle bounds by a few floating-point ulps. Snap only those tiny
  // expansions back to a source ordinate; genuine great-circle extrema remain unchanged.
  private static final double BOUND_ORDINATE_SNAP_TOLERANCE_DEGREES = 1e-12;

  private static boolean nearlyEqual(double a, double b) {
    if (Double.isNaN(a) || Double.isNaN(b)) {
      return false;
    }
    return Math.abs(a - b) < EPSILON;
  }

  public static Geography getEnvelope(Geography geography, boolean splitAtAntiMeridian) {
    if (geography == null) return null;
    // Empty Point WKB stores NaN ordinates, which getPointX() reports as null. The isPoint() guard
    // distinguishes that case from non-point inputs, for which getPointX() also returns null.
    // Avoid constructing an S2PointRegion from the NaN ordinates.
    if (geography instanceof WKBGeography
        && ((WKBGeography) geography).isPoint()
        && ((WKBGeography) geography).getPointX() == null) {
      return geography;
    }
    Geometry sourceGeometry = toJTS(geography);
    S2LatLngRect rect = geography.region().getRectBound();
    // Match the Geometry overload by preserving an empty input's type and SRID.
    if (rect.isEmpty()) return geography;
    double lngLo = rect.lngLo().degrees();
    double latLo = rect.latLo().degrees();
    double lngHi = rect.lngHi().degrees();
    double latHi = rect.latHi().degrees();
    Coordinate[] sourceCoordinates = sourceGeometry.getCoordinates();
    lngLo = snapToSourceOrdinate(lngLo, sourceCoordinates, true);
    latLo = snapToSourceOrdinate(latLo, sourceCoordinates, false);
    lngHi = snapToSourceOrdinate(lngHi, sourceCoordinates, true);
    latHi = snapToSourceOrdinate(latHi, sourceCoordinates, false);
    GeometryFactory geometryFactory =
        new GeometryFactory(new PrecisionModel(), geography.getSRID());

    if (nearlyEqual(latLo, latHi) && nearlyEqual(lngLo, lngHi)) {
      if (sourceGeometry instanceof Point && !sourceGeometry.isEmpty()) {
        Coordinate coordinate = sourceGeometry.getCoordinate();
        return WKBGeography.fromJTS(
            geometryFactory.createPoint(new Coordinate(coordinate.x, coordinate.y)));
      }
      return WKBGeography.fromJTS(geometryFactory.createPoint(new Coordinate(lngLo, latLo)));
    }

    Geometry envelope;
    if (splitAtAntiMeridian && rect.lng().isInverted()) {
      Polygon left = rectToPolygon(geometryFactory, lngLo, latLo, 180.0, latHi);
      Polygon right = rectToPolygon(geometryFactory, -180.0, latLo, lngHi, latHi);
      envelope = geometryFactory.createMultiPolygon(new Polygon[] {left, right});
    } else {
      envelope = rectToPolygon(geometryFactory, lngLo, latLo, lngHi, latHi);
    }
    return WKBGeography.fromJTS(envelope);
  }

  private static double snapToSourceOrdinate(
      double bound, Coordinate[] sourceCoordinates, boolean longitude) {
    double closest = bound;
    double closestDifference = BOUND_ORDINATE_SNAP_TOLERANCE_DEGREES;
    for (Coordinate coordinate : sourceCoordinates) {
      double candidate = longitude ? coordinate.x : coordinate.y;
      double difference = Math.abs(bound - candidate);
      if (difference <= closestDifference) {
        closest = candidate;
        closestDifference = difference;
      }
    }
    return closest;
  }

  private static Polygon rectToPolygon(
      GeometryFactory geometryFactory, double lngLo, double latLo, double lngHi, double latHi) {
    ArrayList<S2Point> v = new ArrayList<>(4);
    v.add(S2LatLng.fromDegrees(latLo, lngLo).toPoint());
    v.add(S2LatLng.fromDegrees(latLo, lngHi).toPoint());
    v.add(S2LatLng.fromDegrees(latHi, lngHi).toPoint());
    v.add(S2LatLng.fromDegrees(latHi, lngLo).toPoint());

    S2Loop loop = new S2Loop(v);
    Coordinate[] coordinates;
    if (loop.isNormalized()) {
      coordinates =
          new Coordinate[] {
            new Coordinate(lngLo, latLo),
            new Coordinate(lngHi, latLo),
            new Coordinate(lngHi, latHi),
            new Coordinate(lngLo, latHi),
            new Coordinate(lngLo, latLo)
          };
    } else {
      // S2Loop.normalize() reverses all four vertices when the rectangle covers more than a
      // hemisphere. Mirror that ordering while retaining the exact rectangle ordinates in WKB.
      coordinates =
          new Coordinate[] {
            new Coordinate(lngLo, latHi),
            new Coordinate(lngHi, latHi),
            new Coordinate(lngHi, latLo),
            new Coordinate(lngLo, latLo),
            new Coordinate(lngLo, latHi)
          };
    }
    return geometryFactory.createPolygon(coordinates);
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
   *       S2Polyline#getCentroid()}; when every edge has zero length, the mean of the remaining
   *       vertices is used.
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
      S2Point degenerateSum = null;
      for (S2Polyline p : ((PolylineGeography) g).getPolylines()) {
        if (p.numVertices() == 1) {
          degenerateSum = addOrInit(degenerateSum, p.vertex(0));
        } else if (p.numVertices() >= 2) {
          sum = addOrInit(sum, p.getCentroid());
        }
      }
      return sum != null ? sum : degenerateSum;
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

  /** Return WKT from the geography's structural WKB/JTS representation. */
  public static String asText(Geography g) {
    if (g == null) return null;
    return toJTS(g).toText();
  }

  /** Return the longitude (X coordinate) of a point geography, or null for non-point inputs. */
  public static Double x(Geography g) {
    if (g == null) return null;
    if (g instanceof WKBGeography) return ((WKBGeography) g).getPointX();
    return org.apache.sedona.common.Functions.x(toJTS(g));
  }

  /** Return the latitude (Y coordinate) of a point geography, or null for non-point inputs. */
  public static Double y(Geography g) {
    if (g == null) return null;
    if (g instanceof WKBGeography) return ((WKBGeography) g).getPointY();
    return org.apache.sedona.common.Functions.y(toJTS(g));
  }

  /**
   * Returns the smallest convex region containing {@code g} on the sphere.
   *
   * <p>The result follows the usual convex-hull dimensionality rules: one unique vertex produces a
   * point, two or more collinear vertices produce a line, and non-collinear vertices produce a
   * polygon whose edges are geodesics. Empty inputs preserve their input type.
   *
   * @throws UnsupportedOperationException when the hull is the full sphere, which cannot be
   *     represented by OGC WKB
   */
  public static Geography convexHull(Geography g) {
    if (g == null) return null;
    if (g instanceof WKBGeography && ((WKBGeography) g).isEmpty()) return g;
    if (!(g instanceof WKBGeography) && g.numShapes() == 0) return g;

    Geography typed = (g instanceof WKBGeography) ? ((WKBGeography) g).getS2Geography() : g;
    S2ConvexHullQuery query = new S2ConvexHullQuery();
    List<S2Point> vertices = new ArrayList<>();
    Geometry sourceGeometry = null;
    boolean needsJtsFallback = addPointAndLineVertices(typed, query, vertices);
    if (needsJtsFallback) {
      sourceGeometry = toJTS(g);
      if (sourceGeometry != null) {
        addJtsPointAndLineVertices(sourceGeometry, query, vertices);
      }
    }
    boolean hasPolygon = addPolygonRegions(typed, query, vertices);
    if (vertices.isEmpty() && !hasPolygon) return g;

    S2Loop loop = query.getConvexHull();
    if (loop.isFull()) {
      throw new UnsupportedOperationException(
          "ST_ConvexHull produced the full sphere, which cannot be represented as OGC WKB");
    }

    S2Point[] degenerateHull = vertices.isEmpty() ? null : getDegenerateHull(vertices);
    if (degenerateHull != null) {
      if (sourceGeometry == null) sourceGeometry = toJTS(g);
      return createDegenerateHull(sourceGeometry, degenerateHull, g.getSRID());
    }

    if (sourceGeometry == null) sourceGeometry = toJTS(g);
    return createPolygonHull(sourceGeometry, loop, g.getSRID());
  }

  /**
   * Collects geographies into the corresponding multi-geography or geography collection. Null
   * elements are ignored, member order and duplicates are retained, and the first non-null input
   * supplies the output SRID.
   */
  public static Geography createMultiGeography(Geography[] geographies) {
    List<Geometry> geometries = new ArrayList<>();
    Integer srid = null;
    for (Geography geography : geographies) {
      if (geography == null) continue;
      if (srid == null) srid = geography.getSRID();
      Geometry geometry = toJTS(geography);
      if (geometry != null) geometries.add(geometry);
    }

    Geometry result =
        org.apache.sedona.common.Functions.createMultiGeometry(geometries.toArray(new Geometry[0]));
    if (srid != null) result.setSRID(srid);
    return WKBGeography.fromJTS(result);
  }

  /**
   * Adds point and polyline vertices from the S2 representation.
   *
   * @return whether an empty S2 polyline needs a JTS fallback to recover coincident source vertices
   *     that S2Builder collapsed
   */
  private static boolean addPointAndLineVertices(
      Geography geography, S2ConvexHullQuery query, List<S2Point> vertices) {
    if (geography instanceof PointGeography) {
      for (S2Point point : ((PointGeography) geography).getPoints()) {
        query.addPoint(point);
        vertices.add(point);
      }
      return false;
    }
    if (geography instanceof PolylineGeography) {
      List<S2Polyline> polylines = ((PolylineGeography) geography).getPolylines();
      boolean needsJtsFallback = polylines.isEmpty();
      for (S2Polyline polyline : polylines) {
        if (polyline.numVertices() < 2) {
          needsJtsFallback = true;
        } else {
          query.addPolyline(polyline);
          vertices.addAll(polyline.vertices());
        }
      }
      return needsJtsFallback;
    }
    if (geography instanceof GeographyCollection) {
      boolean needsJtsFallback = false;
      for (Geography feature : ((GeographyCollection) geography).getFeatures()) {
        needsJtsFallback |= addPointAndLineVertices(feature, query, vertices);
      }
      return needsJtsFallback;
    }
    return false;
  }

  private static void addJtsPointAndLineVertices(
      Geometry geometry, S2ConvexHullQuery query, List<S2Point> vertices) {
    if (geometry instanceof Polygon) return;
    if (geometry instanceof Point || geometry instanceof LineString) {
      for (org.locationtech.jts.geom.Coordinate coordinate : geometry.getCoordinates()) {
        S2Point vertex = S2LatLng.fromDegrees(coordinate.getY(), coordinate.getX()).toPoint();
        query.addPoint(vertex);
        vertices.add(vertex);
      }
      return;
    }
    if (geometry instanceof GeometryCollection) {
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        addJtsPointAndLineVertices(geometry.getGeometryN(i), query, vertices);
      }
    }
  }

  /**
   * Adds polygon regions to the hull query. Polygons must be added as regions rather than as vertex
   * sets so the query respects the S2 representation's resolved interior. Public Geography readers
   * normalize shells to at most one hemisphere, while directly constructed S2 Geography values can
   * still represent a larger directed region whose hull is the full sphere.
   *
   * @return whether a non-empty polygon was added
   */
  private static boolean addPolygonRegions(
      Geography geography, S2ConvexHullQuery query, List<S2Point> vertices) {
    if (geography instanceof PolygonGeography) {
      S2Polygon polygon = ((PolygonGeography) geography).polygon;
      if (polygon.isEmpty()) return false;
      query.addPolygon(polygon);
      for (int i = 0; i < polygon.numLoops(); i++) {
        S2Loop loop = polygon.loop(i);
        if (loop.depth() == 0 && !loop.isEmptyOrFull()) {
          vertices.addAll(loop.vertices());
        }
      }
      return true;
    }
    if (geography instanceof GeographyCollection) {
      boolean hasPolygon = false;
      for (Geography feature : ((GeographyCollection) geography).getFeatures()) {
        hasPolygon |= addPolygonRegions(feature, query, vertices);
      }
      return hasPolygon;
    }
    return false;
  }

  /**
   * Returns either one point for a point hull, two endpoints for a collinear hull, or {@code null}
   * for a polygonal hull. The two-pass farthest-point search is linear and finds the endpoints of
   * any set contained by one geodesic segment.
   */
  private static S2Point[] getDegenerateHull(List<S2Point> vertices) {
    S2Point firstEndpoint = farthestPoint(vertices.get(0), vertices);
    S2Point secondEndpoint = farthestPoint(firstEndpoint, vertices);
    S1Angle span = new S1Angle(firstEndpoint, secondEndpoint);
    if (span.lessOrEquals(PROJECT_PERPENDICULAR_ERROR)) {
      return new S2Point[] {firstEndpoint};
    }

    for (S2Point vertex : vertices) {
      if (S2EdgeUtil.getDistance(vertex, firstEndpoint, secondEndpoint)
          .greaterThan(PROJECT_PERPENDICULAR_ERROR)) {
        return null;
      }
    }
    return new S2Point[] {firstEndpoint, secondEndpoint};
  }

  private static S2Point farthestPoint(S2Point origin, List<S2Point> vertices) {
    S2Point farthest = origin;
    S1ChordAngle farthestDistance = S1ChordAngle.ZERO;
    for (S2Point vertex : vertices) {
      S1ChordAngle distance = new S1ChordAngle(origin, vertex);
      if (distance.greaterThan(farthestDistance)) {
        farthest = vertex;
        farthestDistance = distance;
      }
    }
    return farthest;
  }

  /**
   * Writes a degenerate hull from its exact source coordinates. S2 still selects the spherical
   * endpoint(s), but converting those points back to longitude/latitude would introduce
   * floating-point drift into an otherwise unchanged input vertex.
   */
  private static Geography createDegenerateHull(
      Geometry sourceGeometry, S2Point[] endpoints, int srid) {
    SourceCoordinateIndex sourceIndex = new SourceCoordinateIndex(sourceGeometry.getCoordinates());
    GeometryFactory factory = new GeometryFactory(new PrecisionModel(), srid);
    Coordinate first = sourceIndex.resolve(endpoints[0]);
    Geometry result;
    if (endpoints.length == 1) {
      result = factory.createPoint(first);
    } else {
      Coordinate second = sourceIndex.resolve(endpoints[1]);
      result = factory.createLineString(new Coordinate[] {first, second});
    }
    return WKBGeography.fromJTS(result);
  }

  /**
   * Writes a polygonal hull from the exact source coordinates selected by S2. For a non-degenerate
   * hull, S2ConvexHullQuery returns a subset of its input vertices, so no computed vertex needs to
   * be rounded back to longitude/latitude.
   */
  private static Geography createPolygonHull(Geometry sourceGeometry, S2Loop loop, int srid) {
    SourceCoordinateIndex sourceIndex = new SourceCoordinateIndex(sourceGeometry.getCoordinates());
    Coordinate[] hullCoordinates = new Coordinate[loop.numVertices() + 1];
    for (int i = 0; i < loop.numVertices(); i++) {
      hullCoordinates[i] = sourceIndex.resolve(loop.vertex(i));
    }
    hullCoordinates[loop.numVertices()] = new Coordinate(hullCoordinates[0]);

    GeometryFactory factory = new GeometryFactory(new PrecisionModel(), srid);
    return WKBGeography.fromJTS(factory.createPolygon(hullCoordinates));
  }

  /**
   * Resolves the S2 vertices selected by the convex-hull query to their exact source coordinates.
   * S2ConvexHullQuery retains input vertices for non-degenerate hulls, so the normal path is an
   * exact hash lookup. The nearest-coordinate fallback covers directly constructed S2 Geography
   * values whose JTS conversion may not reproduce the same S2Point bits.
   */
  private static final class SourceCoordinateIndex {
    private final Coordinate[] sourceCoordinates;
    private final Map<S2Point, Coordinate> exactCoordinates = new HashMap<>();

    SourceCoordinateIndex(Coordinate[] sourceCoordinates) {
      this.sourceCoordinates = sourceCoordinates;
      for (Coordinate coordinate : sourceCoordinates) {
        if (!Double.isFinite(coordinate.x) || !Double.isFinite(coordinate.y)) continue;
        S2Point point = S2LatLng.fromDegrees(coordinate.y, coordinate.x).toPoint();
        // Keep the first source spelling when equivalent coordinates map to the same S2 point.
        exactCoordinates.putIfAbsent(point, new Coordinate(coordinate.x, coordinate.y));
      }
    }

    SourceCoordinateIndex(Coordinate[] first, Coordinate[] second) {
      this(concatenateCoordinates(first, second));
    }

    private static Coordinate[] concatenateCoordinates(Coordinate[] first, Coordinate[] second) {
      Coordinate[] coordinates = new Coordinate[first.length + second.length];
      System.arraycopy(first, 0, coordinates, 0, first.length);
      System.arraycopy(second, 0, coordinates, first.length, second.length);
      return coordinates;
    }

    Coordinate resolve(S2Point point) {
      Coordinate coordinate = exactCoordinates.get(point);
      if (coordinate != null) {
        return new Coordinate(coordinate.x, coordinate.y);
      }
      return nearestSourceCoordinate(point, sourceCoordinates);
    }

    Coordinate resolveExact(S2Point point) {
      Coordinate coordinate = exactCoordinates.get(point);
      return coordinate == null ? null : new Coordinate(coordinate.x, coordinate.y);
    }
  }

  private static Coordinate nearestSourceCoordinate(
      S2Point endpoint, Coordinate[] sourceCoordinates) {
    Coordinate nearest = null;
    double nearestDistance = Double.POSITIVE_INFINITY;
    for (Coordinate coordinate : sourceCoordinates) {
      if (!Double.isFinite(coordinate.x) || !Double.isFinite(coordinate.y)) continue;
      S2Point candidate = S2LatLng.fromDegrees(coordinate.y, coordinate.x).toPoint();
      double distance = endpoint.getDistance2(candidate);
      if (distance < nearestDistance) {
        nearest = coordinate;
        nearestDistance = distance;
      }
    }
    if (nearest == null) {
      throw new IllegalArgumentException("Cannot construct a convex hull without finite vertices");
    }
    return new Coordinate(nearest.x, nearest.y);
  }

  /**
   * Creates a line from two Point, MultiPoint, or LineString geographies. The returned geography
   * preserves the first input's SRID; its edges are interpreted as great-circle arcs by geography
   * measurement functions. No CRS transformation or SRID compatibility check is performed; if the
   * inputs have different SRIDs, the first input's SRID is used. When the second input is a
   * LineString whose first coordinate equals the current endpoint, that seam coordinate is added
   * only once. Point and MultiPoint coordinates are always retained. Empty inputs contribute no
   * coordinates; when exactly one coordinate remains, it is repeated to form a valid LineString.
   */
  public static Geography makeLine(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return null;
    Geometry jts1 = toJTS(g1);
    Geometry jts2 = toJTS(g2);
    if (jts1 == null || jts2 == null) return null;
    if (!isMakeLineInput(jts1) || !isMakeLineInput(jts2)) {
      throw new IllegalArgumentException(
          "ST_MakeLine only supports Point, MultiPoint and LineString geographies");
    }
    Geometry line = makeLineGeometry(jts1, jts2);
    line.setSRID(g1.getSRID());
    // Preserve the JTS coordinate sequence verbatim. Converting through S2 here would collapse
    // zero-length edges and repeated vertices, changing ST_MakeLine's result and potentially
    // producing an empty LineString that cannot round-trip through the Geography WKB reader.
    return WKBGeography.fromJTS(line);
  }

  private static Geometry makeLineGeometry(Geometry first, Geometry second) {
    List<Coordinate> coordinates = new ArrayList<>();
    for (Coordinate coordinate : first.getCoordinates()) {
      coordinates.add(coordinate);
    }

    Coordinate[] appended = second.getCoordinates();
    int start = 0;
    if (second instanceof LineString
        && !coordinates.isEmpty()
        && appended.length > 0
        && coordinates.get(coordinates.size() - 1).equals2D(appended[0])) {
      start = 1;
    }
    for (int i = start; i < appended.length; i++) {
      coordinates.add(appended[i]);
    }

    // PostGIS and SedonaDB skip empty components. JTS cannot represent their one-coordinate
    // LineString result, so repeat that coordinate while preserving the same point set.
    if (coordinates.size() == 1) {
      coordinates.add(new Coordinate(coordinates.get(0)));
    }
    return first.getFactory().createLineString(coordinates.toArray(new Coordinate[0]));
  }

  private static boolean isMakeLineInput(Geometry geometry) {
    return geometry instanceof Point
        || geometry instanceof MultiPoint
        || geometry instanceof LineString;
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
    // Public Geography readers normalize polygon shells to at most one hemisphere, but callers can
    // still supply a directed S2 Geography whose interior is the complementary large region.
    // Preserve ST_Area's small-side contract for both representations.
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

  /**
   * Spherical "distance within" test. Returns true iff the minimum geodesic distance between g1 and
   * g2 (in meters) is less than or equal to {@code distanceMeters}.
   */
  public static boolean dWithin(Geography g1, Geography g2, double distanceMeters) {
    if (g1 == null || g2 == null) return false;
    Double d = distance(g1, g2);
    return d != null && d <= distanceMeters;
  }

  /**
   * Spherical "within" test. Returns true iff g1 is fully inside g2 on the sphere. OGC convention:
   * {@code ST_Within(A, B) == ST_Contains(B, A)}.
   */
  public static boolean within(Geography g1, Geography g2) {
    return contains(g2, g1);
  }

  /** Return EWKT from the geography's structural WKB/JTS representation. */
  public static String asEWKT(Geography geography) {
    if (geography == null) return null;
    String text = asText(geography);
    return geography.getSRID() > 0 ? "SRID=" + geography.getSRID() + "; " + text : text;
  }

  /**
   * Returns the closed-set spherical intersection of two geographies. The overlay is evaluated by
   * S2 on geodesic edges and encoded as a two-dimensional OGC geography. When several output
   * dimensions are present, the result is a geometry collection ordered by increasing dimension.
   * The SRID of the first input is preserved without transforming either input.
   */
  public static Geography intersection(Geography g1, Geography g2) {
    if (g1 == null || g2 == null) return null;

    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), g1.getSRID());

    // Match SedonaDB: an explicitly empty input produces GEOMETRYCOLLECTION EMPTY, while an empty
    // result from two non-empty inputs retains the minimum input dimension.
    IntersectionInput input1 = intersectionInput(g1);
    if (input1.empty) {
      return WKBGeography.fromJTS(geometryFactory.createGeometryCollection());
    }
    IntersectionInput input2 = intersectionInput(g2);
    if (input2.empty) {
      return WKBGeography.fromJTS(geometryFactory.createGeometryCollection());
    }

    S2PointVectorLayer pointLayer = new S2PointVectorLayer();
    S2PolylineVectorLayer.Options polylineOptions =
        new S2PolylineVectorLayer.Options()
            .setEdgeType(S2Builder.EdgeType.UNDIRECTED)
            .setPolylineType(S2BuilderGraph.PolylineType.WALK)
            .setDuplicateEdges(S2Builder.GraphOptions.DuplicateEdges.MERGE);
    S2PolylineVectorLayer polylineLayer = new S2PolylineVectorLayer(polylineOptions);
    S2PolygonLayer polygonLayer = new S2PolygonLayer();

    // A closed-set normalizer assigns boundary-only intersections to the lowest dimension that
    // represents them (for example, two crossing lines intersect in a Point rather than a
    // degenerate LineString) and suppresses lower-dimensional output that duplicates a
    // higher-dimensional vertex or edge.
    S2ClosedSetNormalizer normalizer =
        new S2ClosedSetNormalizer(pointLayer, polylineLayer, polygonLayer);
    S2BooleanOperation operation =
        new S2BooleanOperation.Builder()
            .setPolygonModel(S2BooleanOperation.PolygonModel.CLOSED)
            .setPolylineModel(S2BooleanOperation.PolylineModel.CLOSED)
            .build(
                S2BooleanOperation.OpType.INTERSECTION,
                normalizer.pointLayer(),
                normalizer.lineLayer(),
                normalizer.polygonLayer());

    S2ShapeIndex[] inputs =
        (input1.dimension > 0 || input2.dimension > 0)
                && needsIntersectionAlignment(input1.shapeIndex, input2.shapeIndex)
            ? alignIntersectionInputs(input1.shapeIndex, input2.shapeIndex)
            : new S2ShapeIndex[] {input1.shapeIndex, input2.shapeIndex};
    S2Error error = new S2Error();
    if (!operation.build(inputs[0], inputs[1], error)) {
      throw new IllegalArgumentException(
          "Failed to compute Geography intersection: " + error.text());
    }

    List<S2Point> resultPoints = pointLayer.getPointVector();
    List<S2Polyline> resultPolylines = polylineLayer.getPolylines();
    S2Polygon resultPolygon = polygonLayer.getPolygon();
    SourceCoordinateIndex sourceCoordinates =
        resultPoints.isEmpty() && resultPolylines.isEmpty() && resultPolygon.isEmpty()
            ? null
            : intersectionSourceCoordinates(g1, g2);
    Geometry result =
        intersectionResult(
            resultPoints,
            resultPolylines,
            resultPolygon,
            Math.min(input1.dimension, input2.dimension),
            geometryFactory,
            sourceCoordinates);
    return WKBGeography.fromJTS(result);
  }

  /**
   * Indexes original coordinate spelling only for WKB-backed inputs. Native S2 Geography values
   * have already lost that spelling, and converting them to JTS here can reject valid S2
   * degeneracies such as a one-vertex polyline.
   */
  private static SourceCoordinateIndex intersectionSourceCoordinates(
      Geography first, Geography second) {
    Coordinate[] firstCoordinates =
        first instanceof WKBGeography
            ? ((WKBGeography) first).getJTSGeometry().getCoordinates()
            : null;
    Coordinate[] secondCoordinates =
        second instanceof WKBGeography
            ? ((WKBGeography) second).getJTSGeometry().getCoordinates()
            : null;
    if (firstCoordinates == null) {
      return secondCoordinates == null ? null : new SourceCoordinateIndex(secondCoordinates);
    }
    return secondCoordinates == null
        ? new SourceCoordinateIndex(firstCoordinates)
        : new SourceCoordinateIndex(firstCoordinates, secondCoordinates);
  }

  private static IntersectionInput intersectionInput(Geography geography) {
    int dimension = geography.dimension();
    if (geography instanceof WKBGeography && ((WKBGeography) geography).isEmpty()) {
      return new IntersectionInput(null, dimension, true);
    }

    S2ShapeIndex shapeIndex = toShapeIndex(geography).shapeIndex;
    boolean empty = true;
    int maximumShapeDimension = -1;
    for (S2Shape shape : shapeIndex.getShapes()) {
      empty &= shape.isEmpty();
      maximumShapeDimension = Math.max(maximumShapeDimension, shape.dimension());
    }
    if (dimension < 0) dimension = maximumShapeDimension;
    return new IntersectionInput(shapeIndex, dimension, empty);
  }

  private static final class IntersectionInput {
    private final S2ShapeIndex shapeIndex;
    private final int dimension;
    private final boolean empty;

    private IntersectionInput(S2ShapeIndex shapeIndex, int dimension, boolean empty) {
      this.shapeIndex = shapeIndex;
      this.dimension = dimension;
      this.empty = empty;
    }
  }

  /**
   * Returns whether either input has a vertex in the interior of an edge from the other input.
   * Proper edge crossings are split by {@link S2BooleanOperation} itself, while shared endpoints
   * are already explicit in both indexes. Avoiding a full {@link S2Builder} pass for those common
   * cases keeps ordinary polygon overlays close to the native boolean-operation cost.
   */
  private static boolean needsIntersectionAlignment(S2ShapeIndex input1, S2ShapeIndex input2) {
    return hasVertexInEdgeInterior(input1, input2) || hasVertexInEdgeInterior(input2, input1);
  }

  private static boolean hasVertexInEdgeInterior(S2ShapeIndex vertexIndex, S2ShapeIndex edgeIndex) {
    HashSet<S2Point> vertices = new HashSet<>();
    S2Shape.MutableEdge edge = new S2Shape.MutableEdge();
    for (S2Shape shape : vertexIndex.getShapes()) {
      for (int edgeId = 0; edgeId < shape.numEdges(); edgeId++) {
        shape.getEdge(edgeId, edge);
        vertices.add(edge.a);
        vertices.add(edge.b);
      }
    }
    if (vertices.isEmpty()) return false;

    S2ClosestEdgeQuery.Query query =
        S2ClosestEdgeQuery.builder()
            .setConservativeMaxDistance(INTERSECTION_EDGE_SITE_RADIUS)
            .setIncludeInteriors(false)
            .build(edgeIndex);
    List<S2Shape> edgeShapes = edgeIndex.getShapes();
    S2Shape.MutableEdge candidate = new S2Shape.MutableEdge();
    boolean[] found = new boolean[1];
    for (S2Point vertex : vertices) {
      found[0] = false;
      // Visit every edge inside the conservative radius. One may share this endpoint while another
      // has the same vertex in its interior, as happens at a T-junction within a collection.
      query.findClosestEdges(
          new S2ClosestEdgeQuery.PointTarget<S1ChordAngle>(vertex),
          (distance, shapeId, edgeId) -> {
            S2Shape shape = edgeShapes.get(shapeId);
            if (shape.dimension() == 0) return true;
            shape.getEdge(edgeId, candidate);
            if (vertex.equalsPoint(candidate.a) || vertex.equalsPoint(candidate.b)) return true;
            if (S2Predicates.compareEdgeDistance(
                    vertex, candidate.a, candidate.b, INTERSECTION_EDGE_SITE_RADIUS_LENGTH_2)
                <= 0) {
              found[0] = true;
              return false;
            }
            return true;
          });
      if (found[0]) return true;
    }
    return false;
  }

  /**
   * Rebuilds both inputs with one shared {@link S2Builder}. S2BooleanOperation splits proper edge
   * crossings, but it does not split an edge at a point or vertex in the other input. Sharing the
   * builder's sites makes those T-vertices explicit in both indexes, so point-on-edge and
   * collinear-overlap results retain their complete closed set.
   */
  private static S2ShapeIndex[] alignIntersectionInputs(S2ShapeIndex input1, S2ShapeIndex input2) {
    S2ShapeIndex output1 = new S2ShapeIndex();
    S2ShapeIndex output2 = new S2ShapeIndex();
    S2Builder builder = new S2Builder.Builder(INTERSECTION_ALIGNMENT_SNAP_FUNCTION).build();

    forceIntersectionInputVertices(builder, input1);
    forceIntersectionInputVertices(builder, input2);
    addIntersectionInputLayers(builder, input1, output1);
    addIntersectionInputLayers(builder, input2, output2);

    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IllegalArgumentException(
          "Failed to align Geography intersection inputs: " + error.text());
    }
    return new S2ShapeIndex[] {output1, output2};
  }

  private static void forceIntersectionInputVertices(S2Builder builder, S2ShapeIndex input) {
    S2Shape.MutableEdge edge = new S2Shape.MutableEdge();
    for (S2Shape shape : input.getShapes()) {
      for (int edgeId = 0; edgeId < shape.numEdges(); edgeId++) {
        shape.getEdge(edgeId, edge);
        builder.forceVertex(edge.a);
        builder.forceVertex(edge.b);
      }
    }
  }

  private static void addIntersectionInputLayers(
      S2Builder builder, S2ShapeIndex input, S2ShapeIndex output) {
    for (S2Shape shape : input.getShapes()) {
      S2BuilderShapesLayer layer;
      switch (shape.dimension()) {
        case 0:
          layer = new S2PointVectorLayer();
          break;
        case 1:
          layer = new S2PolylineVectorLayer();
          break;
        case 2:
          // Keep degenerate polygon boundaries so the closed-set normalizer can demote them to
          // their point or line set instead of silently dropping them during input alignment.
          layer = new S2LaxPolygonLayer();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported Geography dimension for intersection: " + shape.dimension());
      }
      builder.startLayer(new S2BuilderUtil.IndexedLayer<>(output, layer));
      if (shape.dimension() == 2) {
        builder.addIsFullPolygonPredicate(S2Builder.isFullPolygon(shape.isFull()));
      }
      builder.addShape(shape);
    }
  }

  private static Geometry intersectionResult(
      List<S2Point> points,
      List<S2Polyline> polylines,
      S2Polygon polygon,
      int emptyDimension,
      GeometryFactory geometryFactory,
      SourceCoordinateIndex sourceCoordinates) {
    List<S2Point> outputPoints = new ArrayList<>(points);
    List<LineString> outputLines = new ArrayList<>(polylines.size());
    for (S2Polyline polyline : polylines) {
      if (polyline.numVertices() == 1) {
        // The closed-set normalizer normally routes degeneracies to the point layer. Preserve the
        // set correctly if an output-layer option ever leaves one here.
        outputPoints.add(polyline.vertex(0));
        continue;
      }
      Coordinate[] coordinates = new Coordinate[polyline.numVertices()];
      for (int i = 0; i < polyline.numVertices(); i++) {
        coordinates[i] = intersectionCoordinate(polyline.vertex(i), sourceCoordinates);
      }
      outputLines.add(geometryFactory.createLineString(coordinates));
    }

    List<Geometry> components = new ArrayList<>(3);
    if (!outputPoints.isEmpty()) {
      Coordinate[] coordinates = new Coordinate[outputPoints.size()];
      for (int i = 0; i < outputPoints.size(); i++) {
        coordinates[i] = intersectionCoordinate(outputPoints.get(i), sourceCoordinates);
      }
      components.add(
          coordinates.length == 1
              ? geometryFactory.createPoint(coordinates[0])
              : geometryFactory.createMultiPointFromCoords(coordinates));
    }

    if (!outputLines.isEmpty()) {
      components.add(
          outputLines.size() == 1
              ? outputLines.get(0)
              : geometryFactory.createMultiLineString(outputLines.toArray(new LineString[0])));
    }

    if (!polygon.isEmpty()) {
      Geometry polygonGeometry =
          Constructors.s2PolygonToGeometry(
              polygon, geometryFactory, point -> intersectionCoordinate(point, sourceCoordinates));
      if (!polygonGeometry.isEmpty()) components.add(polygonGeometry);
    }

    if (components.isEmpty()) {
      switch (emptyDimension) {
        case 0:
          return geometryFactory.createPoint();
        case 1:
          return geometryFactory.createLineString();
        case 2:
          return geometryFactory.createPolygon();
        default:
          return geometryFactory.createGeometryCollection();
      }
    }
    if (components.size() == 1) return components.get(0);
    return geometryFactory.createGeometryCollection(components.toArray(new Geometry[0]));
  }

  private static Coordinate intersectionCoordinate(
      S2Point point, SourceCoordinateIndex sourceCoordinates) {
    if (sourceCoordinates != null) {
      Coordinate source = sourceCoordinates.resolveExact(point);
      if (source != null) return source;
    }
    return toCoordinate(point);
  }

  private static Coordinate toCoordinate(S2Point point) {
    S2LatLng latLng = S2LatLng.fromPoint(point).normalized();
    return new Coordinate(latLng.lngDegrees(), latLng.latDegrees());
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
    // JTS buffer shells are commonly wound for planar geometry semantics. Normalize them through
    // S2 before storing the computed result so the geography keeps the same spherical interior it
    // had before geomToGeography began preserving caller-provided WKB verbatim.
    Geography result = WKBGeography.fromS2Geography(Constructors.geomToS2Geography(buffered));
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
