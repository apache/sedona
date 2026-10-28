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
package org.apache.sedona.common.raster;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.S2Geography.Distance;
import org.apache.sedona.common.S2Geography.ShapeIndexGeography;
import org.apache.sedona.common.S2Geography.WkbS2Shape;
import org.apache.sedona.common.sphere.Haversine;
import org.apache.sedona.common.utils.CachedCRSTransformFinder;
import org.apache.sedona.common.utils.GeomUtils;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.ReferenceIdentifier;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.crs.GeographicCRS;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

public class RasterPredicates {
  /**
   * Test if a raster intersects a query window. When the CRS differs, the query window is
   * transformed into the raster's CRS so the raster footprint stays exact (affine pixel→world maps
   * lines to lines). Falls back to comparing both sides in WGS84 if that transform fails. Please
   * note that the CRS transformation will be lenient, which means that the transformation may not
   * be accurate.
   *
   * @param raster the raster
   * @param geometry the query window
   * @return true if the raster intersects the query window
   */
  public static boolean rsIntersects(GridCoverage2D raster, Geometry geometry) {
    Pair<Geometry, Geometry> geometries = convertCRSIfNeeded(raster, geometry);
    Geometry rasterGeometry = geometries.getLeft();
    Geometry queryWindow = geometries.getRight();
    return rasterGeometry.intersects(queryWindow);
  }

  public static boolean rsContains(GridCoverage2D raster, Geometry geometry) {
    Pair<Geometry, Geometry> geometries = convertCRSIfNeeded(raster, geometry);
    Geometry rasterGeometry = geometries.getLeft();
    Geometry queryWindow = geometries.getRight();
    return rasterGeometry.contains(queryWindow);
  }

  public static boolean rsWithin(GridCoverage2D raster, Geometry geometry) {
    Pair<Geometry, Geometry> geometries = convertCRSIfNeeded(raster, geometry);
    Geometry rasterGeometry = geometries.getLeft();
    Geometry queryWindow = geometries.getRight();
    return rasterGeometry.within(queryWindow);
  }

  public static boolean rsIntersects(GridCoverage2D left, GridCoverage2D right) {
    Pair<Geometry, Geometry> geometries = convertCRSIfNeeded(left, right);
    Geometry leftGeometry = geometries.getLeft();
    Geometry rightGeometry = geometries.getRight();
    return leftGeometry.intersects(rightGeometry);
  }

  public static boolean rsContains(GridCoverage2D left, GridCoverage2D right) {
    Pair<Geometry, Geometry> geometries = convertCRSIfNeeded(left, right);
    Geometry leftGeometry = geometries.getLeft();
    Geometry rightGeometry = geometries.getRight();
    return leftGeometry.contains(rightGeometry);
  }

  /**
   * Test if a raster is within {@code distance} meters of a geometry. Both shapes are projected to
   * WGS84 unconditionally so the distance unit is always meters regardless of input CRS, then the
   * minimum spherical distance between the two shapes (NOT centroid-to-centroid — uses S2's {@code
   * ClosestEdgeQuery}) is compared against the threshold. Two raster footprints that overlap or
   * touch therefore satisfy {@code RS_DWithin(a, b, 0)} just like {@link #rsIntersects}, which
   * matches the natural reading of "are these shapes within d meters." The WGS84-only path is also
   * what the join planner's R-tree filter assumes — see {@code
   * TraitJoinQueryBase.toExpandedWGS84EnvelopeRDD} and the {@code isGeography = true} envelope
   * expansion — so the index bound and the per-row predicate share one unit.
   */
  public static boolean rsDWithin(GridCoverage2D raster, Geometry geometry, double distance) {
    Pair<Geometry, Geometry> geometries = toWGS84Pair(raster, geometry);
    return geographyDWithin(geometries.getLeft(), geometries.getRight(), distance);
  }

  /** Raster-raster variant of {@link #rsDWithin(GridCoverage2D, Geometry, double)}. */
  public static boolean rsDWithin(GridCoverage2D left, GridCoverage2D right, double distance) {
    Pair<Geometry, Geometry> geometries = toWGS84Pair(left, right);
    return geographyDWithin(geometries.getLeft(), geometries.getRight(), distance);
  }

  /**
   * Run S2's {@code ClosestEdgeQuery} on two WGS84 JTS geometries, interpreting ring direction as
   * the spherical interior. The result is the minimum geodesic distance — overlap/touch returns 0 —
   * so the threshold is interpreted strictly as "meters between any two points on the shapes."
   *
   * <p>This intentionally uses a private directed-ring conversion rather than public Geography
   * construction, where polygon ring positions have simple-features semantics independent of
   * winding. Raster convex hulls (especially global mosaics, polar projections, and
   * antimeridian-crossing UTM zones) can cover more than a hemisphere after WGS84 reprojection, so
   * choosing the smaller spherical side would invert the intended footprint. The directed path
   * preserves the orientation set by {@link #orientPolygonForS2} without changing Geography
   * behavior elsewhere.
   */
  private static boolean geographyDWithin(Geometry left, Geometry right, double distance) {
    boolean leftIsPoint = left instanceof Point && !left.isEmpty();
    boolean rightIsPoint = right instanceof Point && !right.isEmpty();

    double radians;
    if (leftIsPoint && rightIsPoint) {
      radians = new S1Angle(toS2Point((Point) left), toS2Point((Point) right)).radians();
    } else {
      WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
      if (leftIsPoint) {
        radians =
            Distance.S2_distancePointToIndex(
                toS2Point((Point) left), toDirectedShapeIndex(right, writer));
      } else if (rightIsPoint) {
        radians =
            Distance.S2_distancePointToIndex(
                toS2Point((Point) right), toDirectedShapeIndex(left, writer));
      } else {
        radians =
            new Distance()
                .S2_distance(
                    toDirectedShapeIndex(left, writer), toDirectedShapeIndex(right, writer));
      }
    }
    return radians * Haversine.AVG_EARTH_RADIUS <= distance;
  }

  private static S2Point toS2Point(Point point) {
    return S2LatLng.fromDegrees(point.getY(), point.getX()).toPoint();
  }

  /**
   * Builds a temporary ShapeIndex whose polygon chains retain the direction supplied by the JTS
   * geometry. Multi-geometries are decomposed into their simple components so the directed behavior
   * never leaks into the general Geography WKB reader.
   */
  private static ShapeIndexGeography toDirectedShapeIndex(Geometry geometry, WKBWriter writer) {
    ShapeIndexGeography result = new ShapeIndexGeography();
    addDirectedShapes(geometry, writer, result);
    return result;
  }

  private static void addDirectedShapes(
      Geometry geometry, WKBWriter writer, ShapeIndexGeography result) {
    if (geometry.isEmpty()) {
      return;
    }
    if (geometry instanceof Polygon) {
      Polygon oriented = orientPolygonForS2((Polygon) geometry);
      result.shapeIndex.add(WkbS2Shape.withPreservedLoopOrientation(writer.write(oriented)));
      return;
    }
    if (geometry instanceof Point || geometry instanceof LineString) {
      result.shapeIndex.add(WkbS2Shape.withPreservedLoopOrientation(writer.write(geometry)));
      return;
    }
    if (geometry instanceof GeometryCollection) {
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        addDirectedShapes(geometry.getGeometryN(i), writer, result);
      }
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported JTS geometry for raster distance: " + geometry.getGeometryType());
  }

  /**
   * Returns a polygon whose shell traverses CCW and whose holes traverse CW, as required by the
   * directed S2 shape. Ring position remains authoritative even when the JTS input uses arbitrary
   * winding. The input polygon is returned unchanged when every ring is already oriented correctly.
   */
  private static Polygon orientPolygonForS2(Polygon polygon) {
    if (polygon.isEmpty()) {
      return polygon;
    }

    LinearRing shell = (LinearRing) polygon.getExteriorRing();
    boolean changed = false;
    if (!Orientation.isCCW(shell.getCoordinates())) {
      shell = (LinearRing) shell.reverse();
      changed = true;
    }

    int numHoles = polygon.getNumInteriorRing();
    LinearRing[] holes = new LinearRing[numHoles];
    for (int i = 0; i < numHoles; i++) {
      LinearRing hole = (LinearRing) polygon.getInteriorRingN(i);
      if (Orientation.isCCW(hole.getCoordinates())) {
        hole = (LinearRing) hole.reverse();
        changed = true;
      }
      holes[i] = hole;
    }
    if (!changed) {
      return polygon;
    }

    Polygon oriented = polygon.getFactory().createPolygon(shell, holes);
    oriented.setSRID(polygon.getSRID());
    return oriented;
  }

  private static Pair<Geometry, Geometry> toWGS84Pair(GridCoverage2D raster, Geometry queryWindow) {
    Geometry rasterGeometry;
    try {
      rasterGeometry = GeometryFunctions.convexHull(raster);
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException("Failed to calculate the convex hull of the raster", e);
    }

    CoordinateReferenceSystem rasterCRS = raster.getCoordinateReferenceSystem();
    if (rasterCRS == null || rasterCRS instanceof DefaultEngineeringCRS) {
      rasterCRS = DefaultGeographicCRS.WGS84;
    }

    int queryWindowSRID = queryWindow.getSRID();
    if (queryWindowSRID <= 0) {
      queryWindowSRID = 4326;
    }
    CoordinateReferenceSystem queryWindowCRS = FunctionsGeoTools.sridToCRS(queryWindowSRID);

    Geometry transformedRaster = transformGeometryToWGS84(rasterGeometry, rasterCRS);
    Geometry transformedQuery = transformGeometryToWGS84(queryWindow, queryWindowCRS);
    return Pair.of(transformedRaster, transformedQuery);
  }

  private static Pair<Geometry, Geometry> toWGS84Pair(GridCoverage2D left, GridCoverage2D right) {
    Geometry leftGeometry;
    Geometry rightGeometry;
    try {
      leftGeometry = GeometryFunctions.convexHull(left);
      rightGeometry = GeometryFunctions.convexHull(right);
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException("Failed to calculate the convex hull of the raster", e);
    }

    CoordinateReferenceSystem leftCRS = left.getCoordinateReferenceSystem();
    if (leftCRS == null || leftCRS instanceof DefaultEngineeringCRS) {
      leftCRS = DefaultGeographicCRS.WGS84;
    }
    CoordinateReferenceSystem rightCRS = right.getCoordinateReferenceSystem();
    if (rightCRS == null || rightCRS instanceof DefaultEngineeringCRS) {
      rightCRS = DefaultGeographicCRS.WGS84;
    }

    Geometry transformedLeft = transformGeometryToWGS84(leftGeometry, leftCRS);
    Geometry transformedRight = transformGeometryToWGS84(rightGeometry, rightCRS);
    return Pair.of(transformedLeft, transformedRight);
  }

  private static Pair<Geometry, Geometry> convertCRSIfNeeded(
      GridCoverage2D raster, Geometry queryWindow) {
    Geometry rasterGeometry;
    try {
      rasterGeometry = GeometryFunctions.convexHull(raster);
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException("Failed to calculate the convex hull of the raster", e);
    }

    CoordinateReferenceSystem rasterCRS = raster.getCoordinateReferenceSystem();
    if (rasterCRS == null || rasterCRS instanceof DefaultEngineeringCRS) {
      rasterCRS = DefaultGeographicCRS.WGS84;
    }

    int queryWindowSRID = queryWindow.getSRID();
    if (queryWindowSRID <= 0) {
      queryWindowSRID = 4326;
    }

    if (isCRSMatchesSRID(rasterCRS, queryWindowSRID)) {
      // Fast path: The CRS of the query window has the same EPSG code as the raster, so we don't
      // need to decode the CRS of the query window and transform it.
      return Pair.of(rasterGeometry, queryWindow);
    }

    // Prefer transforming the (usually smaller) geometry into the raster's native CRS so the
    // raster footprint stays an exact straight-edged quadrilateral. Matches SedonaDB and avoids
    // the corner-chord under-coverage of reprojecting the raster footprint to WGS84 (#3155).
    CoordinateReferenceSystem queryWindowCRS = FunctionsGeoTools.sridToCRS(queryWindowSRID);
    try {
      Geometry transformedQueryWindow =
          transformGeometry(queryWindow, queryWindowCRS, rasterCRS, false);
      return Pair.of(rasterGeometry, transformedQueryWindow);
    } catch (RuntimeException preferredFailed) {
      // Fallback: transform both sides to WGS84. Needed when the preferred direction fails
      // (e.g. some antimeridian / polar degeneracies) and preserves historical join behavior.
      Geometry transformedQueryWindow = transformGeometryToWGS84(queryWindow, queryWindowCRS);
      Geometry transformedRasterGeometry = transformGeometryToWGS84(rasterGeometry, rasterCRS);
      return Pair.of(transformedRasterGeometry, transformedQueryWindow);
    }
  }

  private static Pair<Geometry, Geometry> convertCRSIfNeeded(
      GridCoverage2D left, GridCoverage2D right) {
    Geometry leftGeometry;
    Geometry rightGeometry;
    try {
      leftGeometry = GeometryFunctions.convexHull(left);
      rightGeometry = GeometryFunctions.convexHull(right);
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException("Failed to calculate the convex hull of the raster", e);
    }

    CoordinateReferenceSystem leftCRS = left.getCoordinateReferenceSystem();
    if (leftCRS == null || leftCRS instanceof DefaultEngineeringCRS) {
      leftCRS = DefaultGeographicCRS.WGS84;
    }
    CoordinateReferenceSystem rightCRS = right.getCoordinateReferenceSystem();
    if (rightCRS == null || rightCRS instanceof DefaultEngineeringCRS) {
      rightCRS = DefaultGeographicCRS.WGS84;
    }

    if (leftCRS == rightCRS || CRS.equalsIgnoreMetadata(leftCRS, rightCRS)) {
      return Pair.of(leftGeometry, rightGeometry);
    }

    // Raster-raster: keep comparing in WGS84. Transforming one large footprint into the other's
    // projected CRS silently mishandles antimeridian-crossing cases (see
    // testRasterRasterPredicates*), whereas the geometry→raster path benefits from an exact
    // raster footprint (#3155).
    Geometry transformedLeftGeometry = transformGeometryToWGS84(leftGeometry, leftCRS);
    Geometry transformedRightGeometry = transformGeometryToWGS84(rightGeometry, rightCRS);
    return Pair.of(transformedLeftGeometry, transformedRightGeometry);
  }

  /**
   * Tests intersection without CRS conversion.
   *
   * @param raster raster defining the coordinate space
   * @param queryWindow geometry already transformed into the raster's coordinate space
   */
  static boolean intersectsInRasterCoordinateSpace(GridCoverage2D raster, Geometry queryWindow) {
    try {
      return GeometryFunctions.convexHull(raster).intersects(queryWindow);
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException("Failed to calculate the convex hull of the raster", e);
    }
  }

  /**
   * Test if crs matches the EPSG code. This method tries to avoid the expensive CRS.decode and
   * CRS.equalsIgnoreMetadata calls. If the crs has an identifier matching the EPSG code, we assume
   * that the crs matches the EPSG code.
   *
   * @param crs The crs to test
   * @param srid The SRID to test. The axis-order of the decoded CRS is assumed to be in lon/lat
   *     order
   * @return true if the crs matches the EPSG code, false otherwise
   */
  public static boolean isCRSMatchesSRID(CoordinateReferenceSystem crs, int srid) {
    CRS.AxisOrder axisOrder = CRS.getAxisOrder(crs);
    if (axisOrder == CRS.AxisOrder.NORTH_EAST) {
      // SRID of geometries will always be decoded as CRS in lon/lat axis order. For projected CRS,
      // the
      // axis order should be east/north. If the crs is for Antarctic or Arctic, the axis order may
      // be
      // INAPPLICABLE. In this case, we'll assume that the axis order would match with the query
      // window if
      // they have the same EPSG code.
      return false;
    }

    Set<ReferenceIdentifier> crsIds = crs.getIdentifiers();
    String strSrid = String.valueOf(srid);
    for (ReferenceIdentifier crsId : crsIds) {
      if ("EPSG".equals(crsId.getCodeSpace()) && strSrid.equals(crsId.getCode())) {
        return true;
      }
    }
    return false;
  }

  private static Geometry transformGeometryToWGS84(
      Geometry geometry, CoordinateReferenceSystem crs) {
    return transformGeometry(geometry, crs, DefaultGeographicCRS.WGS84, true);
  }

  /**
   * Transform {@code geometry} from {@code sourceCRS} into {@code targetCRS}. When {@code
   * antiMeridianSafe} is true and the source is projected, apply antimeridian-safe wrapping after
   * transforming into a geographic CRS (historical WGS84 path behavior).
   */
  private static Geometry transformGeometry(
      Geometry geometry,
      CoordinateReferenceSystem sourceCRS,
      CoordinateReferenceSystem targetCRS,
      boolean antiMeridianSafe) {
    if (sourceCRS == targetCRS || CRS.equalsIgnoreMetadata(sourceCRS, targetCRS)) {
      return geometry;
    }
    try {
      MathTransform transform = CachedCRSTransformFinder.findTransform(sourceCRS, targetCRS);
      Geometry transformedGeometry = JTS.transform(geometry, transform);
      if (antiMeridianSafe
          && !(sourceCRS instanceof GeographicCRS)
          && targetCRS instanceof GeographicCRS) {
        transformedGeometry = GeomUtils.antiMeridianSafeGeom(transformedGeometry);
      }
      return transformedGeometry;
    } catch (TransformException e) {
      throw new RuntimeException("Cannot transform CRS for evaluating predicate", e);
    }
  }
}
