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

import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.utils.CachedCRSTransformFinder;
import org.apache.sedona.common.utils.GeomUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.GeographicCRS;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class RasterPredicates {
  /**
   * Test if a raster intersects a query window. If both the raster and the query window have a CRS,
   * the query window and the envelope of the raster will be transformed to a common CRS before
   * testing for intersection. Please note that the CRS transformation will be lenient, which means
   * that the transformation may not be accurate.
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

    // Raster has a non-authoritative CRS, or the CRS of the raster is different from the
    // CRS of the query window. We'll transform both sides to a common CRS (WGS84) before
    // testing for relationship.
    CoordinateReferenceSystem queryWindowCRS;
    queryWindowCRS = FunctionsGeoTools.sridToCRS(queryWindowSRID);
    Geometry transformedQueryWindow = transformGeometryToWGS84(queryWindow, queryWindowCRS);

    // Transform the raster envelope. Here we don't use the envelope transformation method
    // provided by GeoTools since it performs poorly when the raster envelope crosses the
    // anti-meridian.
    Geometry transformedRasterGeometry = transformGeometryToWGS84(rasterGeometry, rasterCRS);
    return Pair.of(transformedRasterGeometry, transformedQueryWindow);
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

    // Transform both sides to WGS84, and then return transformed geometries for evaluating
    // predicates.
    Geometry transformedLeftGeometry = transformGeometryToWGS84(leftGeometry, leftCRS);
    Geometry transformedRightGeometry = transformGeometryToWGS84(rightGeometry, rightCRS);
    return Pair.of(transformedLeftGeometry, transformedRightGeometry);
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
    if (crs == DefaultGeographicCRS.WGS84) {
      return geometry;
    }
    try {
      MathTransform transform =
          CachedCRSTransformFinder.findTransform(crs, DefaultGeographicCRS.WGS84);
      Geometry transformedGeometry = JTS.transform(geometry, transform);
      if (!(crs instanceof GeographicCRS)) {
        transformedGeometry = GeomUtils.antiMeridianSafeGeom(transformedGeometry);
      }
      return transformedGeometry;
    } catch (TransformException e) {
      throw new RuntimeException("Cannot transform CRS for evaluating predicate", e);
    }
  }
}
