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
import org.apache.sedona.common.utils.GeomUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class RasterPredicates {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    /**
     * Test if a raster intersects a query window. If both the raster and the query window have a
     * CRS, the query window and the envelope of the raster will be transformed to a common CRS
     * before testing for intersection.
     * Please note that the CRS transformation will be lenient, which means that the transformation
     * may not be accurate.
     * @param raster the raster
     * @param queryWindow the query window
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

    private static Pair<Geometry, Geometry> convertCRSIfNeeded(GridCoverage2D raster, Geometry queryWindow) {
        Geometry rasterGeometry;
        try {
            rasterGeometry = GeometryFunctions.convexHull(raster);
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException("Failed to calculate the convex hull of the raster", e);
        }
        CoordinateReferenceSystem rasterCRS = raster.getCoordinateReferenceSystem();
        int queryWindowSRID = queryWindow.getSRID();
        if (rasterCRS == null || rasterCRS instanceof DefaultEngineeringCRS || queryWindowSRID <= 0) {
            // Either raster or query window does not have a defined CRS, simply use the original
            // raster envelope and the query window to test for relationship.
            return Pair.of(rasterGeometry, queryWindow);
        }

        // Both raster and query window have a defined CRS
        String queryWindowCRSCode = "EPSG:" + queryWindowSRID;
        if (isCRSMatchesEPSGCode(rasterCRS, queryWindowCRSCode)) {
            // The CRS of the query window has the same EPSG code as the raster, so we don't need to
            // transform it.
            // Please note that even though the EPSG code is the same, the CRS may not be the same.
            // The query window and the raster may not have the same axis order. It is user's
            // responsibility to provide a query window with the same axis order as the raster.
            return Pair.of(rasterGeometry, queryWindow);
        }

        // Raster has a non-authoritative CRS, or the CRS of the raster is different from the
        // CRS of the query window. We'll transform both sides to a common CRS (WGS84) before
        // testing for relationship.
        try {
            CoordinateReferenceSystem queryWindowCRS = CRS.decode(queryWindowCRSCode, true);
            MathTransform transform = CRS.findMathTransform(queryWindowCRS,
                DefaultGeographicCRS.WGS84, true);
            queryWindow = JTS.transform(queryWindow, transform);
            if (queryWindowSRID != 4326) {
                queryWindow = GeomUtils.antiMeridianSafeGeom(queryWindow);
            } else {
                // The query window is already in WGS84, which is a geographic CRS. We'll assume that
                // the query window provided by the user is already anti-meridian safe.
                // If the query window has a width greater than 180, the antiMeridianSafeGeom method
                // will treat it as crossing the anti-meridian, which may not be what the user wants.
            }

            // Transform the raster envelope. Here we don't use the envelope transformation method
            // provided by GeoTools since it performs poorly when the raster envelope crosses the
            // anti-meridian.
            transform = CRS.findMathTransform(rasterCRS, DefaultGeographicCRS.WGS84, true);
            rasterGeometry = JTS.transform(rasterGeometry, transform);
            rasterGeometry = GeomUtils.antiMeridianSafeGeom(rasterGeometry);
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException("Cannot transform CRS of query window", e);
        }

        return Pair.of(rasterGeometry, queryWindow);
    }

    private static boolean isCRSMatchesEPSGCode(CoordinateReferenceSystem crs, String epsgCode) {
        CRS.AxisOrder axisOrder = CRS.getAxisOrder(crs);
        if (axisOrder == CRS.AxisOrder.NORTH_EAST) {
            // SRID of geometries will always be decoded as CRS in lon/lat axis order. For projected CRS, the
            // axis order should be east/north. If the crs is for Antarctic or Arctic, the axis order may be
            // INAPPLICABLE. In this case, we'll assume that the axis order would match with the query window if
            // they have the same EPSG code.
            return false;
        }

        Set<ReferenceIdentifier> crsIds = crs.getIdentifiers();
        if (crsIds.isEmpty()) {
            return false;
        }
        ReferenceIdentifier crsId = crsIds.iterator().next();
        String code = crsId.getCodeSpace() + ":" + crsId.getCode();
        return code.equals(epsgCode);
    }
}
