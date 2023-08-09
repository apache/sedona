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

import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class RasterPredicates {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    /**
     * Test if a raster intersects a query window. If both the raster and the query window have a
     * CRS, the query window will be transformed to the CRS of the raster before testing for intersection.
     * Please note that the CRS transformation will be lenient, which means that the transformation may
     * not be accurate.
     * @param raster the raster
     * @param queryWindow the query window
     * @return true if the raster intersects the query window
     */
    public static boolean rsIntersects(GridCoverage2D raster, Geometry queryWindow) {
        Envelope2D rasterEnvelope2D = raster.getEnvelope2D();
        CoordinateReferenceSystem rasterCRS = rasterEnvelope2D.getCoordinateReferenceSystem();
        queryWindow = RasterUtils.convertCRSIfNeeded(queryWindow, rasterCRS);
        Envelope rasterEnvelope = JTS.toEnvelope(rasterEnvelope2D);
        Geometry rasterGeometry = GEOMETRY_FACTORY.toGeometry(rasterEnvelope);
        return rasterGeometry.intersects(queryWindow);
    }

    public static boolean rsContains(GridCoverage2D raster, Geometry geometry) {
        Envelope2D rasterEnvelope2D = raster.getEnvelope2D();
        CoordinateReferenceSystem rasterCRS = rasterEnvelope2D.getCoordinateReferenceSystem();
        geometry = RasterUtils.convertCRSIfNeeded(geometry, rasterCRS);
        Envelope rasterEnvelope = JTS.toEnvelope(rasterEnvelope2D);
        Geometry rasterGeometry = GEOMETRY_FACTORY.toGeometry(rasterEnvelope);
        return  rasterGeometry.contains(geometry);
    }

    public static boolean rsWithin(GridCoverage2D raster, Geometry geometry) {
        Envelope2D rasterEnvelope2D = raster.getEnvelope2D();
        CoordinateReferenceSystem rasterCRS = rasterEnvelope2D.getCoordinateReferenceSystem();
        geometry = RasterUtils.convertCRSIfNeeded(geometry, rasterCRS);
        Envelope rasterEnvelope = JTS.toEnvelope(rasterEnvelope2D);
        Geometry rasterGeometry = GEOMETRY_FACTORY.toGeometry(rasterEnvelope);
        return  rasterGeometry.within(geometry);
    }
}
