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
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform2D;

import java.util.Map;

public class RasterEditors
{
    public static GridCoverage2D setSrid(GridCoverage2D raster, int srid) throws FactoryException
    {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        } else {
            crs = CRS.decode("EPSG:" + srid, true);
        }

        GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
        MathTransform2D transform = raster.getGridGeometry().getGridToCRS2D();
        Map<?, ?> properties = raster.getProperties();
        GridCoverage[] sources = raster.getSources().toArray(new GridCoverage[0]);
        return gridCoverageFactory.create(raster.getName().toString(), raster.getRenderedImage(), crs, transform, raster.getSampleDimensions(), sources, properties);
    }

    public static GridCoverage2D setGeoReference(GridCoverage2D raster, String geoRefCoords, String format) {
        String[] coords = geoRefCoords.split(" ");
        if (coords.length != 6) {
            return null;
        }

        double scaleX = Double.parseDouble(coords[0]);
        double skewY = Double.parseDouble(coords[1]);
        double skewX = Double.parseDouble(coords[2]);
        double scaleY = Double.parseDouble(coords[3]);
        double upperLeftX = Double.parseDouble(coords[4]);
        double upperLeftY = Double.parseDouble(coords[5]);
        AffineTransform2D affine;


        if (format.equalsIgnoreCase("GDAL")) {
            affine = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        } else if (format.equalsIgnoreCase("ESRI")) {
            upperLeftX = upperLeftX - (scaleX * 0.5);
            upperLeftY = upperLeftY - (scaleY * 0.5);
            affine = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        } else {
            throw new IllegalArgumentException("Please select between the following formats GDAL and ESRI");
        }
        int height = RasterAccessors.getHeight(raster), width = RasterAccessors.getWidth(raster);

        GridGeometry2D gridGeometry2D = new GridGeometry2D(
                new GridEnvelope2D(0, 0, width, height),
                PixelOrientation.UPPER_LEFT,
                affine, raster.getCoordinateReferenceSystem(), null
        );

        return RasterUtils.create(raster.getRenderedImage(), gridGeometry2D, raster.getSampleDimensions(),
                null);
    }

    public static GridCoverage2D setGeoReference(GridCoverage2D raster, String geoRefCoords) {
        return setGeoReference(raster, geoRefCoords, "GDAL");
    }

    public static GridCoverage2D setGeoReference(GridCoverage2D raster, double upperLeftX, double upperLeftY,
                                                 double scaleX, double scaleY, double skewX, double skewY) {
        String geoRedCoord = String.format("%f %f %f %f %f %f", scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        return setGeoReference(raster, geoRedCoord, "GDAL");
    }
}
