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

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.operation.Affine;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.awt.image.RenderedImage;
import java.util.Optional;

public class RasterAccessors
{
    public static int srid(GridCoverage2D raster) throws FactoryException
    {
        CoordinateReferenceSystem crs = raster.getCoordinateReferenceSystem();
        if (crs instanceof DefaultEngineeringCRS) {
            // GeoTools defaults to internal non-standard epsg codes, like 404000, if crs is missing.
            // We need to check for this case and return 0 instead.
            if (((DefaultEngineeringCRS) crs).isWildcard()) {
                return 0;
            }
        }
        return Optional.ofNullable(CRS.lookupEpsgCode(crs, true)).orElse(0);
    }

    public static int numBands(GridCoverage2D raster) {
        return raster.getNumSampleDimensions();
    }

    public static int getWidth(GridCoverage2D raster) {
        return raster.getGridGeometry().getGridRange().getSpan(0);
    }

    public static int getHeight(GridCoverage2D raster) {
        return raster.getGridGeometry().getGridRange().getSpan(1);
    }


    public static double getScaleX(GridCoverage2D raster) {
        return getAffineTransform(raster).getScaleX();
    }

    public static double getScaleY(GridCoverage2D raster) {
        return getAffineTransform(raster).getScaleY();
    }

    private static AffineTransform2D getAffineTransform(GridCoverage2D raster) throws UnsupportedOperationException {
        GridGeometry2D gridGeometry2D = raster.getGridGeometry();
        MathTransform crsTransform = gridGeometry2D.getGridToCRS2D();
        if (!(crsTransform instanceof AffineTransform2D)) {
            throw new UnsupportedOperationException("Only AffineTransform2D is supported");
        }
        return (AffineTransform2D) crsTransform;
    }


    public static Geometry envelope(GridCoverage2D raster) throws FactoryException {
        Envelope2D envelope2D = raster.getEnvelope2D();

        Envelope envelope = new Envelope(envelope2D.getMinX(), envelope2D.getMaxX(), envelope2D.getMinY(), envelope2D.getMaxY());
        int srid = srid(raster);
        return new GeometryFactory(new PrecisionModel(), srid).toGeometry(envelope);
    }

    /**
     * Returns the metadata of a raster as an array of doubles.
     * @param raster
     * @return double[] with the following values:
     * 0: minX: upper left x
     * 1: maxY: upper left y
     * 2: width: number of pixels on x axis
     * 3: height: number of pixels on y axis
     * 4: scaleX: pixel width
     * 5: scaleY: pixel height
     * 6: shearX: skew on x axis
     * 7: shearY: skew on y axis
     * 8: srid
     * 9: numBands
     * @throws FactoryException
     */
    public static double[] metadata(GridCoverage2D raster)
            throws FactoryException
    {
        RenderedImage image = raster.getRenderedImage();
        // Georeference metadata
        Envelope2D envelope2D = raster.getEnvelope2D();
        MathTransform gridToCRS = raster.getGridGeometry().getGridToCRS2D();
        if (gridToCRS instanceof AffineTransform2D) {
            AffineTransform2D affine = (AffineTransform2D) gridToCRS;

            // Get the affine parameters
            double scaleX = affine.getScaleX();
            double scaleY = affine.getScaleY();
            double shearX = affine.getShearX();
            double shearY = affine.getShearY();
            return new double[] {envelope2D.getMinX(), envelope2D.getMaxY(), image.getWidth(), image.getHeight(), scaleX, scaleY, shearX, shearY, srid(raster), raster.getNumSampleDimensions()};
        }
        else {
            // Handle the case where gridToCRS is not an AffineTransform2D
            throw new UnsupportedOperationException("Only AffineTransform2D is supported");
        }
    }
}
