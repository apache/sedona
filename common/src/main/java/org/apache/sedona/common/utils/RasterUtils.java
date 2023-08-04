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
package org.apache.sedona.common.utils;

import com.sun.media.imageioimpl.common.BogusColorSpace;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Arrays;

/**
 * Utility functions for working with GridCoverage2D objects.
 */
public class RasterUtils {
    private RasterUtils() {}

    private static final GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);

    /**
     * Create a new empty raster from the given WritableRaster object.
     * @param raster The raster object to be wrapped as an image.
     * @param gridGeometry The grid geometry of the raster.
     * @param bands The bands of the raster.
     * @return A new GridCoverage2D object.
     */
    public static GridCoverage2D create(WritableRaster raster, GridGeometry2D gridGeometry, GridSampleDimension[] bands) {
        int numBand = raster.getNumBands();
        int rasterDataType = raster.getDataBuffer().getDataType();

        // Construct a color model for the rendered image. This color model should be able to be serialized and
        // deserialized. The color model object automatically constructed by grid coverage factory may not be
        // serializable, please refer to https://issues.apache.org/jira/browse/SEDONA-319 for more details.
        final ColorSpace cs = new BogusColorSpace(numBand);
        final int[] nBits = new int[numBand];
        Arrays.fill(nBits, DataBuffer.getDataTypeSize(rasterDataType));
        ColorModel colorModel =
                new ComponentColorModel(cs, nBits, false, true, Transparency.OPAQUE, rasterDataType);

        final RenderedImage image = new BufferedImage(colorModel, raster, false, null);
        return gridCoverageFactory.create("genericCoverage", image, gridGeometry, bands, null, null);
    }

    /**
     * Get a GDAL-compliant affine transform from the given raster, where the grid coordinate indicates the upper left
     * corner of the pixel. PostGIS also follows GDAL convention.
     * @param raster The raster to get the affine transform from.
     * @return The affine transform.
     */
    public static AffineTransform2D getGDALAffineTransform(GridCoverage2D raster) {
        return getAffineTransform(raster, PixelOrientation.UPPER_LEFT);
    }

    public static AffineTransform2D getAffineTransform(GridCoverage2D raster, PixelOrientation orientation) throws UnsupportedOperationException {
        GridGeometry2D gridGeometry2D = raster.getGridGeometry();
        MathTransform crsTransform = gridGeometry2D.getGridToCRS2D(orientation);
        if (!(crsTransform instanceof AffineTransform2D)) {
            throw new UnsupportedOperationException("Only AffineTransform2D is supported");
        }
        return (AffineTransform2D) crsTransform;
    }

    public static Point2D getCornerCoordinates(GridCoverage2D raster, int colX, int rowY) throws TransformException {
        return raster.getGridGeometry().getGridToCRS2D(PixelOrientation.UPPER_LEFT).transform(new GridCoordinates2D(colX - 1, rowY - 1), null);
    }
}
