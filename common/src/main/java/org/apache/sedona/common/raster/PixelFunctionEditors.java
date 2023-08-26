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

import javax.media.jai.RasterFactory;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;

public class PixelFunctionEditors {

    public static GridCoverage2D setValues(GridCoverage2D raster, int band, int colX, int rowY, int width, int height, double[] values, boolean keepNoData) {
        RasterUtils.ensureBand(raster, band);
        if (values.length != width * height) {
            throw new IllegalArgumentException("Shape of 'values' doesn't match provided width and height.");
        }

        RenderedImage originalImage = raster.getRenderedImage();
        Raster rasterTemp = originalImage.getData();
        Point location = raster.getRenderedImage().getData().getBounds().getLocation();
        WritableRaster wr = RasterFactory.createBandedRaster(rasterTemp.getDataBuffer().getDataType(), originalImage.getWidth(), originalImage.getHeight(), raster.getNumSampleDimensions(), location);

        WritableRaster rasterCopied = raster.getRenderedImage().copyData(wr);

        Double noDataValue = null;
        if (keepNoData) {
            noDataValue = RasterBandAccessors.getBandNoDataValue(raster, band);
        }
        int iterator = 0;
        for (int j = rowY; j < rowY + height; j++) {
            for (int i = colX; i < colX + width; i++) {
                double[] pixel = rasterCopied.getPixel(i, j, (double[]) null);
                if (keepNoData && noDataValue != pixel[band - 1]) {
                    pixel[band - 1] = values[iterator];
                } else if (!keepNoData) {
                    pixel[band - 1] = values[iterator];
                }
                rasterCopied.setPixel(i, j, pixel);
                iterator++;
            }
        }
        return RasterUtils.create(wr, raster.getGridGeometry(), raster.getSampleDimensions());
    }

    public static GridCoverage2D setValues(GridCoverage2D raster, int band, int colX, int rowY, int width, int height, double[] values) {
        return setValues(raster, band, colX, rowY, width, height, values, false);
    }
}
