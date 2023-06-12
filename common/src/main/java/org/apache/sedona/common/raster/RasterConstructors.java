/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.raster;

import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.gce.arcgrid.ArcGridReader;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultEngineeringCRS;

import javax.media.jai.RasterFactory;

import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class RasterConstructors
{

    public static GridCoverage2D fromArcInfoAsciiGrid(byte[] bytes) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            ArcGridReader reader = new ArcGridReader(inputStream);
            return reader.read(null);
        }
    }

    public static GridCoverage2D fromGeoTiff(byte[] bytes) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            GeoTiffReader geoTiffReader = new GeoTiffReader(inputStream);
            return geoTiffReader.read(null);
        }
    }

    /**
     * Create a new empty raster with the given number of empty bands
     * The bounding envelope is defined by the upper left corner and the scale
     * The math formula of the envelope is: minX = upperLeftX = lowerLeftX (minX), minY (lowerLeftY) = upperLeftY - height * pixelSizeInDegrees
     * The raster is defined by the width and height
     * The affine transform is defined by the skewX and skewY
     * The upper left corner is defined by the upperLeftX and upperLeftY
     * The scale is defined by the scaleX and scaleY
     * @param widthInPixel
     * @param heightInPixel
     * @param upperLeftX the upper left corner of the raster. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster. Note that: the minY of the envelope is equal to the upperLeftY - height * scaleY
     * @param pixelSizeInDegrees the size of the pixel in degrees
     * @param numBand the number of bands
     * @return
     */
    public static GridCoverage2D makeEmptyRaster(int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double pixelSizeInDegrees, int numBand)
    {
        // Create a new empty raster
        WritableRaster raster = RasterFactory.createBandedRaster(DataBuffer.TYPE_DOUBLE, widthInPixel, heightInPixel, numBand, null);

        ReferencedEnvelope referencedEnvelope = new ReferencedEnvelope(upperLeftX, upperLeftX + widthInPixel * pixelSizeInDegrees, upperLeftY - heightInPixel * pixelSizeInDegrees, upperLeftY, DefaultEngineeringCRS.GENERIC_2D);
        // Create a new coverage
        GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
        return gridCoverageFactory.create("genericCoverage", raster, referencedEnvelope);
    }

    /**
     * Create a new empty raster with 1 empty band
     * The bounding envelope is defined by the upper left corner and the scale
     * The math formula of the envelope is: minX = upperLeftX = lowerLeftX (minX), minY (lowerLeftY) = upperLeftY - height * pixelSizeInDegrees
     * The raster is defined by the width and height
     * The affine transform is defined by the skewX and skewY
     * The upper left corner is defined by the upperLeftX and upperLeftY
     * The scale is defined by the scaleX and scaleY
     * @param widthInPixel
     * @param heightInPixel
     * @param upperLeftX the upper left corner of the raster. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster. Note that: the minY of the envelope is equal to the upperLeftY - height * scaleY
     * @param pixelSizeInDegrees the size of the pixel in degrees
     * @return
     */
    public static GridCoverage2D makeEmptyRaster(int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double pixelSizeInDegrees)
    {
        return makeEmptyRaster(widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSizeInDegrees, 1);
    }
}
