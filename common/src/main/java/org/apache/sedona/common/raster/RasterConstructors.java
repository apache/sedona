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

import org.apache.sedona.common.raster.inputstream.ByteArrayImageInputStream;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.gce.arcgrid.ArcGridReader;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import javax.media.jai.RasterFactory;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.io.IOException;

public class RasterConstructors
{
    public static GridCoverage2D fromArcInfoAsciiGrid(byte[] bytes) throws IOException {
        ArcGridReader reader = new ArcGridReader(new ByteArrayImageInputStream(bytes));
        return reader.read(null);
    }

    public static GridCoverage2D fromGeoTiff(byte[] bytes) throws IOException {
        GeoTiffReader geoTiffReader = new GeoTiffReader(new ByteArrayImageInputStream(bytes));
        return geoTiffReader.read(null);
    }

    /**
     * Create a new empty raster with the given number of empty bands
     * The bounding envelope is defined by the upper left corner and the scale
     * The math formula of the envelope is: minX = upperLeftX = lowerLeftX, minY (lowerLeftY) = upperLeftY - height * pixelSize
     * The raster is defined by the width and height
     * The affine transform is defined by the skewX and skewY
     * The upper left corner is defined by the upperLeftX and upperLeftY
     * The scale is defined by the scaleX and scaleY
     * SRID is default to 0 which means the default CRS (Cartesian 2D)
     * @param numBand the number of bands
     * @param widthInPixel
     * @param heightInPixel
     * @param upperLeftX the upper left corner of the raster. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster. Note that: the minY of the envelope is equal to the upperLeftY - height * scaleY
     * @param pixelSize the size of the pixel in the unit of the CRS
     * @return
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double pixelSize)
            throws FactoryException
    {
        return makeEmptyRaster(numBand, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, pixelSize, 0, 0, 0);
    }

    /**
     * Create a new empty raster with the given number of empty bands
     * @param numBand the number of bands
     * @param widthInPixel the width of the raster, in pixel
     * @param heightInPixel the height of the raster, in pixel
     * @param upperLeftX the upper left corner of the raster, in the CRS unit. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster, in the CRS unit. Note that: the minY of the envelope is equal to the upperLeftY - height * scaleY
     * @param scaleX the scale of the raster (pixel size on X), in the CRS unit
     * @param scaleY the scale of the raster (pixel size on Y), in the CRS unit
     * @param skewX the skew of the raster on X, in the CRS unit
     * @param skewY the skew of the raster on Y, in the CRS unit
     * @param srid the srid of the CRS. 0 means the default CRS (Cartesian 2D)
     * @return
     * @throws FactoryException
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double scaleX, double scaleY, double skewX, double skewY, int srid)
            throws FactoryException
    {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        } else {
            crs = CRS.decode("EPSG:" + srid);
        }
        // If scaleY is not defined, use scaleX
        // MAX_VALUE is used to indicate that the scaleY is not defined
        double actualScaleY = scaleY;
        if (scaleY == Integer.MAX_VALUE) {
            actualScaleY = scaleX;
        }
        // Create a new empty raster
        WritableRaster raster = RasterFactory.createBandedRaster(DataBuffer.TYPE_DOUBLE, widthInPixel, heightInPixel, numBand, null);
        MathTransform transform = new AffineTransform2D(scaleX, skewY, skewX, -actualScaleY, upperLeftX + scaleX / 2, upperLeftY - actualScaleY / 2);
        GridGeometry2D gridGeometry = new GridGeometry2D(new GridEnvelope2D(0, 0, widthInPixel, heightInPixel), transform, crs);
        ReferencedEnvelope referencedEnvelope = new ReferencedEnvelope(gridGeometry.getEnvelope2D());
        // Create a new coverage
        GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
        return gridCoverageFactory.create("genericCoverage", raster, referencedEnvelope);
    }
}
