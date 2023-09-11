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

import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.raster.inputstream.ByteArrayImageInputStream;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.gce.arcgrid.ArcGridReader;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.vector.VectorToRasterProcess;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;

import javax.media.jai.RasterFactory;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Arrays;

public class RasterConstructors
{
    public static GridCoverage2D fromArcInfoAsciiGrid(byte[] bytes) throws IOException {
        ArcGridReader reader = new ArcGridReader(new ByteArrayImageInputStream(bytes), new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
        return reader.read(null);
    }

    public static GridCoverage2D fromGeoTiff(byte[] bytes) throws IOException {
        GeoTiffReader geoTiffReader = new GeoTiffReader(new ByteArrayImageInputStream(bytes), new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
        return geoTiffReader.read(null);
    }

    /**
    * Returns a raster that is converted from the geometry provided.
     * @param geom The geometry to convert
     * @param raster The reference raster
     * @param pixelType The data type of pixel/cell of resultant raster
     * @param value The value of the pixel of the resultant raster
     * @param noDataValue The noDataValue of the resultant raster
     *
     * @return Rasterized Geometry
     * @throws FactoryException
    */
    public static GridCoverage2D asRaster(Geometry geom, GridCoverage2D raster, String pixelType, double value, Double noDataValue) throws FactoryException {

        DefaultFeatureCollection featureCollection = getFeatureCollection(geom, raster.getCoordinateReferenceSystem());

        double[] metadata = RasterAccessors.metadata(raster);
        // The current implementation doesn't support rasters with properties below
        // It is not a problem as most rasters don't have these properties
        // ScaleX < 0
        if (metadata[4] < 0) {
            throw new IllegalArgumentException(String.format("ScaleX %f of the raster is negative, it should be positive", metadata[4]));
        }
        // ScaleY > 0
        if (metadata[5] > 0) {
            throw new IllegalArgumentException(String.format("ScaleY %f of the raster is positive. It should be negative.", metadata[5]));
        }
        // SkewX should be zero
        if (metadata[6] != 0) {
            throw new IllegalArgumentException(String.format("SkewX %d of the raster is not zero.", metadata[6]));
        }
        // SkewY should be zero
        if (metadata[7] != 0) {
            throw new IllegalArgumentException(String.format("SkewY %d of the raster is not zero.", metadata[7]));
        }

        Envelope2D bound = JTS.getEnvelope2D(geom.getEnvelopeInternal(), raster.getCoordinateReferenceSystem2D());

        double scaleX = Math.abs(metadata[4]), scaleY = Math.abs(metadata[5]);
        int width = (int) bound.getWidth(), height = (int) bound.getHeight();
        if (width == 0 && height == 0) {
            bound = new Envelope2D(bound.getCoordinateReferenceSystem(), bound.getCenterX() - scaleX * 0.5, bound.getCenterY() - scaleY * 0.5, scaleX, scaleY);
            width = 1;
            height = 1;
        } else if (height == 0) {
            bound = new Envelope2D(bound.getCoordinateReferenceSystem(), bound.getCenterX() - scaleX * 0.5, bound.getCenterY() - scaleY * 0.5, width, scaleY);
            height = 1;
        } else if (width == 0) {
            bound = new Envelope2D(bound.getCoordinateReferenceSystem(), bound.getCenterX() - scaleX * 0.5, bound.getCenterY() - scaleY * 0.5, scaleX, height);
            width = 1;
        } else {
            // To preserve scale of reference raster
            width = (int) (width / scaleX);
            height = (int) (height / scaleY);
        }

        VectorToRasterProcess rasterProcess = new VectorToRasterProcess();
        GridCoverage2D rasterized = rasterProcess.execute(featureCollection, width, height, "value", Double.toString(value), bound, null);
        if (noDataValue != null) {
            rasterized = RasterBandEditors.setBandNoDataValue(rasterized, 1, noDataValue);
        }
        WritableRaster writableRaster = RasterFactory.createBandedRaster(RasterUtils.getDataTypeCode(pixelType), width, height, 1, null);
        double [] samples = RasterUtils.getRaster(rasterized.getRenderedImage()).getSamples(0, 0, width, height, 0, (double[]) null);
        writableRaster.setSamples(0, 0, width, height, 0, samples);

        return RasterUtils.create(writableRaster, rasterized.getGridGeometry(), rasterized.getSampleDimensions(), noDataValue);
    }

    /**
     * Returns a raster that is converted from the geometry provided. A convenience function for asRaster.
     *
     * @param geom The geometry to convert
     * @param raster The reference raster
     * @param pixelType The data type of pixel/cell of resultant raster.
     *
     * @return Rasterized Geometry
     * @throws FactoryException
     */
    public static GridCoverage2D asRaster(Geometry geom, GridCoverage2D raster, String pixelType) throws FactoryException {
        return asRaster(geom, raster, pixelType, 1, null);
    }

    /**
     * Returns a raster that is converted from the geometry provided. A convenience function for asRaster.
     *
     * @param geom The geometry to convert
     * @param raster The reference raster
     * @param pixelType The data type of pixel/cell of resultant raster.
     * @param value The value of the pixel of the resultant raster
     *
     * @return Rasterized Geometry
     * @throws FactoryException
     */
    public static GridCoverage2D asRaster(Geometry geom, GridCoverage2D raster, String pixelType, double value) throws FactoryException {
        return asRaster(geom, raster, pixelType, value, null);
    }

    public static DefaultFeatureCollection getFeatureCollection(Geometry geom, CoordinateReferenceSystem crs) {
        SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
        simpleFeatureTypeBuilder.setName("Raster");
        simpleFeatureTypeBuilder.setCRS(crs);
        simpleFeatureTypeBuilder.add("geometry", Geometry.class);

        SimpleFeatureType featureType = simpleFeatureTypeBuilder.buildFeatureType();
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        featureBuilder.add(geom);
        SimpleFeature simpleFeature = featureBuilder.buildFeature("1");
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
        featureCollection.add(simpleFeature);

        return featureCollection;
    }

    /**
     * Convenience function setting DOUBLE as datatype for the bands
     * Create a new empty raster with the given number of empty bands.
     * The bounding envelope is defined by the upper left corner and the scale.
     * The math formula of the envelope is: minX = upperLeftX = lowerLeftX, minY (lowerLeftY) = upperLeftY - height * pixelSize
     * <ul>
     *   <li>The raster is defined by the width and height
     *   <li>The upper left corner is defined by the upperLeftX and upperLeftY
     *   <li>The scale is defined by pixelSize. The scaleX is equal to pixelSize and scaleY is equal to -pixelSize
     *   <li>skewX and skewY are zero, which means no shear or rotation.
     *   <li>SRID is default to 0 which means the default CRS (Generic 2D)
     * </ul>
     * @param numBand the number of bands
     * @param widthInPixel the width of the raster, in pixel
     * @param heightInPixel the height of the raster, in pixel
     * @param upperLeftX the upper left corner of the raster. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster. Note that: the minY of the envelope is equal to the upperLeftY - height * pixelSize
     * @param pixelSize the size of the pixel in the unit of the CRS
     * @return the new empty raster
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double pixelSize)
            throws FactoryException
    {
        return makeEmptyRaster(numBand, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize, 0, 0, 0);
    }

    /**
     * Convenience function allowing explicitly setting the datatype for all the bands
     * @param numBand
     * @param dataType
     * @param widthInPixel
     * @param heightInPixel
     * @param upperLeftX
     * @param upperLeftY
     * @param pixelSize
     * @return
     * @throws FactoryException
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, String dataType, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double pixelSize)
            throws FactoryException
    {
        return makeEmptyRaster(numBand, dataType, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize, 0, 0, 0);
    }


    /**
     * Convenience function for creating a raster with data type DOUBLE for all the bands
     * @param numBand
     * @param widthInPixel
     * @param heightInPixel
     * @param upperLeftX
     * @param upperLeftY
     * @param scaleX
     * @param scaleY
     * @param skewX
     * @param skewY
     * @param srid
     * @return
     * @throws FactoryException
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double scaleX, double scaleY, double skewX, double skewY, int srid)
            throws FactoryException
    {
        return makeEmptyRaster(numBand, "d", widthInPixel, heightInPixel, upperLeftX, upperLeftY, scaleX, scaleY, skewX, skewY, srid);
    }

    /**
     * Create a new empty raster with the given number of empty bands
     * @param numBand the number of bands
     * @param bandDataType the data type of the raster, one of D | B | I | F | S | US
     * @param widthInPixel the width of the raster, in pixel
     * @param heightInPixel the height of the raster, in pixel
     * @param upperLeftX the upper left corner of the raster, in the CRS unit. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster, in the CRS unit. Note that: the minY of the envelope is equal to the upperLeftY + height * scaleY
     * @param scaleX the scale of the raster (pixel size on X), in the CRS unit
     * @param scaleY the scale of the raster (pixel size on Y), in the CRS unit
     * @param skewX the skew of the raster on X, in the CRS unit
     * @param skewY the skew of the raster on Y, in the CRS unit
     * @param srid the srid of the CRS. 0 means the default CRS (Cartesian 2D)
     * @return the new empty raster
     * @throws FactoryException
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, String bandDataType, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double scaleX, double scaleY, double skewX, double skewY, int srid)
            throws FactoryException
    {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        } else {
            // Create the CRS from the srid
            // Longitude first, Latitude second
            crs = FunctionsGeoTools.sridToCRS(srid);
        }

        // Create a new empty raster
        WritableRaster raster = RasterFactory.createBandedRaster(RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBand, null);
        MathTransform transform = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        GridGeometry2D gridGeometry = new GridGeometry2D(
                new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
                PixelInCell.CELL_CORNER,
                transform, crs, null);
        return RasterUtils.create(raster, gridGeometry, null);
    }

    public static GridCoverage2D makeNonEmptyRaster(int numBands, String bandDataType, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double scaleX, double scaleY, double skewX, double skewY, int srid, double[] rasterValues) {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        } else {
            // Create the CRS from the srid
            // Longitude first, Latitude second
            crs = FunctionsGeoTools.sridToCRS(srid);
        }

        // Create a new empty raster
        WritableRaster raster = RasterFactory.createBandedRaster(RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBands, null);
        raster.setPixels(0, 0, widthInPixel, heightInPixel, rasterValues);
        MathTransform transform = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        GridGeometry2D gridGeometry = new GridGeometry2D(
                new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
                PixelInCell.CELL_CORNER,
                transform, crs, null);
        return RasterUtils.create(raster, gridGeometry, null);
    }
}
