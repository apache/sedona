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
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.raster.RasterAccessors;
import org.geotools.coverage.Category;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.ClassChanger;
import org.geotools.util.NumberRange;
import org.locationtech.jts.geom.Geometry;
import org.opengis.geometry.DirectPosition;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.InternationalString;

import javax.media.jai.RasterFactory;
import javax.media.jai.RenderedImageAdapter;
import java.awt.Color;
import java.awt.Point;
import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.*;

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
        return create(raster, gridGeometry, bands, null);
    }

    /**
     *
     * @param raster WriteableRaster to be used while creating the new raster.
     * @param gridGeometry2D gridGeometry2D to be used while cloning
     * @param bands bands to be used while cloning
     * @param referenceRaster referenceRaster to clone from
     * @param noDataValue noDataValue (if any) to be applied to the bands. If provided null. bands are unchanged.
     * @param keepMetadata if passed true, clone all possible metadata from the referenceRaster.
                keepMetaData controls the presence/absence of the following metadata of the referenceRasterObject:
                    raster name: Name of the raster (GridCoverage2D level)
                    raster properties: A Map of raster and image properties combined.
                    raster sources
     * @return A cloned raster
     */
    public static GridCoverage2D clone(WritableRaster raster, GridGeometry2D gridGeometry2D, GridSampleDimension[] bands, GridCoverage2D referenceRaster, Double noDataValue, boolean keepMetadata) {
        Map propertyMap = null;
        if (keepMetadata) {
            propertyMap = referenceRaster.getProperties();
        }
        ColorModel originalColorModel = referenceRaster.getRenderedImage().getColorModel();
        if (Objects.isNull(gridGeometry2D)) {
            gridGeometry2D = referenceRaster.getGridGeometry();
        }
        int numBand = raster.getNumBands();
        int rasterDataType = raster.getDataBuffer().getDataType();
        ColorModel colorModel;
        if (originalColorModel.isCompatibleRaster(raster)) {
            colorModel = originalColorModel;
        }else {
            final ColorSpace cs = new BogusColorSpace(numBand);
            final int[] nBits = new int[numBand];
            Arrays.fill(nBits, DataBuffer.getDataTypeSize(rasterDataType));
            colorModel = new ComponentColorModel(cs, nBits, false, true, Transparency.OPAQUE, rasterDataType);
        }
        if (noDataValue != null) {
            GridSampleDimension[] newBands = new GridSampleDimension[numBand];
            for (int k = 0; k < numBand; k++) {
                if (bands != null) {
                    newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
                } else {
                    newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
                }
            }
            bands = newBands;
        }

        GridCoverage2D[] referenceRasterSources = keepMetadata ? referenceRaster.getSources().toArray(new GridCoverage2D[0]) : null;
        CharSequence rasterName = keepMetadata ? referenceRaster.getName() : "genericCoverage";

        final RenderedImage image = new BufferedImage(colorModel, raster, false, null);
        return gridCoverageFactory.create(rasterName, image, gridGeometry2D, bands, referenceRasterSources, propertyMap);

    }

    public static GridCoverage2D clone(WritableRaster raster, GridSampleDimension[] bands, GridCoverage2D referenceRaster, Double noDataValue, boolean keepMetadata) {
        return RasterUtils.clone(raster, null, bands, referenceRaster, noDataValue, keepMetadata);
    }

    public static GridCoverage2D clone(RenderedImage image, GridSampleDimension[] bands, GridCoverage2D referenceRaster, Double noDataValue, boolean keepMetadata) {
        return RasterUtils.clone(image, null, bands, referenceRaster, noDataValue, keepMetadata);
    }

    /**
     *
     * @param image Rendered image to create the raster from
     * @param gridGeometry2D gridGeometry2D to be used while cloning
     * @param bands bands to be used while cloning
     * @param referenceRaster referenceRaster to clone from
     * @param noDataValue noDataValue (if any) to be applied to the bands. If provided null. bands are unchanged.
     * @param keepMetadata if passed true, clone all possible metadata from the referenceRaster.
        keepMetaData controls the presence/absence of the following metadata of the referenceRasterObject:
            raster name: Name of the raster (GridCoverage2D level)
            raster properties: A Map of raster and image properties combined.
            raster sources
     * @return A cloned raster
     */
    public static GridCoverage2D clone(RenderedImage image, GridGeometry2D gridGeometry2D, GridSampleDimension[] bands, GridCoverage2D referenceRaster, Double noDataValue, boolean keepMetadata) {
        int numBand = image.getSampleModel().getNumBands();
        if (Objects.isNull(gridGeometry2D)) {
            gridGeometry2D = referenceRaster.getGridGeometry();
        }
        if (noDataValue != null) {
            GridSampleDimension[] newBands = new GridSampleDimension[numBand];
            for (int k = 0; k < numBand; k++) {
                if (bands != null) {
                    newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
                } else {
                    newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
                }
            }
            bands = newBands;
        }
        GridCoverage2D[] referenceRasterSources = keepMetadata ? referenceRaster.getSources().toArray(new GridCoverage2D[0]) : null;
        Map propertyMap = null;
        if (keepMetadata) {
            propertyMap = referenceRaster.getProperties();
        }
        CharSequence rasterName = keepMetadata ? referenceRaster.getName() : "genericCoverage";
        return gridCoverageFactory.create(rasterName, image, gridGeometry2D, bands, referenceRasterSources, propertyMap);
    }

    /**
     * Create a new empty raster from the given WritableRaster object.
     * @param raster The raster object to be wrapped as an image.
     * @param gridGeometry The grid geometry of the raster.
     * @param bands The bands of the raster.
     * @param noDataValue the noDataValue (if any) to be applied to all bands. If provided null, bands are unchanged.
     *                     keepMetaData controls the presence/absence of the following metadata of the referenceRasterObject:
     *             raster name: Name of the raster (GridCoverage2D level)
     *             raster properties: A Map of raster and image properties combined.
     *             raster sources
     * @return A new GridCoverage2D object.
     */
    public static GridCoverage2D create(WritableRaster raster, GridGeometry2D gridGeometry, GridSampleDimension[] bands, Double noDataValue) {
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

        if (noDataValue != null) {
            GridSampleDimension[] newBands = new GridSampleDimension[numBand];
            for (int k = 0; k < numBand; k++) {
                if (bands != null) {
                    newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
                } else {
                    newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
                }
            }
            bands = newBands;
        }

        final RenderedImage image = new BufferedImage(colorModel, raster, false, null);
        return gridCoverageFactory.create("genericCoverage", image, gridGeometry, bands, null, null);
    }

    public static GridCoverage2D create(RenderedImage image, GridGeometry2D gridGeometry, GridSampleDimension[] bands, Double noDataValue) {
        int numBand = image.getSampleModel().getNumBands();
        if (noDataValue != null) {
            GridSampleDimension[] newBands = new GridSampleDimension[numBand];
            for (int k = 0; k < numBand; k++) {
                if (bands != null) {
                    newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
                } else {
                    newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
                }
            }
            bands = newBands;
        }
        return gridCoverageFactory.create("genericCoverage", image, gridGeometry, bands, null, null);
    }

    /**
     * Create a sample dimension using a given sampleDimension as template, with the give no data value.
     * @param sampleDimension The sample dimension to be used as template.
     * @param noDataValue The no data value.
     * @return A new sample dimension with the given no data value.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static GridSampleDimension createSampleDimensionWithNoDataValue(GridSampleDimension sampleDimension, double noDataValue) {
        // if noDataValues contain noDataValue, then return the original sample dimension
        double existingNoDataValue = getNoDataValue(sampleDimension);
        if (Double.compare(existingNoDataValue, noDataValue) == 0) {
            return sampleDimension;
        }

        String description = sampleDimension.getDescription().toString();
        List<Category> categories = sampleDimension.getCategories();
        double offset = sampleDimension.getOffset();
        double scale = sampleDimension.getScale();

        // Copy existing categories. If the category contains noDataValue, split it into two categories.
        List<Category> newCategories = new ArrayList<>(categories.size());
        for (Category category : categories) {
            NumberRange<? extends Number> range = category.getRange();
            if (range.contains((Number) noDataValue)) {
                // Split this range to two ranges, one is [min, noDataValue), the other is (noDataValue, max]
                Number min = range.getMinValue();
                Number max = range.getMaxValue();
                final Class<? extends Number> clazz = ClassChanger.getWidestClass(min, max);
                min = ClassChanger.cast(min, clazz);
                max = ClassChanger.cast(max, clazz);
                Number nodata = ClassChanger.cast(noDataValue, clazz);
                if (min.doubleValue() < noDataValue) {
                    Category leftCategory = new Category(category.getName(), category.getColors(),
                            new NumberRange(clazz, min, range.isMinIncluded(), nodata, false));
                    newCategories.add(leftCategory);
                }
                if (max.doubleValue() > noDataValue) {
                    Category rightCategory = new Category(category.getName(), category.getColors(),
                            new NumberRange(clazz, nodata, false, max, range.isMaxIncluded()));
                    newCategories.add(rightCategory);
                }
            } else if (!category.getName().equals(Category.NODATA.getName())) {
                // This category does not contain no data value, just keep it as is.
                newCategories.add(category);
            }
        }

        // Add the no data value as a new category
        Number nodata = TypeMap.wrapSample(noDataValue, sampleDimension.getSampleDimensionType(), false);
        newCategories.add(new Category(Category.NODATA.getName(),
                new Color(0, 0, 0, 0),
                new NumberRange(nodata.getClass(), nodata, nodata)));

        return new GridSampleDimension(description, newCategories.toArray(new Category[0]), offset, scale);
    }

    public static GridSampleDimension createSampleDimensionWithNoDataValue(String description, double noDataValue) {
        Category noDataCategory = new Category(
                Category.NODATA.getName(),
                new Color(0, 0, 0, 0),
                new NumberRange<>(java.lang.Double.class, noDataValue, noDataValue));
        Category[] categories = new Category[] {noDataCategory};
        return new GridSampleDimension(description, categories, null);
    }

    /**
     * Remove no data value from the given sample dimension.
     * @param sampleDimension The sample dimension to be processed.
     * @return A new sample dimension without no data value, or the original sample dimension if it does not contain
     * no data value.
     */
    public static GridSampleDimension removeNoDataValue(GridSampleDimension sampleDimension) {
        String description = sampleDimension.getDescription().toString();
        List<Category> categories = sampleDimension.getCategories();
        List<Category> newCategories = new ArrayList<>(categories.size());
        InternationalString noDataCategoryName = Category.NODATA.getName();
        for (Category category : categories) {
            if (!category.getName().equals(noDataCategoryName)) {
                newCategories.add(category);
            }
        }

        if (newCategories.size() == categories.size()) {
            // Nothing changed, return the original sample dimension
            return sampleDimension;
        }

        double offset = sampleDimension.getOffset();
        double scale = sampleDimension.getScale();
        return new GridSampleDimension(description, newCategories.toArray(new Category[0]), offset, scale);
    }

    /**
     * Get the no data value from the given sample dimension. Please use this method to retrieve the no data value
     * of a raster band, instead of {@link GridSampleDimension#getNoDataValues()}. The reason is that the latter
     * method has a strange semantics: it treats whatever qualitative categories as no data value, which is not what
     * we want. Additionally, the GeoTiff writer and ArcGrid writer uses the same algorithm as our method for finding
     * no data values when writing the metadata of raster bands.
     * @param sampleDimension The sample dimension to be processed.
     * @return The no data value, or {@link Double#NaN} if the sample dimension does not contain no data value.
     */
    public static double getNoDataValue(GridSampleDimension sampleDimension) {
        List<Category> categories = sampleDimension.getCategories();
        InternationalString noDataCategoryName = Category.NODATA.getName();
        for (Category category : categories) {
            if (category.getName().equals(noDataCategoryName)) {
                return category.getRange().getMinimum();
            }
        }
        return Double.NaN;
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

    public static Point2D getWorldCornerCoordinates(GridCoverage2D raster, int colX, int rowY) throws TransformException {
        return raster.getGridGeometry().getGridToCRS2D(PixelOrientation.UPPER_LEFT).transform(new GridCoordinates2D(colX - 1, rowY - 1), null);
    }

    /***
     * Returns the world coordinates of the given grid coordinate. The expected grid coordinates are 1 indexed. The function also enforces a range check to make sure given grid coordinates are actually inside the grid.
     * @param raster
     * @param colX
     * @param rowY
     * @return
     * @throws IndexOutOfBoundsException
     * @throws TransformException
     */
    public static Point2D getWorldCornerCoordinatesWithRangeCheck(GridCoverage2D raster, int colX, int rowY) throws IndexOutOfBoundsException, TransformException {
        GridCoordinates2D gridCoordinates2D = new GridCoordinates2D(colX - 1, rowY - 1);
        if (!(raster.getGridGeometry().getGridRange2D().contains(gridCoordinates2D))) throw new IndexOutOfBoundsException(String.format("Specified pixel coordinates (%d, %d) do not lie in the raster", colX, rowY));
        return raster.getGridGeometry().getGridToCRS2D(PixelOrientation.UPPER_LEFT).transform(gridCoordinates2D, null);
    }
    public static int[] getGridCoordinatesFromWorld(GridCoverage2D raster, double longitude, double latitude) throws TransformException {
        DirectPosition2D directPosition2D = new DirectPosition2D(raster.getCoordinateReferenceSystem2D(), longitude, latitude);
        DirectPosition worldCoord = raster.getGridGeometry().getCRSToGrid2D(PixelOrientation.UPPER_LEFT).transform((DirectPosition) directPosition2D, null);
        double[] coords = worldCoord.getCoordinate();
        int[] gridCoords = new int[] {(int) Math.floor(coords[0]), (int) Math.floor(coords[1])};
        return gridCoords;
    }

    /***
     * Throws an exception if band index is greater than the number of bands in a raster
     * @param raster
     * @param band
     * @return
     * @throws IllegalArgumentException
     */
    public static void ensureBand(GridCoverage2D raster, int band) throws IllegalArgumentException {
        if (band < 1 || band > RasterAccessors.numBands(raster)) {
            throw new IllegalArgumentException(String.format("Provided band index %d is not present in the raster", band));
        }
    }

    public static Raster getRaster(RenderedImage renderedImage) {
        while (renderedImage instanceof RenderedImageAdapter) {
            renderedImage = ((RenderedImageAdapter) renderedImage).getWrappedImage();
        }
        if (renderedImage instanceof BufferedImage) {
            // This is a fast path for BufferedImage. If we call getData() directly, it will make a
            // hard copy of the raster. We can avoid this overhead by calling getRaster().
            return ((BufferedImage) renderedImage).getRaster();
        } else {
            return renderedImage.getData();
        }
    }

    public static Geometry convertCRSIfNeeded(Geometry geometry, CoordinateReferenceSystem targetCRS) {
        int geomSRID = geometry.getSRID();
        // If the geometry has a SRID and it is not the same as the raster CRS, we need to transform the geometry
        // to the raster CRS.
        // Note that:
        // In Sedona vector, we do not perform implicit CRS transform. Everything must be done explicitly via ST_Transform
        // In Sedona raster, we do implicit CRS transform if the geometry has a SRID and the raster has a CRS
        if (targetCRS != null && !(targetCRS instanceof DefaultEngineeringCRS) && geomSRID > 0) {
            try {
                geometry = FunctionsGeoTools.transformToGivenTarget(geometry, null, targetCRS, true);
            } catch (FactoryException | TransformException e) {
                throw new RuntimeException("Cannot transform CRS of query window", e);
            }
        }
        return geometry;
    }

    /**
     * Converts data types to data type codes
     * @param s pixel data type possible values {D, I, B, F, S, US} <br><br>
     *          Update: add support to convert RS_BandPixelType data type string to data type code possible values: <br>
     *          {REAL_64BITS, SIGNED_32BITS, UNSIGNED_8BITS, REAL_32BITS, SIGNED_16BITS, UNSIGNED_16BITS}
     * @return Data type code
     */
    public static int getDataTypeCode(String s) {
        switch (s.toUpperCase()) {
            case "D":
            case "REAL_64BITS":
                return 5;
            case "I":
            case "SIGNED_32BITS":
                return 3;
            case "B":
            case "UNSIGNED_8BITS":
                return 0;
            case "F":
            case "REAL_32BITS":
                return 4;
            case "S":
            case "SIGNED_16BITS":
                return 2;
            case "US":
            case "UNSIGNED_16BITS":
                return 1;
        }
        return 5; // defaulting to double
    }

    public static boolean isDataTypeIntegral(int dataTypeCode) {
        //returns true if the datatype code refers to an int-like datatype (int, short, etc)
        switch (dataTypeCode) {
            case 3: //int
            case 0: //byte
            case 2: //short
            case 1: //unsigned short
                return true;
            case 5: //double
            case 4: //float
            default:
                return false;
        }
    }

    /**
     * This is an experimental method as it does not copy the original raster properties (e.g. color model, sample model, etc.)
     * moved from MapAlgebra.java
     * TODO: Copy the original raster properties
     * @param gridCoverage2D
     * @param bandValues
     * @return
     */
    public static GridCoverage2D copyRasterAndAppendBand(GridCoverage2D gridCoverage2D, Number[] bandValues, Double noDataValue) {
        // Get the original image and its properties
        RenderedImage originalImage = gridCoverage2D.getRenderedImage();
        Raster raster = getRaster(originalImage);
        Point location = raster.getBounds().getLocation();
        WritableRaster wr = RasterFactory.createBandedRaster(raster.getDataBuffer().getDataType(), originalImage.getWidth(), originalImage.getHeight(), gridCoverage2D.getNumSampleDimensions() + 1, location);
        // Copy the raster data and append the new band values
        for (int i = 0; i < raster.getWidth(); i++) {
            for (int j = 0; j < raster.getHeight(); j++) {
                if (bandValues instanceof Double[]) {
                    double[] pixels = raster.getPixel(i, j, (double[]) null);
                    double[] copiedPixels = new double[pixels.length + 1];
                    System.arraycopy(pixels, 0, copiedPixels, 0, pixels.length);
                    copiedPixels[pixels.length] = (double) bandValues[j * raster.getWidth() + i];
                    wr.setPixel(i, j, copiedPixels);
                } else if (bandValues instanceof Integer[]) {
                    int[] pixels = raster.getPixel(i, j, (int[]) null);
                    int[] copiedPixels = new int[pixels.length + 1];
                    System.arraycopy(pixels, 0, copiedPixels, 0, pixels.length);
                    copiedPixels[pixels.length] = (int) bandValues[j * raster.getWidth() + i];
                    wr.setPixel(i, j, copiedPixels);
                }
            }
        }
        // Add a sample dimension for newly added band
        int numBand = wr.getNumBands();
        GridSampleDimension[] originalSampleDimensions = gridCoverage2D.getSampleDimensions();
        GridSampleDimension[] sampleDimensions = new GridSampleDimension[numBand];
        System.arraycopy(originalSampleDimensions, 0, sampleDimensions, 0, originalSampleDimensions.length);
        if (noDataValue != null) {
            sampleDimensions[numBand - 1] = createSampleDimensionWithNoDataValue("band" + numBand, noDataValue);
        } else {
            sampleDimensions[numBand - 1] = new GridSampleDimension("band" + numBand);
        }
        // Construct a GridCoverage2D with the copied image.
        return clone(wr, gridCoverage2D.getGridGeometry(), sampleDimensions, gridCoverage2D, null, true);
    }

    public static GridCoverage2D copyRasterAndAppendBand(GridCoverage2D gridCoverage2D, Number[] bandValues) {
        return copyRasterAndAppendBand(gridCoverage2D, bandValues, null);
    }

    public static GridCoverage2D copyRasterAndReplaceBand(GridCoverage2D gridCoverage2D, int bandIndex, Number[] bandValues, Double noDataValue, boolean removeNoDataIfNull) {
        // Do not allow the band index to be out of bounds
        ensureBand(gridCoverage2D, bandIndex);
        // Get the original image and its properties
        RenderedImage originalImage = gridCoverage2D.getRenderedImage();
        Raster raster = getRaster(originalImage);
        WritableRaster wr = raster.createCompatibleWritableRaster();
        // Copy the raster data and replace the band values
        for (int i = 0; i < raster.getWidth(); i++) {
            for (int j = 0; j < raster.getHeight(); j++) {
                if (bandValues instanceof Double[]) {
                    double[] bands = raster.getPixel(i, j, (double[]) null);
                    bands[bandIndex - 1] = (double) bandValues[j * raster.getWidth() + i];
                    wr.setPixel(i, j, bands);
                } else if (bandValues instanceof Integer[]) {
                    int[] bands = raster.getPixel(i, j, (int[]) null);
                    bands[bandIndex - 1] = (int) bandValues[j * raster.getWidth() + i];
                    wr.setPixel(i, j, bands);
                }
            }
        }
        GridSampleDimension[] sampleDimensions = gridCoverage2D.getSampleDimensions();
        GridSampleDimension sampleDimension = sampleDimensions[bandIndex - 1];
        if (noDataValue == null && removeNoDataIfNull) {
            sampleDimensions[bandIndex - 1] = removeNoDataValue(sampleDimension);
        } else if (noDataValue != null) {
            sampleDimensions[bandIndex - 1] = createSampleDimensionWithNoDataValue(sampleDimension, noDataValue);
        }
        return clone(wr, gridCoverage2D.getGridGeometry(), sampleDimensions, gridCoverage2D, null, true);
    }

    public static GridCoverage2D copyRasterAndReplaceBand(GridCoverage2D gridCoverage2D, int bandIndex, Number[] bandValues) {
        return copyRasterAndReplaceBand(gridCoverage2D, bandIndex, bandValues, null, false);
    }
}
