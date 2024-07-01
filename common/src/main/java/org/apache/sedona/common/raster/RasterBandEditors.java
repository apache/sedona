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

import java.awt.geom.Point2D;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Collections;
import javax.media.jai.RasterFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.processing.operation.Crop;
import org.locationtech.jts.geom.Geometry;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class RasterBandEditors {
  /**
   * Adds no-data value to the raster.
   *
   * @param raster Source raster to add no-data value
   * @param bandIndex Band index to add no-data value
   * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
   * @param replace if true replaces the previous no-data value with the specified no-data value
   * @return Raster with no-data value
   */
  public static GridCoverage2D setBandNoDataValue(
      GridCoverage2D raster, int bandIndex, Double noDataValue, boolean replace) {
    RasterUtils.ensureBand(raster, bandIndex);
    Double rasterNoData = RasterBandAccessors.getBandNoDataValue(raster, bandIndex);

    // Remove no-Data if it is null
    if (noDataValue == null) {
      if (RasterBandAccessors.getBandNoDataValue(raster) == null) {
        return raster;
      }
      GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
      sampleDimensions[bandIndex - 1] =
          RasterUtils.removeNoDataValue(sampleDimensions[bandIndex - 1]);
      return RasterUtils.clone(
          raster.getRenderedImage(), null, sampleDimensions, raster, null, true);
    }

    if (rasterNoData != null && rasterNoData.equals(noDataValue)) {
      return raster;
    }
    GridSampleDimension[] bands = raster.getSampleDimensions();
    bands[bandIndex - 1] =
        RasterUtils.createSampleDimensionWithNoDataValue(bands[bandIndex - 1], noDataValue);

    if (replace) {
      if (rasterNoData == null) {
        throw new IllegalArgumentException(
            "The raster provided doesn't have a no-data value. Please provide a raster that has a no-data value to use `replace` option.");
      }

      Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());
      int dataTypeCode = rasterData.getDataBuffer().getDataType();
      int numBands = RasterAccessors.numBands(raster);
      int height = RasterAccessors.getHeight(raster);
      int width = RasterAccessors.getWidth(raster);
      WritableRaster wr =
          RasterFactory.createBandedRaster(dataTypeCode, width, height, numBands, null);
      double[] bandData =
          rasterData.getSamples(0, 0, width, height, bandIndex - 1, (double[]) null);
      for (int i = 0; i < bandData.length; i++) {
        if (bandData[i] == rasterNoData) {
          bandData[i] = noDataValue;
        }
      }
      wr.setSamples(0, 0, width, height, bandIndex - 1, bandData);
      return RasterUtils.clone(wr, null, bands, raster, null, true);
    }

    return RasterUtils.clone(raster.getRenderedImage(), null, bands, raster, null, true);
  }

  /**
   * Adds no-data value to the raster.
   *
   * @param raster Source raster to add no-data value
   * @param bandIndex Band index to add no-data value
   * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
   * @return Raster with no-data value
   */
  public static GridCoverage2D setBandNoDataValue(
      GridCoverage2D raster, int bandIndex, Double noDataValue) {
    return setBandNoDataValue(raster, bandIndex, noDataValue, false);
  }

  /**
   * Adds no-data value to the raster.
   *
   * @param raster Source raster to add no-data value
   * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
   * @return Raster with no-data value
   */
  public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, Double noDataValue) {
    return setBandNoDataValue(raster, 1, noDataValue, false);
  }

  /**
   * @param toRaster Raster to add the band
   * @param fromRaster Raster from the band will be copied
   * @param fromBand Index of Raster band that will be copied
   * @param toRasterIndex Index of Raster where the copied band will be changed
   * @return A raster with either replacing a band or adding a new band at the end the toRaster,
   *     with the specified band values extracted from the fromRaster at given band
   */
  public static GridCoverage2D addBand(
      GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand, int toRasterIndex) {
    RasterUtils.ensureBand(fromRaster, fromBand);
    ensureBandAppend(toRaster, toRasterIndex);
    RasterUtils.isRasterSameShape(toRaster, fromRaster);

    int width = RasterAccessors.getWidth(toRaster), height = RasterAccessors.getHeight(toRaster);

    Raster rasterData = RasterUtils.getRaster(fromRaster.getRenderedImage());

    // get datatype of toRaster to preserve data type
    int dataTypeCode =
        RasterUtils.getRaster(toRaster.getRenderedImage()).getDataBuffer().getDataType();
    int numBands = RasterAccessors.numBands(toRaster);
    Double noDataValue = RasterBandAccessors.getBandNoDataValue(fromRaster, fromBand);

    if (RasterUtils.isDataTypeIntegral(dataTypeCode)) {
      int[] bandValues = rasterData.getSamples(0, 0, width, height, fromBand - 1, (int[]) null);
      if (numBands + 1 == toRasterIndex) {
        return RasterUtils.copyRasterAndAppendBand(toRaster, bandValues, noDataValue);
      } else {
        return RasterUtils.copyRasterAndReplaceBand(
            toRaster, fromBand, bandValues, noDataValue, false);
      }
    } else {
      double[] bandValues =
          rasterData.getSamples(0, 0, width, height, fromBand - 1, (double[]) null);
      if (numBands + 1 == toRasterIndex) {
        return RasterUtils.copyRasterAndAppendBand(toRaster, bandValues, noDataValue);
      } else {
        return RasterUtils.copyRasterAndReplaceBand(
            toRaster, fromBand, bandValues, noDataValue, false);
      }
    }
  }

  /**
   * The new band will be added to the end of the toRaster
   *
   * @param toRaster Raster to add the band
   * @param fromRaster Raster from the band will be copied
   * @param fromBand Index of Raster band that will be copied
   * @return A raster with either replacing a band or adding a new band at the end the toRaster,
   *     with the specified band values extracted from the fromRaster at given band
   */
  public static GridCoverage2D addBand(
      GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand) {
    int endBand = RasterAccessors.numBands(toRaster) + 1;
    return addBand(toRaster, fromRaster, fromBand, endBand);
  }

  /**
   * Index of fromRaster will be taken as 1 and will be copied at the end of the toRaster
   *
   * @param toRaster Raster to add the band
   * @param fromRaster Raster from the band will be copied
   * @return A raster with either replacing a band or adding a new band at the end the toRaster,
   *     with the specified band values extracted from the fromRaster at given band
   */
  public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster) {
    return addBand(toRaster, fromRaster, 1);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4,
      GridCoverage2D raster5,
      GridCoverage2D raster6,
      GridCoverage2D raster7) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3, raster4, raster5, raster6), raster7);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4,
      GridCoverage2D raster5,
      GridCoverage2D raster6) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3, raster4, raster5), raster6);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4,
      GridCoverage2D raster5) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3, raster4), raster5);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1,
      GridCoverage2D raster2,
      GridCoverage2D raster3,
      GridCoverage2D raster4) {
    return rasterUnion(rasterUnion(raster1, raster2, raster3), raster4);
  }

  public static GridCoverage2D rasterUnion(
      GridCoverage2D raster1, GridCoverage2D raster2, GridCoverage2D raster3) {
    return rasterUnion(rasterUnion(raster1, raster2), raster3);
  }

  public static GridCoverage2D rasterUnion(GridCoverage2D raster1, GridCoverage2D raster2) {
    // Ensure both rasters have the same dimensions
    RasterUtils.isRasterSameShape(raster1, raster2);

    int numBands1 = RasterAccessors.numBands(raster1);
    int numBands2 = RasterAccessors.numBands(raster2);

    // Start with raster1 as the initial result
    GridCoverage2D result = raster1;

    // Append each band from raster2 to the result
    for (int bandIndex = 1; bandIndex <= numBands2; bandIndex++) {
      result = addBand(result, raster2, bandIndex);
    }

    return result;
  }

  /**
   * Check if the band index is either present or at the end of the raster
   *
   * @param raster Raster to check
   * @param band Band index to append to the raster
   */
  private static void ensureBandAppend(GridCoverage2D raster, int band) {
    if (band < 1 || band > RasterAccessors.numBands(raster) + 1) {
      throw new IllegalArgumentException(
          String.format("Provided band index %d is not present in the raster", band));
    }
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @param noDataValue no-Data value for empty cells
   * @param crop Specifies to keep the original extent or not
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(
      GridCoverage2D raster, int band, Geometry geometry, double noDataValue, boolean crop)
      throws FactoryException, TransformException {

    // Selecting the band from original raster
    RasterUtils.ensureBand(raster, band);
    GridCoverage2D singleBandRaster = RasterBandAccessors.getBand(raster, new int[] {band});

    Pair<GridCoverage2D, Geometry> pair =
        RasterUtils.setDefaultCRSAndTransform(singleBandRaster, geometry);
    singleBandRaster = pair.getLeft();
    geometry = pair.getRight();

    // Crop the raster
    // this will shrink the extent of the raster to the geometry
    Crop cropObject = new Crop();
    ParameterValueGroup parameters = cropObject.getParameters();
    parameters.parameter("Source").setValue(singleBandRaster);
    parameters.parameter(Crop.PARAMNAME_DEST_NODATA).setValue(new double[] {noDataValue});
    parameters.parameter(Crop.PARAMNAME_ROI).setValue(geometry);

    GridCoverage2D newRaster = (GridCoverage2D) cropObject.doOperation(parameters, null);

    if (!crop) {
      double[] metadataOriginal = RasterAccessors.metadata(raster);
      int widthOriginalRaster = (int) metadataOriginal[2],
          heightOriginalRaster = (int) metadataOriginal[3];
      Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());

      // create a new raster and set a default value that's the no-data value
      String bandType = RasterBandAccessors.getBandType(raster, 1);
      int dataTypeCode = RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, 1));
      boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(dataTypeCode);
      WritableRaster resultRaster =
          RasterFactory.createBandedRaster(
              dataTypeCode, widthOriginalRaster, heightOriginalRaster, 1, null);
      int sizeOfArray = widthOriginalRaster * heightOriginalRaster;
      if (isDataTypeIntegral) {
        int[] array =
            ArrayUtils.toPrimitive(
                Collections.nCopies(sizeOfArray, (int) noDataValue)
                    .toArray(new Integer[sizeOfArray]));
        resultRaster.setSamples(0, 0, widthOriginalRaster, heightOriginalRaster, 0, array);
      } else {
        double[] array =
            ArrayUtils.toPrimitive(
                Collections.nCopies(sizeOfArray, noDataValue).toArray(new Double[sizeOfArray]));
        resultRaster.setSamples(0, 0, widthOriginalRaster, heightOriginalRaster, 0, array);
      }

      // rasterize the geometry to iterate over the clipped raster
      GridCoverage2D rasterized = RasterConstructors.asRaster(geometry, raster, bandType, 150);
      Raster rasterizedData = RasterUtils.getRaster(rasterized.getRenderedImage());
      double[] metadataRasterized = RasterAccessors.metadata(rasterized);
      int widthRasterized = (int) metadataRasterized[2],
          heightRasterized = (int) metadataRasterized[3];

      for (int j = 0; j < heightRasterized; j++) {
        for (int i = 0; i < widthRasterized; i++) {
          Point2D point = RasterUtils.getWorldCornerCoordinates(rasterized, i, j);
          int[] rasterCoord =
              RasterUtils.getGridCoordinatesFromWorld(raster, point.getX(), point.getY());
          int x = Math.abs(rasterCoord[0]), y = Math.abs(rasterCoord[1]);

          if (rasterizedData.getPixel(i, j, (int[]) null)[0] == 0) {
            continue;
          }

          if (isDataTypeIntegral) {
            int[] pixelValue = rasterData.getPixel(x, y, (int[]) null);

            resultRaster.setPixel(x, y, new int[] {pixelValue[band - 1]});
          } else {
            double[] pixelValue = rasterData.getPixel(x, y, (double[]) null);

            resultRaster.setPixel(x, y, new double[] {pixelValue[band - 1]});
          }
        }
      }
      newRaster =
          RasterUtils.clone(
              resultRaster,
              raster.getGridGeometry(),
              newRaster.getSampleDimensions(),
              newRaster,
              noDataValue,
              true);
    } else {
      RenderedImage image = newRaster.getRenderedImage();
      int minX = image.getMinX();
      int minY = image.getMinY();
      if (minX != 0 || minY != 0) {
        newRaster = RasterUtils.shiftRasterToZeroOrigin(newRaster, noDataValue);
      } else {
        newRaster =
            RasterUtils.clone(
                newRaster.getRenderedImage(),
                newRaster.getGridGeometry(),
                newRaster.getSampleDimensions(),
                newRaster,
                noDataValue,
                true);
      }
    }

    return newRaster;
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry.
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @param noDataValue no-Data value for empty cells
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(
      GridCoverage2D raster, int band, Geometry geometry, double noDataValue)
      throws FactoryException, TransformException {
    return clip(raster, band, geometry, noDataValue, true);
  }

  /**
   * Return a clipped raster with the specified ROI by the geometry. No-data value will be taken as
   * the lowest possible value for the data type and crop will be `true`.
   *
   * @param raster Raster to clip
   * @param band Band number to perform clipping
   * @param geometry Specify ROI
   * @return A clip Raster with defined ROI by the geometry
   */
  public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry)
      throws FactoryException, TransformException {
    boolean isDataTypeIntegral =
        RasterUtils.isDataTypeIntegral(
            RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band)));

    double noDataValue;
    if (isDataTypeIntegral) {
      noDataValue = Integer.MIN_VALUE;
    } else {
      noDataValue = Double.MIN_VALUE;
    }
    return clip(raster, band, geometry, noDataValue, true);
  }
}
