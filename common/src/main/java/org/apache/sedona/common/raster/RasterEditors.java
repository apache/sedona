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

import static org.apache.sedona.common.raster.MapAlgebra.addBandFromArray;
import static org.apache.sedona.common.raster.MapAlgebra.bandAsArray;

import java.awt.geom.Point2D;
import java.awt.image.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import javax.media.jai.Interpolation;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.utils.RasterInterpolate;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.index.strtree.STRtree;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;

public class RasterEditors {

  /**
   * Changes the band pixel type of a specific band of a raster.
   *
   * @param raster The input raster.
   * @param dataType The desired data type of the pixel.
   * @return The modified raster with updated pixel type.
   */
  public static GridCoverage2D setPixelType(GridCoverage2D raster, String dataType) {
    int newDataType = RasterUtils.getDataTypeCode(dataType);

    // Extracting the original data
    RenderedImage originalImage = raster.getRenderedImage();
    Raster originalData = RasterUtils.getRaster(originalImage);

    int width = originalImage.getWidth();
    int height = originalImage.getHeight();
    int numBands = originalImage.getSampleModel().getNumBands();

    // Create a new writable raster with the specified data type
    WritableRaster modifiedRaster =
        RasterFactory.createBandedRaster(newDataType, width, height, numBands, null);

    // Populate modified raster and recreate sample dimensions
    GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
    for (int band = 0; band < numBands; band++) {
      double[] samples = originalData.getSamples(0, 0, width, height, band, (double[]) null);
      modifiedRaster.setSamples(0, 0, width, height, band, samples);
      if (!Double.isNaN(RasterUtils.getNoDataValue(sampleDimensions[band]))) {
        sampleDimensions[band] =
            RasterUtils.createSampleDimensionWithNoDataValue(
                sampleDimensions[band],
                castRasterDataType(
                    RasterUtils.getNoDataValue(sampleDimensions[band]), newDataType));
      }
    }

    // Clone the original GridCoverage2D with the modified raster
    return RasterUtils.clone(
        modifiedRaster, raster.getGridGeometry(), sampleDimensions, raster, null, true);
  }

  public static GridCoverage2D setSrid(GridCoverage2D raster, int srid) {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      crs = FunctionsGeoTools.sridToCRS(srid);
    }

    GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
    MathTransform2D transform = raster.getGridGeometry().getGridToCRS2D();
    Map<?, ?> properties = raster.getProperties();
    GridCoverage[] sources = raster.getSources().toArray(new GridCoverage[0]);
    return gridCoverageFactory.create(
        raster.getName().toString(),
        raster.getRenderedImage(),
        crs,
        transform,
        raster.getSampleDimensions(),
        sources,
        properties);
  }

  public static GridCoverage2D setGeoReference(
      GridCoverage2D raster, String geoRefCoords, String format) {
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
      throw new IllegalArgumentException(
          "Please select between the following formats GDAL and ESRI");
    }
    int height = RasterAccessors.getHeight(raster), width = RasterAccessors.getWidth(raster);

    GridGeometry2D gridGeometry2D =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, width, height),
            PixelOrientation.UPPER_LEFT,
            affine,
            raster.getCoordinateReferenceSystem(),
            null);
    return RasterUtils.clone(
        raster.getRenderedImage(),
        gridGeometry2D,
        raster.getSampleDimensions(),
        raster,
        null,
        true);
  }

  public static GridCoverage2D setGeoReference(GridCoverage2D raster, String geoRefCoords) {
    return setGeoReference(raster, geoRefCoords, "GDAL");
  }

  public static GridCoverage2D setGeoReference(
      GridCoverage2D raster,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY) {
    String geoRedCoord =
        String.format("%f %f %f %f %f %f", scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    return setGeoReference(raster, geoRedCoord, "GDAL");
  }

  public static GridCoverage2D resample(
      GridCoverage2D raster,
      double widthOrScale,
      double heightOrScale,
      double gridX,
      double gridY,
      boolean useScale,
      String algorithm)
      throws TransformException {
    /*
     * Old Parameters
     */
    AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);
    int originalWidth = RasterAccessors.getWidth(raster),
        originalHeight = RasterAccessors.getHeight(raster);
    double upperLeftX = affine.getTranslateX(), upperLeftY = affine.getTranslateY();
    double originalSkewX = affine.getShearX(), originalSkewY = affine.getShearY();
    double originalScaleX = affine.getScaleX(), originalScaleY = affine.getScaleY();
    CoordinateReferenceSystem crs = raster.getCoordinateReferenceSystem2D();

    /*
     * New Parameters
     */
    int newWidth = useScale ? originalWidth : (int) Math.floor(widthOrScale);
    int newHeight = useScale ? originalHeight : (int) Math.floor(heightOrScale);
    double newScaleX = useScale ? widthOrScale : originalScaleX;
    double newScaleY = useScale ? heightOrScale : originalScaleY;
    double newUpperLeftX = upperLeftX, newUpperLeftY = upperLeftY;

    if (noConfigChange(
        originalWidth,
        originalHeight,
        upperLeftX,
        upperLeftY,
        originalScaleX,
        originalScaleY,
        newWidth,
        newHeight,
        gridX,
        gridY,
        newScaleX,
        newScaleY,
        useScale)) {
      // no reconfiguration parameters provided
      return raster;
    }

    Envelope2D envelope2D = raster.getEnvelope2D();
    // process scale changes due to changes in widthOrScale and heightOrScale
    if (!useScale) {
      newScaleX = (Math.abs(envelope2D.getMaxX() - envelope2D.getMinX())) / newWidth;
      newScaleY =
          Math.signum(originalScaleY)
              * Math.abs(envelope2D.getMaxY() - envelope2D.getMinY())
              / newHeight;
    } else {
      // height and width cannot have floating point, ceil them to next greatest integer in that
      // case.
      newWidth =
          (int)
              Math.ceil(
                  Math.abs(envelope2D.getMaxX() - envelope2D.getMinX()) / Math.abs(newScaleX));
      newHeight =
          (int)
              Math.ceil(
                  Math.abs(envelope2D.getMaxY() - envelope2D.getMinY()) / Math.abs(newScaleY));
    }

    if (!approximateEquals(upperLeftX, gridX) || !approximateEquals(upperLeftY, gridY)) {
      // change upperLefts to gridX/Y to check if any warping is needed
      GridCoverage2D tempRaster =
          setGeoReference(raster, gridX, gridY, newScaleX, newScaleY, originalSkewX, originalSkewY);

      // check expected grid coordinates for old upperLefts
      int[] expectedCellCoordinates =
          RasterUtils.getGridCoordinatesFromWorld(tempRaster, upperLeftX, upperLeftY);

      // get expected world coordinates at the expected grid coordinates
      Point2D expectedGeoPoint =
          RasterUtils.getWorldCornerCoordinates(
              tempRaster, expectedCellCoordinates[0] + 1, expectedCellCoordinates[1] + 1);

      // check for shift
      if (!approximateEquals(expectedGeoPoint.getX(), upperLeftX)) {
        if (!useScale) {
          newScaleX = Math.abs(envelope2D.getMaxX() - expectedGeoPoint.getX()) / newWidth;
        } else {
          // width cannot have floating point, ceil it to next greatest integer in that case.
          newWidth =
              (int)
                  Math.ceil(
                      Math.abs(envelope2D.getMaxX() - expectedGeoPoint.getX())
                          / Math.abs(newScaleX));
        }
        newUpperLeftX = expectedGeoPoint.getX();
      }

      if (!approximateEquals(expectedGeoPoint.getY(), upperLeftY)) {
        if (!useScale) {
          newScaleY =
              Math.signum(newScaleY)
                  * Math.abs(envelope2D.getMinY() - expectedGeoPoint.getY())
                  / newHeight;
        } else {
          // height cannot have floating point, ceil it to next greatest integer in that case.
          newHeight =
              (int)
                  Math.ceil(
                      Math.abs(envelope2D.getMinY() - expectedGeoPoint.getY())
                          / Math.abs(newScaleY));
        }
        newUpperLeftY = expectedGeoPoint.getY();
      }
    }

    MathTransform transform =
        new AffineTransform2D(
            newScaleX, originalSkewY, originalSkewX, newScaleY, newUpperLeftX, newUpperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, newWidth, newHeight),
            PixelInCell.CELL_CORNER,
            transform,
            crs,
            null);
    Interpolation resamplingAlgorithm = createInterpolationAlgorithm(algorithm);
    GridCoverage2D newRaster;
    GridCoverage2D noDataValueMask;
    GridCoverage2D resampledNoDataValueMask;

    if ((!Objects.isNull(algorithm) && !algorithm.isEmpty())
        && (algorithm.equalsIgnoreCase("Bilinear") || algorithm.equalsIgnoreCase("Bicubic"))) {
      // Create and resample noDataValue mask
      noDataValueMask = RasterUtils.extractNoDataValueMask(raster);
      resampledNoDataValueMask =
          resample(
              noDataValueMask,
              widthOrScale,
              heightOrScale,
              gridX,
              -gridY,
              useScale,
              "NearestNeighbor");

      // Replace noDataValues with mean of neighbors and resample
      raster = RasterUtils.replaceNoDataValues(raster);
      newRaster =
          (GridCoverage2D)
              Operations.DEFAULT.resample(raster, null, gridGeometry, resamplingAlgorithm);

      // Apply resampled noDataValue mask to resampled raster
      newRaster = RasterUtils.applyRasterMask(newRaster, resampledNoDataValueMask);
    } else {
      newRaster =
          (GridCoverage2D)
              Operations.DEFAULT.resample(raster, null, gridGeometry, resamplingAlgorithm);
    }
    return newRaster;
  }

  public static GridCoverage2D resample(
      GridCoverage2D raster,
      double widthOrScale,
      double heightOrScale,
      boolean useScale,
      String algorithm)
      throws TransformException {
    return resample(
        raster,
        widthOrScale,
        heightOrScale,
        RasterAccessors.getUpperLeftX(raster),
        RasterAccessors.getUpperLeftY(raster),
        useScale,
        algorithm);
  }

  public static GridCoverage2D resample(
      GridCoverage2D raster, GridCoverage2D referenceRaster, boolean useScale, String algorithm)
      throws FactoryException, TransformException {
    int srcSRID = RasterAccessors.srid(raster);
    int destSRID = RasterAccessors.srid(referenceRaster);
    if (srcSRID != destSRID) {
      throw new IllegalArgumentException(
          "Provided input raster and reference raster have different SRIDs");
    }
    double[] refRasterMetadata = RasterAccessors.metadata(referenceRaster);
    int newWidth = (int) refRasterMetadata[2];
    int newHeight = (int) refRasterMetadata[3];
    double gridX = refRasterMetadata[0];
    double gridY = refRasterMetadata[1];
    double newScaleX = refRasterMetadata[4];
    double newScaleY = refRasterMetadata[5];
    if (useScale) {
      return resample(raster, newScaleX, newScaleY, gridX, gridY, useScale, algorithm);
    }
    return resample(raster, newWidth, newHeight, gridX, gridY, useScale, algorithm);
  }

  private static boolean approximateEquals(double a, double b) {
    double tolerance = 1E-6;
    return Math.abs(a - b) <= tolerance;
  }

  private static boolean noConfigChange(
      int oldWidth,
      int oldHeight,
      double oldUpperX,
      double oldUpperY,
      double originalScaleX,
      double originalScaleY,
      int newWidth,
      int newHeight,
      double newUpperX,
      double newUpperY,
      double newScaleX,
      double newScaleY,
      boolean useScale) {
    if (!useScale)
      return ((oldWidth == newWidth)
          && (oldHeight == newHeight)
          && (approximateEquals(oldUpperX, newUpperX))
          && (approximateEquals(oldUpperY, newUpperY)));
    return ((approximateEquals(originalScaleX, newScaleX))
        && (approximateEquals(originalScaleY, newScaleY))
        && (approximateEquals(oldUpperX, newUpperX))
        && (approximateEquals(oldUpperY, newUpperY)));
  }

  public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom) {
    return normalizeAll(rasterGeom, 0d, 255d, true, null, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom, double minLim, double maxLim) {
    return normalizeAll(rasterGeom, minLim, maxLim, true, null, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom, double minLim, double maxLim, boolean normalizeAcrossBands) {
    return normalizeAll(rasterGeom, minLim, maxLim, normalizeAcrossBands, null, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom,
      double minLim,
      double maxLim,
      boolean normalizeAcrossBands,
      Double noDataValue) {
    return normalizeAll(rasterGeom, minLim, maxLim, normalizeAcrossBands, noDataValue, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom,
      double minLim,
      double maxLim,
      Double noDataValue,
      Double minValue,
      Double maxValue) {
    return normalizeAll(rasterGeom, minLim, maxLim, true, noDataValue, minValue, maxValue);
  }

  /**
   * @param rasterGeom Raster to be normalized
   * @param minLim Lower limit of normalization range
   * @param maxLim Upper limit of normalization range
   * @param normalizeAcrossBands flag to determine the normalization method
   * @param noDataValue NoDataValue used in raster
   * @param minValue Minimum value in raster
   * @param maxValue Maximum value in raster
   * @return a raster with all values in all bands normalized between minLim and maxLim
   */
  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom,
      double minLim,
      double maxLim,
      boolean normalizeAcrossBands,
      Double noDataValue,
      Double minValue,
      Double maxValue) {
    if (minLim > maxLim) {
      throw new IllegalArgumentException("minLim cannot be greater than maxLim");
    }

    int numBands = rasterGeom.getNumSampleDimensions();
    RenderedImage renderedImage = rasterGeom.getRenderedImage();
    int rasterDataType = renderedImage.getSampleModel().getDataType();

    double globalMin = minValue != null ? minValue : Double.MAX_VALUE;
    double globalMax = maxValue != null ? maxValue : -Double.MAX_VALUE;

    // Initialize arrays to store band-wise min and max values
    double[] minValues = new double[numBands];
    double[] maxValues = new double[numBands];
    Arrays.fill(minValues, Double.MAX_VALUE);
    Arrays.fill(maxValues, -Double.MAX_VALUE);

    // Trigger safe mode if noDataValue is null - noDataValue is set to maxLim and data values are
    // normalized to range [minLim, maxLim-1].
    // This is done to prevent setting valid data as noDataValue.
    double safetyTrigger = (noDataValue == null) ? 1 : 0;

    // Compute global min and max values across all bands if necessary and not provided
    if (minValue == null || maxValue == null) {
      for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
        double[] bandValues = bandAsArray(rasterGeom, bandIndex + 1);
        double bandNoDataValue =
            RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(bandIndex));

        if (noDataValue == null) {
          noDataValue = maxLim;
        }

        for (double val : bandValues) {
          if (val != bandNoDataValue) {
            if (normalizeAcrossBands) {
              globalMin = Math.min(globalMin, val);
              globalMax = Math.max(globalMax, val);
            } else {
              minValues[bandIndex] = Math.min(minValues[bandIndex], val);
              maxValues[bandIndex] = Math.max(maxValues[bandIndex], val);
            }
          }
        }
      }
    } else {
      globalMin = minValue;
      globalMax = maxValue;
    }

    // Normalize each band
    for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
      double[] bandValues = bandAsArray(rasterGeom, bandIndex + 1);
      double bandNoDataValue = RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(bandIndex));
      double currentMin =
          normalizeAcrossBands ? globalMin : (minValue != null ? minValue : minValues[bandIndex]);
      double currentMax =
          normalizeAcrossBands ? globalMax : (maxValue != null ? maxValue : maxValues[bandIndex]);

      if (Double.compare(currentMax, currentMin) == 0) {
        Arrays.fill(bandValues, minLim);
      } else {
        for (int i = 0; i < bandValues.length; i++) {
          if (bandValues[i] != bandNoDataValue) {
            double normalizedValue =
                minLim
                    + ((bandValues[i] - currentMin) * (maxLim - safetyTrigger - minLim))
                        / (currentMax - currentMin);
            bandValues[i] = castRasterDataType(normalizedValue, rasterDataType);
          } else {
            bandValues[i] = noDataValue;
          }
        }
      }

      // Update the raster with the normalized band and noDataValue
      rasterGeom = addBandFromArray(rasterGeom, bandValues, bandIndex + 1);
      rasterGeom = RasterBandEditors.setBandNoDataValue(rasterGeom, bandIndex + 1, noDataValue);
    }

    return rasterGeom;
  }

  public static GridCoverage2D reprojectMatch(
      GridCoverage2D source, GridCoverage2D target, String interpolationAlgorithm) {
    Interpolation interp = createInterpolationAlgorithm(interpolationAlgorithm);
    CoordinateReferenceSystem crs = target.getCoordinateReferenceSystem();
    GridGeometry gridGeometry = target.getGridGeometry();
    GridCoverage2D result =
        (GridCoverage2D) Operations.DEFAULT.resample(source, crs, gridGeometry, interp);
    return RasterUtils.shiftRasterToZeroOrigin(result, null);
  }

  private static Interpolation createInterpolationAlgorithm(String algorithm) {
    Interpolation interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
    if (!Objects.isNull(algorithm) && !algorithm.isEmpty()) {
      if (algorithm.equalsIgnoreCase("nearestneighbor")) {
        interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
      } else if (algorithm.equalsIgnoreCase("bilinear")) {
        interp = Interpolation.getInstance(Interpolation.INTERP_BILINEAR);
      } else if (algorithm.equalsIgnoreCase("bicubic")) {
        interp = Interpolation.getInstance(Interpolation.INTERP_BICUBIC);
      }
    }
    return interp;
  }

  private static double castRasterDataType(double value, int dataType) {
    switch (dataType) {
      case DataBuffer.TYPE_BYTE:
        // Cast to unsigned byte (0-255)
        double remainder = value % 256;
        double v = (remainder < 0) ? remainder + 256 : remainder;
        return (int) v;
      case DataBuffer.TYPE_SHORT:
        return (short) value;
      case DataBuffer.TYPE_INT:
        return (int) value;
      case DataBuffer.TYPE_USHORT:
        return (char) value;
      case DataBuffer.TYPE_FLOAT:
        return (float) value;
      case DataBuffer.TYPE_DOUBLE:
      default:
        return value;
    }
  }

  public static GridCoverage2D interpolate(GridCoverage2D inputRaster)
      throws IllegalArgumentException {
    return interpolate(inputRaster, 2.0, "fixed", null, null, null);
  }

  public static GridCoverage2D interpolate(GridCoverage2D inputRaster, Double power)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, "fixed", null, null, null);
  }

  public static GridCoverage2D interpolate(GridCoverage2D inputRaster, Double power, String mode)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, mode, null, null, null);
  }

  public static GridCoverage2D interpolate(
      GridCoverage2D inputRaster, Double power, String mode, Double numPointsOrRadius)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, mode, numPointsOrRadius, null, null);
  }

  public static GridCoverage2D interpolate(
      GridCoverage2D inputRaster,
      Double power,
      String mode,
      Double numPointsOrRadius,
      Double maxRadiusOrMinPoints)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, mode, numPointsOrRadius, maxRadiusOrMinPoints, null);
  }

  public static GridCoverage2D interpolate(
      GridCoverage2D inputRaster,
      Double power,
      String mode,
      Double numPointsOrRadius,
      Double maxRadiusOrMinPoints,
      Integer band)
      throws IllegalArgumentException {
    if (!mode.equalsIgnoreCase("variable") && !mode.equalsIgnoreCase("fixed")) {
      throw new IllegalArgumentException(
          "Invalid 'mode': '" + mode + "'. Expected one of: 'Variable', 'Fixed'.");
    }

    Raster rasterData = inputRaster.getRenderedImage().getData();
    WritableRaster raster =
        rasterData.createCompatibleWritableRaster(
            RasterAccessors.getWidth(inputRaster), RasterAccessors.getHeight(inputRaster));
    int width = raster.getWidth();
    int height = raster.getHeight();
    int numBands = raster.getNumBands();
    GridSampleDimension[] gridSampleDimensions = inputRaster.getSampleDimensions();

    if (band != null && (band < 1 || band > numBands)) {
      throw new IllegalArgumentException("Band index out of range.");
    }

    // Interpolation for each band
    for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
      if (band == null || bandIndex == band - 1) {
        // Generate STRtree
        STRtree strtree = RasterInterpolate.generateSTRtree(inputRaster, bandIndex);
        Double noDataValue = RasterUtils.getNoDataValue(inputRaster.getSampleDimension(bandIndex));
        int countNoDataValues = 0;

        // Skip band if STRtree is empty or has all valid data pixels
        if (strtree.isEmpty() || strtree.size() == width * height) {
          continue;
        }

        if (mode.equalsIgnoreCase("variable") && strtree.size() < numPointsOrRadius) {
          throw new IllegalArgumentException(
              "Parameter 'numPoints' is larger than no. of valid pixels in band "
                  + bandIndex
                  + ". Please choose an appropriate value");
        }

        // Perform interpolation
        for (int y = 0; y < height; y++) {
          for (int x = 0; x < width; x++) {
            double value = rasterData.getSampleDouble(x, y, bandIndex);
            if (Double.isNaN(value) || value == noDataValue) {
              countNoDataValues++;
              double interpolatedValue =
                  RasterInterpolate.interpolateIDW(
                      x,
                      y,
                      strtree,
                      width,
                      height,
                      power,
                      mode,
                      numPointsOrRadius,
                      maxRadiusOrMinPoints);
              interpolatedValue =
                  (Double.isNaN(interpolatedValue)) ? noDataValue : interpolatedValue;
              if (interpolatedValue != noDataValue) {
                countNoDataValues--;
              }
              raster.setSample(x, y, bandIndex, interpolatedValue);
            } else {
              raster.setSample(x, y, bandIndex, value);
            }
          }
        }

        // If all noDataValues are interpolated, update band metadata (remove nodatavalue)
        if (countNoDataValues == 0) {
          gridSampleDimensions[bandIndex] =
              RasterUtils.removeNoDataValue(inputRaster.getSampleDimension(bandIndex));
        }
      } else {
        raster.setSamples(
            0,
            0,
            raster.getWidth(),
            raster.getHeight(),
            band,
            rasterData.getSamples(
                0, 0, raster.getWidth(), raster.getHeight(), band, (double[]) null));
      }
    }

    return RasterUtils.clone(
        raster, inputRaster.getGridGeometry(), gridSampleDimensions, inputRaster, null, true);
  }
}
