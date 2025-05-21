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

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.media.jai.RasterFactory;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;

public class RasterBandAccessors {

  public static Double getBandNoDataValue(GridCoverage2D raster, int band) {
    RasterUtils.ensureBand(raster, band);
    GridSampleDimension bandSampleDimension = raster.getSampleDimension(band - 1);
    double noDataValue = RasterUtils.getNoDataValue(bandSampleDimension);
    if (Double.isNaN(noDataValue)) {
      return null;
    } else {
      return noDataValue;
    }
  }

  public static Double getBandNoDataValue(GridCoverage2D raster) {
    return getBandNoDataValue(raster, 1);
  }

  public static long getCount(GridCoverage2D raster, int band, boolean excludeNoDataValue) {
    Double bandNoDataValue = RasterBandAccessors.getBandNoDataValue(raster, band);
    int width = RasterAccessors.getWidth(raster);
    int height = RasterAccessors.getHeight(raster);
    if (excludeNoDataValue && bandNoDataValue != null) {
      RasterUtils.ensureBand(raster, band);
      Raster r = RasterUtils.getRaster(raster.getRenderedImage());
      double[] pixels = r.getSamples(0, 0, width, height, band - 1, (double[]) null);
      long numberOfPixel = 0;
      for (double bandValue : pixels) {
        if (Double.compare(bandValue, bandNoDataValue) != 0) {
          numberOfPixel += 1;
        }
      }
      return numberOfPixel;
    } else {
      // code for false
      return (long) width * (long) height;
    }
  }

  public static long getCount(GridCoverage2D raster) {
    return getCount(raster, 1, true);
  }

  public static long getCount(GridCoverage2D raster, int band) {
    return getCount(raster, band, true);
  }

  //    Removed for now as it InferredExpression doesn't support function with same arity but
  // different argument types
  //    Will be added later once it is supported.
  //    public static Integer getCount(GridCoverage2D raster, boolean excludeNoDataValue) {
  //        return getCount(raster, 1, excludeNoDataValue);
  //    }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param allTouched Include pixels touched by roi geometry
   * @param excludeNoData Specifies whether to exclude no-data value or not
   * @param lenient Return null if the raster and roi do not intersect when set to true, otherwise
   *     will throw an exception
   * @return An array with all the stats for the region
   * @throws FactoryException
   */
  public static Double[] getZonalStatsAll(
      GridCoverage2D raster,
      Geometry roi,
      int band,
      boolean allTouched,
      boolean excludeNoData,
      boolean lenient)
      throws FactoryException {
    List<Object> objects = getStatObjects(raster, roi, band, allTouched, excludeNoData, lenient);
    if (objects == null) {
      return null;
    }
    DescriptiveStatistics stats = (DescriptiveStatistics) objects.get(0);
    double[] pixelData = (double[]) objects.get(1);

    // Shortcut for an edge case where ROI barely intersects with raster's extent, but it doesn't
    // intersect with the centroid of the pixel.
    // This happens when allTouched parameter is false.
    if (pixelData.length == 0) {
      return new Double[] {0.0, null, null, null, null, null, null, null, null};
    }

    // order of stats
    // count, sum, mean, median, mode, stddev, variance, min, max
    Double[] result = new Double[9];
    result[0] = (double) stats.getN();
    if (stats.getN() == 0) {
      result[1] = null;
    } else {
      result[1] = stats.getSum();
    }
    double mean = stats.getMean();
    result[2] = Double.isNaN(mean) ? null : mean;
    double median = stats.getPercentile(50);
    result[3] = Double.isNaN(median) ? null : median;
    result[4] = zonalMode(pixelData);
    double stdDev = stats.getStandardDeviation();
    result[5] = Double.isNaN(stdDev) ? null : stats.getStandardDeviation();
    double variance = stats.getVariance();
    result[6] = Double.isNaN(variance) ? null : variance;
    double min = stats.getMin();
    result[7] = Double.isNaN(min) ? null : min;
    double max = stats.getMax();
    result[8] = Double.isNaN(max) ? null : max;

    return result;
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param allTouched Include pixels touched by roi geometry
   * @param excludeNoData Specifies whether to exclude no-data value or not
   * @return An array with all the stats for the region
   * @throws FactoryException
   */
  public static Double[] getZonalStatsAll(
      GridCoverage2D raster, Geometry roi, int band, boolean allTouched, boolean excludeNoData)
      throws FactoryException {
    return getZonalStatsAll(raster, roi, band, allTouched, excludeNoData, true);
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param allTouched Include pixels touched by roi geometry
   * @return An array with all the stats for the region, excludeNoData is set to true
   * @throws FactoryException
   */
  public static Double[] getZonalStatsAll(
      GridCoverage2D raster, Geometry roi, int band, boolean allTouched) throws FactoryException {
    return getZonalStatsAll(raster, roi, band, allTouched, true);
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @return An array with all the stats for the region, excludeNoData is set to true
   * @throws FactoryException
   */
  public static Double[] getZonalStatsAll(GridCoverage2D raster, Geometry roi, int band)
      throws FactoryException {
    return getZonalStatsAll(raster, roi, band, false);
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @return An array with all the stats for the region, excludeNoData is set to true and band is
   *     set to 1
   * @throws FactoryException
   */
  public static Double[] getZonalStatsAll(GridCoverage2D raster, Geometry roi)
      throws FactoryException {
    return getZonalStatsAll(raster, roi, 1);
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param statType Define the statistic to be computed
   * @param allTouched Include pixels touched by roi geometry
   * @param excludeNoData Specifies whether to exclude no-data value or not
   * @param lenient Return null if the raster and roi do not intersect when set to true, otherwise
   *     will throw an exception
   * @return A double precision floating point number representing the requested statistic
   *     calculated over the specified region.
   * @throws FactoryException
   */
  public static Double getZonalStats(
      GridCoverage2D raster,
      Geometry roi,
      int band,
      String statType,
      boolean allTouched,
      boolean excludeNoData,
      boolean lenient)
      throws FactoryException {
    List<Object> objects = getStatObjects(raster, roi, band, allTouched, excludeNoData, lenient);
    if (objects == null) {
      return null;
    }
    DescriptiveStatistics stats = (DescriptiveStatistics) objects.get(0);
    double[] pixelData = (double[]) objects.get(1);

    switch (statType.toLowerCase()) {
      case "sum":
        if (pixelData.length == 0) {
          return null;
        } else {
          return stats.getSum();
        }
      case "average":
      case "avg":
      case "mean":
        double mean = stats.getMean();
        return Double.isNaN(mean) ? null : mean;
      case "count":
        return (double) stats.getN();
      case "max":
        double max = stats.getMax();
        return Double.isNaN(max) ? null : max;
      case "min":
        double min = stats.getMin();
        return Double.isNaN(min) ? null : min;
      case "stddev":
      case "sd":
        double stdDev = stats.getStandardDeviation();
        return Double.isNaN(stdDev) ? null : stdDev;
      case "median":
        double median = stats.getPercentile(50);
        return Double.isNaN(median) ? null : median;
      case "mode":
        return zonalMode(pixelData);
      case "variance":
        double variance = stats.getVariance();
        return Double.isNaN(variance) ? null : variance;
      default:
        throw new IllegalArgumentException(
            "Please select from the accepted options. Some of the valid options are sum, mean, stddev, etc.");
    }
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param statType Define the statistic to be computed
   * @param allTouched Include pixels touched by roi geometry
   * @param excludeNoData Specifies whether to exclude no-data value or not
   * @return A double precision floating point number representing the requested statistic
   *     calculated over the specified region.
   * @throws FactoryException
   */
  public static Double getZonalStats(
      GridCoverage2D raster,
      Geometry roi,
      int band,
      String statType,
      boolean allTouched,
      boolean excludeNoData)
      throws FactoryException {
    return getZonalStats(raster, roi, band, statType, allTouched, excludeNoData, true);
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param statType Define the statistic to be computed
   * @param allTouched Include pixels touched by roi geometry
   * @return A double precision floating point number representing the requested statistic
   *     calculated over the specified region. The excludeNoData is set to true.
   * @throws FactoryException
   */
  public static Double getZonalStats(
      GridCoverage2D raster, Geometry roi, int band, String statType, boolean allTouched)
      throws FactoryException {
    return getZonalStats(raster, roi, band, statType, allTouched, true);
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param statType Define the statistic to be computed
   * @return A double precision floating point number representing the requested statistic
   *     calculated over the specified region. The excludeNoData is set to true.
   * @throws FactoryException
   */
  public static Double getZonalStats(GridCoverage2D raster, Geometry roi, int band, String statType)
      throws FactoryException {
    return getZonalStats(raster, roi, band, statType, false);
  }

  /**
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param statType Define the statistic to be computed
   * @return A double precision floating point number representing the requested statistic
   *     calculated over the specified region. The excludeNoData is set to true and band is set to
   *     1.
   * @throws FactoryException
   */
  public static Double getZonalStats(GridCoverage2D raster, Geometry roi, String statType)
      throws FactoryException {
    return getZonalStats(raster, roi, 1, statType, false);
  }

  /**
   * @param pixelData An array of double with pixel values
   * @return Mode of the pixel values. If there is multiple with same occurrence, then the largest
   *     value will be returned.
   */
  private static Double zonalMode(double[] pixelData) {
    double[] modes = StatUtils.mode(pixelData);
    // Return NaN when ROI and raster's extent overlap, but there's no pixel data.
    // This behavior only happens when allTouched parameter is false.
    if (modes.length == 0) {
      return null;
    }
    return modes[modes.length - 1];
  }

  /**
   * An intermediate function to compute zonal statistics
   *
   * @param raster Raster to use for computing stats
   * @param roi Geometry to define the region of interest
   * @param band Band to be used for computation
   * @param allTouched Include pixels touched by roi geometry
   * @param excludeNoData Specifies whether to exclude no-data value or not
   * @param lenient Return null if the raster and roi do not intersect when set to true, otherwise
   *     will throw an exception
   * @return an object of DescriptiveStatistics and an array of double with pixel data.
   * @throws FactoryException
   */
  private static List<Object> getStatObjects(
      GridCoverage2D raster,
      Geometry roi,
      int band,
      boolean allTouched,
      boolean excludeNoData,
      boolean lenient)
      throws FactoryException {
    RasterUtils.ensureBand(raster, band);

    if (RasterAccessors.srid(raster) != roi.getSRID()) {
      // implicitly converting roi geometry CRS to raster CRS
      roi = RasterUtils.convertCRSIfNeeded(roi, raster.getCoordinateReferenceSystem());
      // have to set the SRID as RasterUtils.convertCRSIfNeeded doesn't set it even though the
      // geometry is in raster's CRS
      roi = Functions.setSRID(roi, RasterAccessors.srid(raster));
    }

    // checking if the raster contains the geometry
    if (!RasterPredicates.rsIntersects(raster, roi)) {
      if (lenient) {
        return null;
      } else {
        throw new IllegalArgumentException(
            "The provided geometry is not intersecting the raster. Please provide a geometry that is in the raster's extent.");
      }
    }

    Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());
    String datatype = RasterBandAccessors.getBandType(raster, band);
    Double noDataValue = RasterBandAccessors.getBandNoDataValue(raster, band);
    // Adding an arbitrary value '150' for the pixels that are under the geometry.
    GridCoverage2D rasterizedGeom =
        RasterConstructors.asRasterWithRasterExtent(roi, raster, datatype, allTouched, 150, null);
    Raster rasterizedData = RasterUtils.getRaster(rasterizedGeom.getRenderedImage());
    int width = RasterAccessors.getWidth(rasterizedGeom),
        height = RasterAccessors.getHeight(rasterizedGeom);
    double[] rasterizedPixelData =
        rasterizedData.getSamples(0, 0, width, height, 0, (double[]) null);
    double[] rasterPixelData =
        rasterData.getSamples(0, 0, width, height, band - 1, (double[]) null);

    List<Double> pixelData = new ArrayList<>();

    for (int k = 0; k < rasterPixelData.length; k++) {

      // Pixels with a value of 0 in the rasterizedPixelData are skipped during computation,
      // as they fall outside the geometry specified by 'roi'.
      // The region of interest defined by 'roi' contains pixel values of 150,
      // as initialized when constructing the raster via
      // RasterConstructors.asRasterWithRasterExtent.
      if (rasterizedPixelData[k] == 0
          || excludeNoData && noDataValue != null && rasterPixelData[k] == noDataValue) {
        continue;
      } else {
        pixelData.add(rasterPixelData[k]);
      }
    }

    double[] pixelsArray = pixelData.stream().mapToDouble(d -> d).toArray();

    DescriptiveStatistics stats = new DescriptiveStatistics(pixelsArray);

    List<Object> statObjects = new ArrayList<>();

    statObjects.add(stats);
    statObjects.add(pixelsArray);
    return statObjects;
  }

  public static double getSummaryStats(
      GridCoverage2D rasterGeom, String statType, int band, boolean excludeNoDataValue) {
    double[] stats = getSummaryStatsAll(rasterGeom, band, excludeNoDataValue);

    if ("count".equalsIgnoreCase(statType)) {
      return stats[0];
    } else if ("sum".equalsIgnoreCase(statType)) {
      return stats[1];
    } else if ("mean".equalsIgnoreCase(statType)) {
      return stats[2];
    } else if ("stddev".equalsIgnoreCase(statType)) {
      return stats[3];
    } else if ("min".equalsIgnoreCase(statType)) {
      return stats[4];
    } else if ("max".equalsIgnoreCase(statType)) {
      return stats[5];
    } else {
      throw new IllegalArgumentException(
          "Invalid 'statType': '"
              + statType
              + "'. Expected one of: 'count', 'sum', 'mean', 'stddev', 'min', 'max'.");
    }
  }

  public static double getSummaryStats(GridCoverage2D rasterGeom, String statType, int band) {
    return getSummaryStats(rasterGeom, statType, band, true);
  }

  public static double getSummaryStats(GridCoverage2D rasterGeom, String statType) {
    return getSummaryStats(rasterGeom, statType, 1, true);
  }

  public static double[] getSummaryStatsAll(
      GridCoverage2D rasterGeom, int band, boolean excludeNoDataValue) {
    RasterUtils.ensureBand(rasterGeom, band);
    Raster raster = RasterUtils.getRaster(rasterGeom.getRenderedImage());
    int height = RasterAccessors.getHeight(rasterGeom),
        width = RasterAccessors.getWidth(rasterGeom);
    double[] pixels = raster.getSamples(0, 0, width, height, band - 1, (double[]) null);

    List<Double> pixelData = null;

    if (excludeNoDataValue) {
      pixelData = new ArrayList<>();
      Double noDataValue = RasterBandAccessors.getBandNoDataValue(rasterGeom, band);
      for (double pixel : pixels) {
        if (noDataValue == null || pixel != noDataValue) {
          pixelData.add(pixel);
        }
      }
    }

    DescriptiveStatistics stats = null;

    if (pixelData != null) {
      pixels = pixelData.stream().mapToDouble(d -> d).toArray();
    }
    stats = new DescriptiveStatistics(pixels);

    StandardDeviation sd = new StandardDeviation(false);

    double count = stats.getN();
    double sum = stats.getSum();
    double mean = stats.getMean();
    double stddev = sd.evaluate(pixels, mean);
    double min = stats.getMin();
    double max = stats.getMax();

    return new double[] {count, sum, mean, stddev, min, max};
  }

  public static double[] getSummaryStatsAll(GridCoverage2D raster, int band) {
    return getSummaryStatsAll(raster, band, true);
  }

  public static double[] getSummaryStatsAll(GridCoverage2D raster) {
    return getSummaryStatsAll(raster, 1, true);
  }

  //  Adding the function signature when InferredExpression supports function with same arity but
  // different argument types
  //    public static double[] getSummaryStats(GridCoverage2D raster, boolean excludeNoDataValue) {
  //        return getSummaryStats(raster, 1, excludeNoDataValue);
  //    }

  /**
   * @param rasterGeom The raster where the bands will be extracted from.
   * @param bandIndexes The bands to be added to new raster.
   * @return Raster with the specified bands.
   */
  public static GridCoverage2D getBand(GridCoverage2D rasterGeom, int[] bandIndexes)
      throws FactoryException {
    Double noDataValue;
    double[] metadata = RasterAccessors.metadata(rasterGeom);
    int width = (int) metadata[2], height = (int) metadata[3];
    GridCoverage2D resultRaster =
        RasterConstructors.makeEmptyRaster(
            bandIndexes.length,
            width,
            height,
            metadata[0],
            metadata[1],
            metadata[4],
            metadata[5],
            metadata[6],
            metadata[7],
            (int) metadata[8]);

    // Get raster data type
    Raster raster = RasterUtils.getRaster(rasterGeom.getRenderedImage());
    int dataTypeCode = raster.getDataBuffer().getDataType();
    boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(dataTypeCode);

    // Get band data that's required
    int[] bandsDistinct = Arrays.stream(bandIndexes).distinct().toArray();
    HashMap<Integer, Object> bandData = new HashMap<>();
    for (int curBand : bandsDistinct) {
      RasterUtils.ensureBand(rasterGeom, curBand);
      if (isDataTypeIntegral) {
        bandData.put(
            curBand - 1, raster.getSamples(0, 0, width, height, curBand - 1, (int[]) null));
      } else {
        bandData.put(
            curBand - 1, raster.getSamples(0, 0, width, height, curBand - 1, (double[]) null));
      }
    }

    // Create Writable Raster with the datatype of given raster
    WritableRaster wr =
        RasterFactory.createBandedRaster(dataTypeCode, width, height, bandIndexes.length, null);

    GridSampleDimension[] sampleDimensionsOg = rasterGeom.getSampleDimensions();
    GridSampleDimension[] sampleDimensionsResult = resultRaster.getSampleDimensions();
    for (int i = 0; i < bandIndexes.length; i++) {
      sampleDimensionsResult[i] = sampleDimensionsOg[bandIndexes[i] - 1];
      if (isDataTypeIntegral) {
        wr.setSamples(0, 0, width, height, i, (int[]) bandData.get(bandIndexes[i] - 1));
      } else {
        wr.setSamples(0, 0, width, height, i, (double[]) bandData.get(bandIndexes[i] - 1));
      }
      noDataValue = RasterBandAccessors.getBandNoDataValue(rasterGeom, bandIndexes[i]);
      GridSampleDimension sampleDimension = sampleDimensionsResult[i];
      if (noDataValue != null) {
        sampleDimensionsResult[i] =
            RasterUtils.createSampleDimensionWithNoDataValue(sampleDimension, noDataValue);
      }
    }
    return RasterUtils.clone(
        wr,
        sampleDimensionsResult,
        rasterGeom,
        null,
        false); // do not keep meta-data since this will most probably be a new raster
  }

  public static String getBandType(GridCoverage2D raster, int band) {
    RasterUtils.ensureBand(raster, band);
    GridSampleDimension bandSampleDimension = raster.getSampleDimension(band - 1);
    return bandSampleDimension.getSampleDimensionType().name();
  }

  public static String getBandType(GridCoverage2D raster) {
    return getBandType(raster, 1);
  }

  /**
   * Returns true if the band is filled with only nodata values.
   *
   * @param raster The raster to check
   * @param band The 1-based index of band to check
   * @return true if the band is filled with only nodata values, false otherwise
   */
  public static boolean bandIsNoData(GridCoverage2D raster, int band) {
    RasterUtils.ensureBand(raster, band);
    Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());
    int width = rasterData.getWidth();
    int height = rasterData.getHeight();
    double noDataValue = RasterUtils.getNoDataValue(raster.getSampleDimension(band - 1));
    if (Double.isNaN(noDataValue)) {
      return false;
    }
    double[] pixels = rasterData.getSamples(0, 0, width, height, band - 1, (double[]) null);
    for (double pixel : pixels) {
      if (Double.compare(pixel, noDataValue) != 0) {
        return false;
      }
    }
    return true;
  }

  public static boolean bandIsNoData(GridCoverage2D raster) {
    return bandIsNoData(raster, 1);
  }
}
