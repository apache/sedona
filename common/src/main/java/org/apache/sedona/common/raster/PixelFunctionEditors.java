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

import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import javax.media.jai.RasterFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.locationtech.jts.geom.Geometry;

public class PixelFunctionEditors {

  /**
   * Returns a raster by replacing the values of pixels in a specified rectangular region.
   *
   * @param raster Raster to be edited
   * @param band Band of the raster to be edited
   * @param colX UpperLeftX of the region
   * @param rowY UpperLeftY of the region
   * @param width Width of the said region
   * @param height Height of the said region
   * @param values Array of values to be inserted into the said region
   * @param keepNoData To keep No Data value or add the given value to the raster
   * @return An updated Raster
   */
  public static GridCoverage2D setValues(
      GridCoverage2D raster,
      int band,
      int colX,
      int rowY,
      int width,
      int height,
      double[] values,
      boolean keepNoData) {
    RasterUtils.ensureBand(raster, band);
    if (values.length != width * height) {
      throw new IllegalArgumentException(
          "Shape of 'values' doesn't match provided width and height.");
    }

    WritableRaster rasterCopied = makeCopiedRaster(raster);

    Double noDataValue = null;
    if (keepNoData) {
      noDataValue = RasterBandAccessors.getBandNoDataValue(raster, band);
    }

    // making them 0-indexed
    colX--;
    rowY--;

    int iterator = 0;
    for (int j = rowY; j < rowY + height; j++) {
      for (int i = colX; i < colX + width; i++) {
        double[] pixel = rasterCopied.getPixel(i, j, (double[]) null);
        if (keepNoData && noDataValue != null && noDataValue == pixel[band - 1]) {
          iterator++;
          continue;
        } else {
          pixel[band - 1] = values[iterator];
        }
        rasterCopied.setPixel(i, j, pixel);
        iterator++;
      }
    }
    return RasterUtils.clone(
        rasterCopied,
        raster.getSampleDimensions(),
        raster,
        null,
        true); // Keep metadata since this is essentially the same raster
  }

  /**
   * Returns a raster by replacing the values of pixels in a specified rectangular region.
   * Convenience function without keepNoData parameter.
   *
   * @param raster Raster to be edited
   * @param band Band of the raster to be edited
   * @param colX UpperLeftX of the region
   * @param rowY UpperLeftY of the region
   * @param width Width of the said region
   * @param height Height of the said region
   * @param values Array of values to be inserted into the said region
   * @return An updated Raster
   */
  public static GridCoverage2D setValues(
      GridCoverage2D raster, int band, int colX, int rowY, int width, int height, double[] values) {
    return setValues(raster, band, colX, rowY, width, height, values, false);
  }

  /**
   * Returns a raster by replacing the values of pixels in a specified geometry region. It converts
   * the Geometry to a raster using RS_AsRaster.
   *
   * @param raster Raster to be edited
   * @param band Band of the raster to be edited
   * @param geom Geometry region to update
   * @param value Value to updated in the said region
   * @param allTouched When set to true, sets value for all pixels touched by geom
   * @param keepNoData To keep no data value or not
   * @return An updated raster
   * @throws FactoryException
   * @throws TransformException
   */
  public static GridCoverage2D setValues(
      GridCoverage2D raster,
      int band,
      Geometry geom,
      double value,
      boolean allTouched,
      boolean keepNoData)
      throws FactoryException, TransformException {
    RasterUtils.ensureBand(raster, band);

    Pair<GridCoverage2D, Geometry> pair = RasterUtils.setDefaultCRSAndTransform(raster, geom);
    raster = pair.getLeft();
    geom = pair.getRight();

    // checking if the raster contains the geometry
    if (!RasterPredicates.rsIntersects(raster, geom)) {
      throw new IllegalArgumentException(
          "The provided geometry is not intersecting the raster. Please provide a geometry that is in the raster's extent.");
    }

    String bandDataType = RasterBandAccessors.getBandType(raster, band);

    GridCoverage2D rasterizedGeom;
    Double noDataValue = null;

    if (keepNoData) {
      noDataValue = RasterBandAccessors.getBandNoDataValue(raster, band);
      rasterizedGeom =
          RasterConstructors.asRaster(geom, raster, bandDataType, allTouched, value, noDataValue);
    } else {
      rasterizedGeom = RasterConstructors.asRaster(geom, raster, bandDataType, allTouched, value);
    }

    Raster rasterizedGeomData = RasterUtils.getRaster(rasterizedGeom.getRenderedImage());
    double colX = RasterAccessors.getUpperLeftX(rasterizedGeom),
        rowY = RasterAccessors.getUpperLeftY(rasterizedGeom);
    int heightGeometryRaster = RasterAccessors.getHeight(rasterizedGeom),
        widthGeometryRaster = RasterAccessors.getWidth(rasterizedGeom);
    int heightOriginalRaster = RasterAccessors.getHeight(raster),
        widthOriginalRaster = RasterAccessors.getWidth(raster);
    WritableRaster rasterCopied = makeCopiedRaster(raster);

    // Converting geometry to raster and then iterating through them
    int[] pixelLocation = RasterUtils.getGridCoordinatesFromWorld(raster, colX, rowY);
    int x = pixelLocation[0], y = pixelLocation[1];

    // rasterX & rasterY are the starting pixels for the target raster
    int rasterX = Math.max(x, 0);
    int rasterY = Math.max(y, 0);
    // geometryX & geometryY are the starting pixels for the geometry raster
    int geometryX = rasterX - x;
    int geometryY = rasterY - y;
    // widthRegion & heightRegion are the size of the region to update
    int widthRegion = Math.min(widthGeometryRaster - geometryX, widthOriginalRaster - rasterX);
    int heightRegion = Math.min(heightGeometryRaster - geometryY, heightOriginalRaster - rasterY);

    for (int j = 0; j < heightRegion; j++) {
      for (int i = 0; i < widthRegion; i++) {
        double[] pixel = rasterCopied.getPixel(rasterX + i, rasterY + j, (double[]) null);
        // [0] as only one band in the rasterized Geometry
        double pixelNew =
            rasterizedGeomData.getPixel(geometryX + i, geometryY + j, (double[]) null)[0];
        // skipping 0 from the rasterized geometry as
        if (pixelNew == 0 || keepNoData && noDataValue != null && noDataValue == pixel[band - 1]) {
          continue;
        } else {
          pixel[band - 1] = pixelNew;
        }
        rasterCopied.setPixel(rasterX + i, rasterY + j, pixel);
      }
    }

    return RasterUtils.clone(
        rasterCopied,
        raster.getSampleDimensions(),
        raster,
        null,
        true); // keep metadata since this is essentially the same raster
  }

  /**
   * Returns a raster by replacing the values of pixels in a specified geometry region. It converts
   * the Geometry to a raster using RS_AsRaster. A convenience function with keepNoData as false.
   *
   * @param raster Input raster to be updated
   * @param band Band of the raster to be edited
   * @param geom Geometry region to update
   * @param value Value to updated in the said region
   * @param allTouched When set to true, sets value for all pixels touched by geom
   * @return An updated raster
   * @throws FactoryException
   * @throws TransformException
   */
  public static GridCoverage2D setValues(
      GridCoverage2D raster, int band, Geometry geom, double value, boolean allTouched)
      throws FactoryException, TransformException {
    return setValues(raster, band, geom, value, allTouched, false);
  }

  /**
   * Returns a raster by replacing the values of pixels in a specified geometry region. It converts
   * the Geometry to a raster using RS_AsRaster. A convenience function with keepNoData as false.
   *
   * @param raster Input raster to be updated
   * @param band Band of the raster to be edited
   * @param geom Geometry region to update
   * @param value Value to updated in the said region
   * @return An updated raster
   * @throws FactoryException
   * @throws TransformException
   */
  public static GridCoverage2D setValues(
      GridCoverage2D raster, int band, Geometry geom, double value)
      throws FactoryException, TransformException {
    return setValues(raster, band, geom, value, false, false);
  }

  /**
   * Return a raster by updated the pixel specified by pixel location.
   *
   * @param raster Input raster to be edited
   * @param band Band of the raster to be updated
   * @param colX Column of the pixel
   * @param rowY Row of the pixel
   * @param newValue New value to be updated
   * @return An updated raster
   */
  public static GridCoverage2D setValue(
      GridCoverage2D raster, int band, int colX, int rowY, double newValue) {
    return setValues(raster, band, colX, rowY, 1, 1, new double[] {newValue}, false);
  }

  /**
   * Return a raster by updated the pixel specified by pixel location.
   *
   * @param raster Input raster to be edited
   * @param colX Column of the pixel
   * @param rowY Row of the pixel
   * @param newValue New value to be updated
   * @return An updated raster
   */
  public static GridCoverage2D setValue(
      GridCoverage2D raster, int colX, int rowY, double newValue) {
    return setValues(raster, 1, colX, rowY, 1, 1, new double[] {newValue}, false);
  }

  /**
   * It copies the raster given to a new WritableRaster object.
   *
   * @param raster Raster to be copied
   * @return A WritableRaster object
   */
  private static WritableRaster makeCopiedRaster(GridCoverage2D raster) {
    RenderedImage originalImage = raster.getRenderedImage();
    Raster rasterTemp = RasterUtils.getRaster(originalImage);
    Point location = rasterTemp.getBounds().getLocation();
    WritableRaster wr =
        RasterFactory.createBandedRaster(
            rasterTemp.getDataBuffer().getDataType(),
            originalImage.getWidth(),
            originalImage.getHeight(),
            raster.getNumSampleDimensions(),
            location);

    return raster.getRenderedImage().copyData(wr);
  }
}
