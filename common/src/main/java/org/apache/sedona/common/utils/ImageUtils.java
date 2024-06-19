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

import java.awt.image.Raster;
import java.awt.image.WritableRaster;

/** Utility functions for image processing. */
public class ImageUtils {
  private ImageUtils() {}

  /**
   * Copy a raster to another raster, with padding if necessary.
   *
   * @param sourceRaster the source raster
   * @param sourceBand the source band
   * @param destRaster the destination raster, which must not be smaller than the source raster
   * @param destBand the destination band
   * @param padValue the padding value, or NaN if no padding is needed
   */
  public static void copyRasterWithPadding(
      Raster sourceRaster,
      int sourceBand,
      WritableRaster destRaster,
      int destBand,
      double padValue) {
    int destWidth = destRaster.getWidth();
    int destHeight = destRaster.getHeight();
    int destMinX = destRaster.getMinX();
    int destMinY = destRaster.getMinY();
    int sourceWidth = sourceRaster.getWidth();
    int sourceHeight = sourceRaster.getHeight();
    int sourceMinX = sourceRaster.getMinX();
    int sourceMinY = sourceRaster.getMinY();
    if (sourceWidth > destWidth || sourceHeight > destHeight) {
      throw new IllegalArgumentException("Source raster is larger than destination raster");
    }

    // Copy the source raster to the destination raster
    double[] samples =
        sourceRaster.getSamples(
            sourceMinX, sourceMinY, sourceWidth, sourceHeight, sourceBand, (double[]) null);
    destRaster.setSamples(destMinX, destMinY, sourceWidth, sourceHeight, destBand, samples);

    // Pad the right edge
    for (int y = destMinY; y < sourceHeight + destMinY; y++) {
      for (int x = sourceWidth + destMinX; x < destWidth + destMinX; x++) {
        destRaster.setSample(x, y, destBand, padValue);
      }
    }
    // Pad the bottom edge
    for (int y = sourceHeight + destMinY; y < destHeight + destMinY; y++) {
      for (int x = destMinX; x < destWidth + destMinX; x++) {
        destRaster.setSample(x, y, destBand, padValue);
      }
    }
  }
}
