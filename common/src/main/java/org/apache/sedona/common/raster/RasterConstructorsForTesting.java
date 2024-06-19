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

import com.sun.media.imageioimpl.common.BogusColorSpace;
import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DirectColorModel;
import java.awt.image.IndexColorModel;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.SinglePixelPackedSampleModel;
import java.awt.image.WritableRaster;
import java.util.Arrays;
import javax.media.jai.RasterFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;

/** Raster constructor for testing the Python implementation of raster deserializer. */
public class RasterConstructorsForTesting {
  private RasterConstructorsForTesting() {}

  public static GridCoverage2D makeRasterForTesting(
      int numBand,
      String bandDataType,
      String sampleModelType,
      int widthInPixel,
      int heightInPixel,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY,
      int srid) {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      // Create the CRS from the srid
      // Longitude first, Latitude second
      crs = FunctionsGeoTools.sridToCRS(srid);
    }

    // Create a new raster with certain pixel values
    WritableRaster raster =
        createRasterWithSampleModel(
            sampleModelType, bandDataType, widthInPixel, heightInPixel, numBand);
    for (int k = 0; k < numBand; k++) {
      for (int y = 0; y < heightInPixel; y++) {
        for (int x = 0; x < widthInPixel; x++) {
          double value = k + y * widthInPixel + x;
          raster.setSample(x, y, k, value);
        }
      }
    }

    MathTransform transform =
        new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
            PixelInCell.CELL_CORNER,
            transform,
            crs,
            null);

    int rasterDataType = raster.getDataBuffer().getDataType();
    ColorModel colorModel;
    if (!sampleModelType.contains("Packed")) {
      final ColorSpace cs = new BogusColorSpace(numBand);
      final int[] nBits = new int[numBand];
      Arrays.fill(nBits, DataBuffer.getDataTypeSize(rasterDataType));
      colorModel =
          new ComponentColorModel(cs, nBits, false, true, Transparency.OPAQUE, rasterDataType);
    } else if (sampleModelType.equals("SinglePixelPackedSampleModel")) {
      colorModel = new DirectColorModel(32, 0x0F, (0x0F) << 4, (0x0F) << 8, (0x0F) << 12);
    } else if (sampleModelType.equals("MultiPixelPackedSampleModel")) {
      byte[] arr = new byte[16];
      for (int k = 0; k < 16; k++) {
        arr[k] = (byte) (k * 16);
      }
      colorModel = new IndexColorModel(4, arr.length, arr, arr, arr);
    } else {
      throw new IllegalArgumentException("Unknown sample model type: " + sampleModelType);
    }

    final RenderedImage image = new BufferedImage(colorModel, raster, false, null);
    GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
    return gridCoverageFactory.create("genericCoverage", image, gridGeometry, null, null, null);
  }

  private static WritableRaster createRasterWithSampleModel(
      String sampleModelType,
      String bandDataType,
      int widthInPixel,
      int heightInPixel,
      int numBand) {
    int dataType = RasterUtils.getDataTypeCode(bandDataType);

    // Create raster according to sample model type
    WritableRaster raster;
    switch (sampleModelType) {
      case "BandedSampleModel":
        raster =
            RasterFactory.createBandedRaster(dataType, widthInPixel, heightInPixel, numBand, null);
        break;
      case "PixelInterleavedSampleModel":
        {
          int scanlineStride = widthInPixel * numBand;
          int[] bandOffsets = new int[numBand];
          for (int i = 0; i < numBand; i++) {
            bandOffsets[i] = i;
          }
          SampleModel sm =
              new PixelInterleavedSampleModel(
                  dataType, widthInPixel, heightInPixel, numBand, scanlineStride, bandOffsets);
          raster = RasterFactory.createWritableRaster(sm, null);
          break;
        }
      case "PixelInterleavedSampleModelComplex":
        {
          int pixelStride = numBand + 2;
          int scanlineStride = widthInPixel * pixelStride + 5;
          int[] bandOffsets = new int[numBand];
          for (int i = 0; i < numBand; i++) {
            bandOffsets[i] = i;
          }
          ArrayUtils.shuffle(bandOffsets);
          SampleModel sm =
              new PixelInterleavedSampleModel(
                  dataType, widthInPixel, heightInPixel, pixelStride, scanlineStride, bandOffsets);
          raster = RasterFactory.createWritableRaster(sm, null);
          break;
        }
      case "ComponentSampleModel":
        {
          int pixelStride = numBand + 1;
          int scanlineStride = widthInPixel * pixelStride + 5;
          int[] bankIndices = new int[numBand];
          for (int i = 0; i < numBand; i++) {
            bankIndices[i] = i;
          }
          ArrayUtils.shuffle(bankIndices);
          int[] bandOffsets = new int[numBand];
          for (int i = 0; i < numBand; i++) {
            bandOffsets[i] = (int) (Math.random() * widthInPixel);
          }
          SampleModel sm =
              new ComponentSampleModel(
                  dataType,
                  widthInPixel,
                  heightInPixel,
                  pixelStride,
                  scanlineStride,
                  bankIndices,
                  bandOffsets);
          raster = RasterFactory.createWritableRaster(sm, null);
          break;
        }
      case "SinglePixelPackedSampleModel":
        {
          if (dataType != DataBuffer.TYPE_INT) {
            throw new IllegalArgumentException(
                "only supports creating SinglePixelPackedSampleModel with int data type");
          }
          if (numBand != 4) {
            throw new IllegalArgumentException(
                "only supports creating SinglePixelPackedSampleModel with 4 bands");
          }
          int bitsPerBand = 4;
          int scanlineStride = widthInPixel + 5;
          int[] bitMasks = new int[numBand];
          int baseMask = (1 << bitsPerBand) - 1;
          for (int i = 0; i < numBand; i++) {
            bitMasks[i] = baseMask << (i * bitsPerBand);
          }
          SampleModel sm =
              new SinglePixelPackedSampleModel(
                  dataType, widthInPixel, heightInPixel, scanlineStride, bitMasks);
          raster = RasterFactory.createWritableRaster(sm, null);
          break;
        }
      case "MultiPixelPackedSampleModel":
        {
          if (dataType != DataBuffer.TYPE_BYTE) {
            throw new IllegalArgumentException(
                "only supports creating MultiPixelPackedSampleModel with byte data type");
          }
          if (numBand != 1) {
            throw new IllegalArgumentException(
                "only supports creating MultiPixelPackedSampleModel with 1 band");
          }
          int numberOfBits = 4;
          int scanlineStride = widthInPixel * numberOfBits / 8 + 2;
          SampleModel sm =
              new MultiPixelPackedSampleModel(
                  dataType, widthInPixel, heightInPixel, numberOfBits, scanlineStride, 80);
          raster = RasterFactory.createWritableRaster(sm, null);
          break;
        }
      default:
        throw new IllegalArgumentException("Unknown sample model type: " + sampleModelType);
    }

    return raster;
  }
}
