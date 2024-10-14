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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.awt.image.ComponentSampleModel;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.SinglePixelPackedSampleModel;
import java.io.IOException;
import org.apache.sedona.common.raster.serde.Serde;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;

public class RasterConstructorsForTestingTest extends RasterTestBase {
  @Test
  public void testBandedRaster() {
    GridCoverage2D raster = makeRasterWithFallbackParams(4, "I", "BandedSampleModel", 4, 3);
    assertTrue(raster.getRenderedImage().getSampleModel() instanceof ComponentSampleModel);
    testSerde(raster);
  }

  @Test
  public void testPixelInterleavedRaster() {
    GridCoverage2D raster =
        makeRasterWithFallbackParams(4, "I", "PixelInterleavedSampleModel", 4, 3);
    assertTrue(raster.getRenderedImage().getSampleModel() instanceof PixelInterleavedSampleModel);
    testSerde(raster);
    raster = makeRasterWithFallbackParams(4, "I", "PixelInterleavedSampleModelComplex", 4, 3);
    assertTrue(raster.getRenderedImage().getSampleModel() instanceof PixelInterleavedSampleModel);
    testSerde(raster);
  }

  @Test
  public void testComponentSampleModel() {
    GridCoverage2D raster = makeRasterWithFallbackParams(4, "I", "ComponentSampleModel", 4, 3);
    assertTrue(raster.getRenderedImage().getSampleModel() instanceof ComponentSampleModel);
    testSerde(raster);
  }

  @Test
  public void testSinglePixelPackedSampleModel() {
    GridCoverage2D raster =
        makeRasterWithFallbackParams(4, "I", "SinglePixelPackedSampleModel", 4, 3);
    assertTrue(raster.getRenderedImage().getSampleModel() instanceof SinglePixelPackedSampleModel);
    testSerde(raster);
  }

  @Test
  public void testMultiPixelPackedSampleModel() {
    GridCoverage2D raster =
        makeRasterWithFallbackParams(1, "B", "MultiPixelPackedSampleModel", 4, 3);
    assertTrue(raster.getRenderedImage().getSampleModel() instanceof MultiPixelPackedSampleModel);
    testSerde(raster);

    raster = makeRasterWithFallbackParams(1, "B", "MultiPixelPackedSampleModel", 21, 8);
    Raster r = RasterUtils.getRaster(raster.getRenderedImage());
    assertTrue(r.getSampleModel() instanceof MultiPixelPackedSampleModel);
    assertEquals(21, r.getWidth());
    assertEquals(8, r.getHeight());
    for (int y = 0; y < r.getHeight(); y++) {
      for (int x = 0; x < r.getWidth(); x++) {
        assertEquals((x + y * 21) % 16, r.getSample(x, y, 0));
      }
    }
  }

  private static GridCoverage2D makeRasterWithFallbackParams(
      int numBand, String bandDataType, String sampleModelType, int width, int height) {
    return RasterConstructorsForTesting.makeRasterForTesting(
        numBand, bandDataType, sampleModelType, width, height, 0.5, -0.5, 1, -1, 0, 0, 3857);
  }

  private static void testSerde(GridCoverage2D raster) {
    try {
      byte[] bytes = Serde.serialize(raster);
      GridCoverage2D roundTripRaster = Serde.deserialize(bytes);
      assertNotNull(roundTripRaster);
      assertEquals(raster.getNumSampleDimensions(), roundTripRaster.getNumSampleDimensions());

      assertEquals(raster.getGridGeometry(), roundTripRaster.getGridGeometry());
      int width = raster.getRenderedImage().getWidth();
      int height = raster.getRenderedImage().getHeight();
      Raster r = RasterUtils.getRaster(raster.getRenderedImage());
      for (int b = 0; b < raster.getNumSampleDimensions(); b++) {
        for (int y = 0; y < height; y++) {
          for (int x = 0; x < width; x++) {
            double value = b + y * width + x;
            assertEquals(value, r.getSampleDouble(x, y, b), 0.0001);
          }
        }
      }

    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
