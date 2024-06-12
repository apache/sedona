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
package org.apache.sedona.common.raster.serde;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import org.apache.sedona.common.raster.RasterConstructors;
import org.apache.sedona.common.raster.RasterTestBase;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.junit.Test;
import org.opengis.referencing.FactoryException;

public class SerdeTest extends RasterTestBase {

  private static final String[] testFilePaths = {
    resourceFolder + "/raster/test1.tiff",
    resourceFolder + "/raster/test2.tiff",
    resourceFolder + "/raster/test3.tif",
    resourceFolder + "/raster_geotiff_color/FAA_UTM18N_NAD83.tif"
  };

  @Test
  public void testRoundTripSerdeSingleBandRaster() throws IOException, ClassNotFoundException {
    testRoundTrip(oneBandRaster);
  }

  @Test
  public void testRoundTripSerdeMultiBandRaster() throws IOException, ClassNotFoundException {
    testRoundTrip(multiBandRaster);
  }

  @Test
  public void testInDbRaster() throws IOException, ClassNotFoundException {
    for (String testFilePath : testFilePaths) {
      GeoTiffReader reader = new GeoTiffReader(new File(testFilePath));
      GridCoverage2D raster = reader.read(null);
      testRoundTrip(raster);
    }
  }

  @Test
  public void testNorthPoleRaster() throws IOException, ClassNotFoundException, FactoryException {
    // If we are not using non-strict mode to serializing CRS, this will raise an exception:
    // org.geotools.referencing.wkt.UnformattableObjectException: This "AxisDirection" object is too
    // complex for
    // WKT syntax.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(
            1, "B", 256, 256, -345000.000, 345000.000, 2000, -2000, 0, 0, 3996);
    testRoundTrip(raster);
  }

  private GridCoverage2D testRoundTrip(GridCoverage2D raster)
      throws IOException, ClassNotFoundException {
    return testRoundTrip(raster, 10);
  }

  private GridCoverage2D testRoundTrip(GridCoverage2D raster, int density)
      throws IOException, ClassNotFoundException {
    byte[] bytes = Serde.serialize(raster);
    GridCoverage2D roundTripRaster = Serde.deserialize(bytes);
    assertNotNull(roundTripRaster);
    assertSameCoverage(raster, roundTripRaster, density);
    bytes = Serde.serialize(roundTripRaster);
    roundTripRaster = Serde.deserialize(bytes);
    assertSameCoverage(raster, roundTripRaster, density);
    return roundTripRaster;
  }
}
