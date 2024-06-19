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

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.sedona.common.Constructors;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class FunctionEditorsTest extends RasterTestBase {

  @Test
  public void testSetValuesWithEmptyRaster() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values =
        new double[] {1, 1, 1, 0, 0, 0, 1, 2, 3, 3, 5, 6, 7, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0d);
    double[] newValues = new double[] {11, 12, 13, 14, 15, 16, 17, 18, 19};
    GridCoverage2D raster =
        PixelFunctionEditors.setValues(emptyRaster, 1, 2, 2, 3, 3, newValues, true);
    double[] actual = MapAlgebra.bandAsArray(raster, 1);
    double[] expected =
        new double[] {
          1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 11.0, 12.0, 13.0, 3.0, 5.0, 14.0, 15.0, 0.0, 0.0, 3.0, 0.0,
          0.0, 19.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(actual, expected, 0.0);

    raster = PixelFunctionEditors.setValues(emptyRaster, 1, 2, 2, 3, 3, newValues);
    actual = MapAlgebra.bandAsArray(raster, 1);
    expected =
        new double[] {
          1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 11.0, 12.0, 13.0, 3.0, 5.0, 14.0, 15.0, 16.0, 0.0, 3.0,
          17.0, 18.0, 19.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(actual, expected, 0.0);
  }

  @Test
  public void testSetValuesWithGeomInRaster()
      throws IOException, ParseException, FactoryException, TransformException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((236722 4204770, 243900 4204770, 243900 4197590, 236722 4197590, 236722 4204770))";
    Geometry geom = Constructors.geomFromWKT(polygon, 26918);

    GridCoverage2D result = PixelFunctionEditors.setValues(raster, 1, geom, 10, false);

    Geometry point = Constructors.geomFromWKT("POINT (243700 4197797)", 26918);
    double actual = PixelFunctions.value(result, point, 1);
    double expected = 10.0;
    assertEquals(expected, actual, 0d);

    point = Constructors.geomFromWKT("POINT (240311 4202806)", 26918);
    actual = PixelFunctions.value(result, point, 1);
    assertEquals(expected, actual, 0d);

    point = Constructors.geomFromWKT("POINT (241800 4199660)", 26918);
    actual = PixelFunctions.value(result, point, 1);
    assertEquals(expected, actual, 0d);
  }

  @Test
  public void testSetValuesWithRasterNoSRID()
      throws IOException, ParseException, FactoryException, TransformException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((-77.9148 37.9545, -77.9123 37.8898, -77.9938 37.8878, -77.9964 37.9524, -77.9148 37.9545))";
    Geometry geom = Constructors.geomFromWKT(polygon, 0);

    GridCoverage2D result = PixelFunctionEditors.setValues(raster, 1, geom, 10, false);

    Geometry point = Constructors.geomFromWKT("POINT (-77.9146 37.8916)", 0);
    double actual = PixelFunctions.value(result, point, 1);
    double expected = 10.0;
    assertEquals(expected, actual, 0d);

    point = Constructors.geomFromWKT("POINT (-77.9549 37.9357)", 0);
    actual = PixelFunctions.value(result, point, 1);
    assertEquals(expected, actual, 0d);
  }

  @Test
  public void testSetValuesWithRaster()
      throws IOException, FactoryException, ParseException, TransformException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((-8682522.873537656 4572703.890837922, -8673439.664183248 4572993.532747675, -8673155.57366801 4563873.2099182755, -8701890.325907696 4562931.7093397, -8682522.873537656 4572703.890837922))";
    Geometry geom = Constructors.geomFromWKT(polygon, 3857);

    GridCoverage2D result = PixelFunctionEditors.setValues(raster, 1, geom, 10, false);

    Geometry point = Constructors.geomFromWKT("POINT (243700 4197797)", 26918);
    double actual = PixelFunctions.value(result, point, 1);
    double expected = 10.0;
    assertEquals(expected, actual, 0d);

    point = Constructors.geomFromWKT("POINT (235749.0869 4200557.7397)", 26918);
    actual = PixelFunctions.value(result, point, 1);
    assertEquals(expected, actual, 0d);

    point = Constructors.geomFromWKT("POINT (240311 4202806)", 26918);
    actual = PixelFunctions.value(result, point, 1);
    assertEquals(expected, actual, 0d);

    point = Constructors.geomFromWKT("POINT (223670 4197650)", 26918);
    actual = PixelFunctions.value(result, point, 1);
    assertEquals(expected, actual, 0d);

    point = Constructors.geomFromWKT("POINT (243850 4197640)", 26918);
    actual = PixelFunctions.value(result, point, 1);
    assertEquals(expected, actual, 0d);
  }

  @Test
  public void testSetValuesGeomVariant()
      throws FactoryException, ParseException, TransformException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 4, 6, 1, -1, 1, -1, 0, 0, 0);
    double[] values =
        new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1);
    Geometry geom =
        Constructors.geomFromWKT("LINESTRING(1 -1, 1 -4, 2 -2, 3 -3, 4 -4, 5 -4, 6 -6)", 0);
    GridCoverage2D raster = PixelFunctionEditors.setValues(emptyRaster, 1, geom, 4235, false);
    double[] actual = MapAlgebra.bandAsArray(raster, 1);
    double[] expected =
        new double[] {
          4235.0, 0.0, 0.0, 0.0, 4235.0, 4235.0, 0.0, 0.0, 4235.0, 4235.0, 4235.0, 0.0, 4235.0, 0.0,
          0.0, 4235.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Point
    geom = Constructors.geomFromWKT("POINT(5 -5)", 0);
    raster = PixelFunctionEditors.setValues(emptyRaster, 1, geom, 35);
    actual = MapAlgebra.bandAsArray(raster, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 35.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // MultiPoint
    geom = Constructors.geomFromWKT("MULTIPOINT((2 -2), (2 -1), (3 -3), (4 -7))", 0);
    raster = PixelFunctionEditors.setValues(emptyRaster, 1, geom, 400, false);
    actual = MapAlgebra.bandAsArray(raster, 1);
    expected =
        new double[] {
          0.0, 400.0, 0.0, 0.0, 0.0, 400.0, 0.0, 0.0, 0.0, 0.0, 400.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 400.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Polygon
    geom = Constructors.geomFromWKT("POLYGON((1 -1, 3 -3, 6 -6, 4 -1, 1 -1))", 0);
    raster = PixelFunctionEditors.setValues(emptyRaster, 1, geom, 255, false);
    actual = MapAlgebra.bandAsArray(raster, 1);
    expected =
        new double[] {
          255.0, 255.0, 255.0, 0.0, 0.0, 255.0, 255.0, 255.0, 0.0, 0.0, 255.0, 255.0, 0.0, 0.0, 0.0,
          255.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Polygon bigger than raster
    geom = Constructors.geomFromWKT("POLYGON((-1 1, 3 4, 4 -4, 5 -5, 9 -9, -1 1))", 0);
    raster = PixelFunctionEditors.setValues(emptyRaster, 1, geom, 56);
    actual = MapAlgebra.bandAsArray(raster, 1);
    expected =
        new double[] {
          56.0, 56.0, 56.0, 0.0, 0.0, 56.0, 56.0, 0.0, 0.0, 0.0, 56.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testSetValueWithEmptyRaster() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values =
        new double[] {1, 1, 1, 0, 0, 0, 1, 2, 3, 3, 5, 6, 7, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0d);
    double newValue = 1777;
    GridCoverage2D raster = PixelFunctionEditors.setValue(emptyRaster, 1, 2, 2, newValue);
    double[] actual = MapAlgebra.bandAsArray(raster, 1);
    double[] expected =
        new double[] {
          1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1777.0, 2.0, 3.0, 3.0, 5.0, 6.0, 7.0, 0.0, 0.0, 3.0, 0.0,
          0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    newValue = 8723;
    raster = PixelFunctionEditors.setValue(emptyRaster, 2, 2, newValue);
    actual = MapAlgebra.bandAsArray(raster, 1);
    expected =
        new double[] {
          1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 8723.0, 2.0, 3.0, 3.0, 5.0, 6.0, 7.0, 0.0, 0.0, 3.0, 0.0,
          0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);
  }
}
