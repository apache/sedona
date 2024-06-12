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

import java.awt.Color;
import org.geotools.coverage.Category;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.util.NumberRange;
import org.junit.Assert;
import org.junit.Test;

public class RasterUtilsTest {
  @Test
  public void testNoDataValue() {
    GridSampleDimension band = new GridSampleDimension("test");
    Assert.assertTrue(Double.isNaN(RasterUtils.getNoDataValue(band)));
    band = RasterUtils.createSampleDimensionWithNoDataValue(band, 100);
    Assert.assertEquals(100, RasterUtils.getNoDataValue(band), 1e-9);
    Assert.assertEquals(100, band.getNoDataValues()[0], 1e-9);
    band = RasterUtils.createSampleDimensionWithNoDataValue("test", 200);
    Assert.assertEquals(200, RasterUtils.getNoDataValue(band), 1e-9);
    Assert.assertEquals(200, band.getNoDataValues()[0], 1e-9);
    band = RasterUtils.removeNoDataValue(band);
    Assert.assertTrue(Double.isNaN(RasterUtils.getNoDataValue(band)));
  }

  @Test
  public void testNoDataValueUsingComplexCategoryList() {
    Category[] categories = {
      new Category("C0", new Color(87, 154, 60, 255), 1),
      new Category("C1", new Color(0, 0, 255, 255), 2),
      new Category("C2", new Color(255, 255, 255, 255), 3),
      new Category(Category.NODATA.getName(), new Color(0, 0, 0, 0), 10),
      new Category("GrayScale", (Color) null, NumberRange.create(100, 200))
    };
    GridSampleDimension band = new GridSampleDimension("test", categories, null);
    Assert.assertEquals(10, RasterUtils.getNoDataValue(band), 1e-9);
    Assert.assertEquals(5, band.getCategories().size());

    // Remove no data value from this band removes the NODATA category
    GridSampleDimension band2 = RasterUtils.removeNoDataValue(band);
    Assert.assertTrue(Double.isNaN(RasterUtils.getNoDataValue(band2)));
    Assert.assertEquals(4, band2.getCategories().size());

    // Create a new band with no data value from band2 adds a NODATA category back
    GridSampleDimension band3 = RasterUtils.createSampleDimensionWithNoDataValue(band2, 20);
    Assert.assertEquals(20, RasterUtils.getNoDataValue(band3), 1e-9);
    Assert.assertEquals(5, band3.getCategories().size());

    // Create a new band with no data value from band replaces the NODATA category
    GridSampleDimension band4 = RasterUtils.createSampleDimensionWithNoDataValue(band, 20);
    Assert.assertEquals(20, RasterUtils.getNoDataValue(band4), 1e-9);
    Assert.assertEquals(5, band4.getCategories().size());

    // If the new no data value falls within the range of an existing qualitative category, the
    // category is replaced
    // with NODATA category.
    GridSampleDimension band5 = RasterUtils.createSampleDimensionWithNoDataValue(band, 1);
    Assert.assertEquals(1, RasterUtils.getNoDataValue(band5), 1e-9);
    Assert.assertEquals(4, band5.getCategories().size());
    Assert.assertEquals(Category.NODATA.getName(), band5.getCategory(1).getName());
    Assert.assertEquals("C1", band5.getCategory(2).getName().toString());
    Assert.assertEquals("C2", band5.getCategory(3).getName().toString());
    Assert.assertNull(band5.getCategory(10));
    Assert.assertEquals("GrayScale", band5.getCategory(100).getName().toString());

    // If the new no data value falls within the range of an existing quantitative category, the
    // category is split
    // into two categories.
    GridSampleDimension band6 = RasterUtils.createSampleDimensionWithNoDataValue(band, 150);
    Assert.assertEquals(150, RasterUtils.getNoDataValue(band6), 1e-9);
    Assert.assertEquals(6, band6.getCategories().size());
    Assert.assertEquals("GrayScale", band5.getCategory(100).getName().toString());
    Assert.assertEquals("GrayScale", band5.getCategory(149).getName().toString());
    Assert.assertEquals("GrayScale", band5.getCategory(151).getName().toString());
    Assert.assertEquals("GrayScale", band5.getCategory(200).getName().toString());
  }

  @Test
  public void testNoDataValueUsingQuantitativeCategory() {
    Category[] categories = {
      new Category(Category.NODATA.getName(), new Color(0, 0, 0, 0), 10),
      new Category("GrayScale", (Color) null, NumberRange.create(100, 200))
    };
    GridSampleDimension band = new GridSampleDimension("test", categories, null);

    GridSampleDimension band2 = RasterUtils.createSampleDimensionWithNoDataValue(band, 100);
    Assert.assertEquals(100, RasterUtils.getNoDataValue(band2), 1e-9);
    Assert.assertEquals(2, band2.getCategories().size());
    Assert.assertEquals("GrayScale", band2.getCategory(101).getName().toString());
    Assert.assertEquals("GrayScale", band2.getCategory(200).getName().toString());

    GridSampleDimension band3 = RasterUtils.createSampleDimensionWithNoDataValue(band, 200);
    Assert.assertEquals(200, RasterUtils.getNoDataValue(band3), 1e-9);
    Assert.assertEquals(2, band3.getCategories().size());
    Assert.assertEquals("GrayScale", band3.getCategory(100).getName().toString());
    Assert.assertEquals("GrayScale", band3.getCategory(199).getName().toString());
  }

  @Test
  public void testNoDataValueUsingFloatQuantitativeCategory() {
    Category[] categories = {
      new Category(Category.NODATA.getName(), new Color(0, 0, 0, 0), 10.0),
      new Category("GrayScale", (Color) null, NumberRange.create(100.0, 200.0))
    };
    GridSampleDimension band = new GridSampleDimension("test", categories, null);

    GridSampleDimension band2 = RasterUtils.createSampleDimensionWithNoDataValue(band, 100);
    Assert.assertEquals(100, RasterUtils.getNoDataValue(band2), 1e-9);
    Assert.assertEquals(2, band2.getCategories().size());
    Assert.assertEquals("GrayScale", band2.getCategory(100.001).getName().toString());
    Assert.assertEquals("GrayScale", band2.getCategory(200).getName().toString());

    GridSampleDimension band3 = RasterUtils.createSampleDimensionWithNoDataValue(band, 200);
    Assert.assertEquals(200, RasterUtils.getNoDataValue(band3), 1e-9);
    Assert.assertEquals(2, band3.getCategories().size());
    Assert.assertEquals("GrayScale", band3.getCategory(100).getName().toString());
    Assert.assertEquals("GrayScale", band3.getCategory(199.999).getName().toString());
  }
}
