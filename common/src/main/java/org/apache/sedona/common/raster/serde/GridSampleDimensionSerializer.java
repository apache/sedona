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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.List;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.Category;
import org.geotools.coverage.GridSampleDimension;

/**
 * GridSampleDimension and RenderedSampleDimension are not serializable. We need to provide a custom
 * serializer
 */
public class GridSampleDimensionSerializer extends Serializer<GridSampleDimension> {
  @Override
  public void write(Kryo kryo, Output output, GridSampleDimension sampleDimension) {
    String description = sampleDimension.getDescription().toString();
    List<Category> categories = sampleDimension.getCategories();
    double offset = sampleDimension.getOffset();
    double scale = sampleDimension.getScale();
    double noDataValue = RasterUtils.getNoDataValue(sampleDimension);
    KryoUtil.writeUTF8String(output, description);
    output.writeDouble(offset);
    output.writeDouble(scale);
    output.writeDouble(noDataValue); // for interoperability with Python RasterType.
    KryoUtil.writeObjectWithLength(kryo, output, categories.toArray());
  }

  /**
   * Skip past a serialized GridSampleDimension without fully deserializing it. Reads the UTF-8
   * description, skips offset+scale+nodata (24 bytes), then skips the length-prefixed categories
   * blob.
   */
  public static void skip(Input input) {
    // Skip description (UTF-8 string: int length + length bytes)
    KryoUtil.skipUTF8String(input);
    // Skip offset (8B) + scale (8B) + noDataValue (8B) = 24 bytes
    input.skip(24);
    // Skip categories (length-prefixed via writeObjectWithLength:
    // the length prefix is a 4-byte int giving the number of bytes that follow)
    int categoriesLength = input.readInt();
    input.skip(categoriesLength);
  }

  @Override
  public GridSampleDimension read(Kryo kryo, Input input, Class aClass) {
    String description = KryoUtil.readUTF8String(input);
    double offset = input.readDouble();
    double scale = input.readDouble();
    double noDataValue = input.readDouble();
    input.readInt(); // skip the length of the next object
    Category[] categories = kryo.readObject(input, Category[].class);
    GridSampleDimension sampleDimension =
        new GridSampleDimension(description, categories, offset, scale);
    return reconcileNoDataValue(sampleDimension, noDataValue);
  }

  /**
   * Reconcile the declared nodata value with the one implied by the categories. Python writers
   * (InDbSedonaRaster.with_bands) replay category blobs taken from a source raster, so the
   * categories may still describe the source's nodata while the declared value is the one the
   * caller asked for. The declared value wins.
   *
   * <p>The JVM writer emits {@link RasterUtils#getNoDataValue}, which is derived from the
   * categories it also writes, so the two always agree for JVM-to-JVM round trips.
   * createSampleDimensionWithNoDataValue returns its argument unchanged in that case, making this a
   * no-op there.
   *
   * @param sampleDimension the sample dimension rebuilt from the serialized categories
   * @param noDataValue the nodata value declared on the wire, or NaN when none was declared
   * @return the original sample dimension, or a new one carrying the declared nodata value
   */
  private static GridSampleDimension reconcileNoDataValue(
      GridSampleDimension sampleDimension, double noDataValue) {
    if (Double.isNaN(noDataValue)) {
      // No nodata declared, so there is nothing to reconcile against. Leave whatever the categories
      // say: NaN cannot be expressed as a nodata category, and this is also the path taken by
      // rasters serialized before the declared value was honored.
      return sampleDimension;
    }
    return RasterUtils.createSampleDimensionWithNoDataValue(sampleDimension, noDataValue);
  }
}
