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
import org.geotools.api.coverage.SampleDimensionType;
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
    return readWithDeclaredNoDataValue(kryo, input).sampleDimension;
  }

  /**
   * Read a sample dimension, keeping the nodata value declared on the wire alongside it.
   *
   * <p>The declared value cannot be applied here. Python writers (InDbSedonaRaster.with_bands)
   * replay category blobs taken from a source raster while declaring the nodata the caller actually
   * asked for, and encoding that value requires knowing the sample dimension type of the pixels it
   * will describe — which are only deserialized later, and whose type may be wider than the
   * source's. {@link #reconcileNoDataValue} performs the reconciliation once the image is
   * available.
   */
  public static DeclaredSampleDimension readWithDeclaredNoDataValue(Kryo kryo, Input input) {
    String description = KryoUtil.readUTF8String(input);
    double offset = input.readDouble();
    double scale = input.readDouble();
    double noDataValue = input.readDouble();
    input.readInt(); // skip the length of the next object
    Category[] categories = kryo.readObject(input, Category[].class);
    return new DeclaredSampleDimension(
        new GridSampleDimension(description, categories, offset, scale), noDataValue);
  }

  /**
   * Reconcile the nodata value declared on the wire with the one implied by the categories. The
   * declared value wins: it is what the writer asked for, whereas the categories may have been
   * replayed unchanged from a different raster.
   *
   * <p>The JVM writer emits {@link RasterUtils#getNoDataValue}, which is derived from the
   * categories it also writes, so the two always agree for JVM-to-JVM round trips and both branches
   * below return the argument unchanged.
   *
   * @param declared the sample dimension and the nodata value declared alongside it
   * @param sampleDimensionType the type of the pixels this sample dimension describes, used to
   *     encode the nodata value; a byte source widened to double by a UDF must encode against
   *     double, not byte
   * @return the original sample dimension, or a new one carrying the declared nodata value
   */
  public static GridSampleDimension reconcileNoDataValue(
      DeclaredSampleDimension declared, SampleDimensionType sampleDimensionType) {
    GridSampleDimension sampleDimension = declared.sampleDimension;
    if (Double.isNaN(declared.noDataValue)) {
      if (Double.isNaN(RasterUtils.getNoDataValue(sampleDimension))) {
        // Neither side declares a nodata value; nothing to do. This is the path for rasters
        // serialized before the declared value was honored.
        return sampleDimension;
      }
      // NaN was declared against categories that carry a real nodata value, which only happens
      // when a writer asked for "no nodata" over replayed categories. Honour that by dropping
      // the category, otherwise the source's value silently survives into the output.
      return RasterUtils.removeNoDataValue(sampleDimension);
    }
    return RasterUtils.createSampleDimensionWithNoDataValue(
        sampleDimension, declared.noDataValue, sampleDimensionType);
  }

  /** A deserialized sample dimension together with the nodata value declared on the wire. */
  public static final class DeclaredSampleDimension {
    public final GridSampleDimension sampleDimension;
    public final double noDataValue;

    DeclaredSampleDimension(GridSampleDimension sampleDimension, double noDataValue) {
      this.sampleDimension = sampleDimension;
      this.noDataValue = noDataValue;
    }
  }
}
