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
import java.awt.Color;
import java.util.List;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.coverage.SampleDimensionType;
import org.geotools.coverage.Category;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;

/**
 * GridSampleDimension and RenderedSampleDimension are not serializable. We need to provide a custom
 * serializer
 */
public class GridSampleDimensionSerializer extends Serializer<GridSampleDimension> {
  static final int NO_DATA_VALUE_OVERRIDE = 1;
  static final int SAMPLE_TYPE_OVERRIDE = 1 << 1;

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
        new GridSampleDimension(description, categories, scale, offset), noDataValue);
  }

  /**
   * Reconcile the nodata value declared on the wire with the one implied by the categories.
   *
   * <p>The JVM writer emits {@link RasterUtils#getNoDataValue}, which is derived from the
   * categories it also writes. Python writers replay opaque category blobs from the source raster;
   * an optional trailer marks metadata that must be rebuilt because {@code nodata=} or the output
   * storage type changed. Without that marker, a scalar equal to the minimum of a range-valued
   * NODATA category must be treated as an ordinary JVM round trip and preserve the range.
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
    boolean noDataValueOverride = (declared.metadataOverrideFlags & NO_DATA_VALUE_OVERRIDE) != 0;
    boolean sampleTypeOverride = (declared.metadataOverrideFlags & SAMPLE_TYPE_OVERRIDE) != 0;
    if (sampleTypeOverride) {
      // with_bands() changed the Java storage type. Its opaque source categories no longer
      // describe the output pixels, so replace them with the standard full range for that type
      // before applying the output's NODATA declaration.
      sampleDimension = retypeSampleDimension(sampleDimension, sampleDimensionType);
    }
    double categoryNoDataValue = RasterUtils.getNoDataValue(sampleDimension);
    if (Double.isNaN(declared.noDataValue)) {
      if (Double.isNaN(categoryNoDataValue)) {
        // Neither side declares a nodata value; nothing to do. This is the path for rasters
        // serialized before the declared value was honored.
        return sampleDimension;
      }
      // NaN was declared against categories that carry a real nodata value, which only happens
      // when a writer asked for "no nodata" over replayed categories. Honour that by dropping
      // the category, otherwise the source's value silently survives into the output.
      return RasterUtils.removeNoDataValue(sampleDimension);
    }
    if (!noDataValueOverride
        && !sampleTypeOverride
        && Double.compare(categoryNoDataValue, declared.noDataValue) == 0) {
      // The unmarked scalar was written from these categories by the JVM. In particular, it is
      // only the minimum of a range-valued NODATA category, not a request to collapse that range.
      return sampleDimension;
    }
    // The shared RasterUtils helper is intentionally idempotent when the scalar matches an
    // existing range-valued NODATA category. Strip that category here so an authoritative Python
    // override, or any genuine wire/category mismatch, is rebuilt as the declared singleton.
    GridSampleDimension stripped = RasterUtils.removeNoDataValue(sampleDimension);
    return RasterUtils.createSampleDimensionWithNoDataValue(
        stripped, declared.noDataValue, sampleDimensionType);
  }

  /** Rebuild a source sample dimension with a default quantitative category for the output type. */
  private static GridSampleDimension retypeSampleDimension(
      GridSampleDimension sampleDimension, SampleDimensionType sampleDimensionType) {
    String description = sampleDimension.getDescription().toString();
    Category data =
        new Category(description, (Color[]) null, TypeMap.getRange(sampleDimensionType), true);
    return new GridSampleDimension(
        description,
        new Category[] {data},
        sampleDimension.getScale(),
        sampleDimension.getOffset());
  }

  /** A deserialized sample dimension together with the nodata value declared on the wire. */
  public static final class DeclaredSampleDimension {
    public final GridSampleDimension sampleDimension;
    public final double noDataValue;
    public final int metadataOverrideFlags;

    DeclaredSampleDimension(GridSampleDimension sampleDimension, double noDataValue) {
      this(sampleDimension, noDataValue, 0);
    }

    DeclaredSampleDimension(
        GridSampleDimension sampleDimension, double noDataValue, int metadataOverrideFlags) {
      this.sampleDimension = sampleDimension;
      this.noDataValue = noDataValue;
      this.metadataOverrideFlags = metadataOverrideFlags;
    }
  }
}
