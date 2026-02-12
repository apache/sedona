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
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.sedona.flink.GeometryTypeSerializer;
import org.locationtech.jts.geom.Geometry;

/**
 * Flink scalar functions that use proj4sedona for CRS transformations. This provides better
 * performance and broader CRS format support compared to GeoTools.
 */
public class FunctionsProj4 {

  /**
   * ST_Transform using proj4sedona library for coordinate reference system transformations.
   *
   * <p>Supports the following CRS formats:
   *
   * <ul>
   *   <li>EPSG codes: "EPSG:4326", "epsg:3857"
   *   <li>PROJ strings: "+proj=longlat +datum=WGS84"
   *   <li>WKT1 and WKT2 CRS definitions
   *   <li>PROJJSON format
   * </ul>
   *
   * <p>Note: The 4-argument version accepts a 'lenient' parameter for API compatibility, but this
   * parameter is ignored by proj4sedona as it always attempts the best available transformation.
   */
  public static class ST_Transform extends ScalarFunction {

    /**
     * Transform geometry using the geometry's SRID as source CRS.
     *
     * @param o the geometry to transform (must have SRID set)
     * @param targetCRS the target CRS (EPSG code, PROJ string, WKT, or PROJJSON)
     * @return the transformed geometry with updated SRID
     */
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeometryTypeSerializer.class,
        bridgedTo = Geometry.class)
    public Geometry eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o,
        @DataTypeHint("String") String targetCRS) {
      Geometry geom = (Geometry) o;
      return org.apache.sedona.common.FunctionsProj4.transform(geom, targetCRS);
    }

    /**
     * Transform geometry from source CRS to target CRS.
     *
     * @param o the geometry to transform
     * @param sourceCRS the source CRS (EPSG code, PROJ string, WKT, or PROJJSON)
     * @param targetCRS the target CRS (EPSG code, PROJ string, WKT, or PROJJSON)
     * @return the transformed geometry with updated SRID
     */
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeometryTypeSerializer.class,
        bridgedTo = Geometry.class)
    public Geometry eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o,
        @DataTypeHint("String") String sourceCRS,
        @DataTypeHint("String") String targetCRS) {
      Geometry geom = (Geometry) o;
      return org.apache.sedona.common.FunctionsProj4.transform(geom, sourceCRS, targetCRS);
    }

    /**
     * Transform geometry from source CRS to target CRS. The lenient parameter is accepted for API
     * compatibility but is ignored by proj4sedona.
     *
     * @param o the geometry to transform
     * @param sourceCRS the source CRS (EPSG code, PROJ string, WKT, or PROJJSON)
     * @param targetCRS the target CRS (EPSG code, PROJ string, WKT, or PROJJSON)
     * @param lenient ignored by proj4sedona (kept for API compatibility)
     * @return the transformed geometry with updated SRID
     */
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeometryTypeSerializer.class,
        bridgedTo = Geometry.class)
    public Geometry eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o,
        @DataTypeHint("String") String sourceCRS,
        @DataTypeHint("String") String targetCRS,
        @DataTypeHint("Boolean") Boolean lenient) {
      Geometry geom = (Geometry) o;
      return org.apache.sedona.common.FunctionsProj4.transform(geom, sourceCRS, targetCRS, lenient);
    }
  }
}
