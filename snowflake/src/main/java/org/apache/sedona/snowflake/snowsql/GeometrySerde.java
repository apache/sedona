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
package org.apache.sedona.snowflake.snowsql;

import java.util.Arrays;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.utils.FormatUtils;
import org.apache.sedona.common.utils.GeomUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

public class GeometrySerde {

  public static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public static byte[] serialize(Geometry geom) {
    return Functions.asEWKB(geom);
  }

  public static String serGeoJson(Geometry geom) {
    return Functions.asGeoJson(geom);
  }

  public static String serGeoJson(Geometry[] geoms) {
    return Functions.asGeoJson(Functions.createMultiGeometry(geoms));
  }

  public static byte[] serialize(Geometry[] geoms) {
    return serialize(Functions.createMultiGeometry(geoms));
  }

  public static Geometry deserialize(byte[] bytes) {
    try {
      return Constructors.geomFromWKB(bytes);
    } catch (ParseException e) {
      String msg =
          String.format(
              "Failed to parse WKB(printed through Arrays.toString(bytes)): %s, error: %s",
              Arrays.toString(bytes), e.getMessage());
      throw new IllegalArgumentException(msg);
    }
  }

  public static Geometry deserGeoJson(String geoJson) {
    FormatUtils<Geometry> formatUtils = new FormatUtils<>(FileDataSplitter.GEOJSON, false);
    try {
      return formatUtils.readGeometry(geoJson);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static Geometry[] deserGeoJson2List(String geoJson) {
    FormatUtils<Geometry> formatUtils = new FormatUtils<>(FileDataSplitter.GEOJSON, false);
    Geometry geom;
    try {
      geom = formatUtils.readGeometry(geoJson);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    assert geom instanceof GeometryCollection;
    return GeomUtils.getSubGeometries(geom);
  }

  public static Geometry[] deserialize2List(byte[] bytes) {
    Geometry geom = GeometrySerde.deserialize(bytes);
    assert geom instanceof GeometryCollection;
    return GeomUtils.getSubGeometries(geom);
  }
}
