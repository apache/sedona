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
package org.apache.sedona.flink.expressions.geography;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.flink.GeographyTypeSerializer;
import org.apache.sedona.flink.GeometryTypeSerializer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

/** Constructors for the {@link Geography} type, wrapping {@link Constructors}. */
public class GeographyConstructors {

  public static class ST_GeogFromWKT extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("String") String wktString) throws ParseException {
      return Constructors.geogFromWKT(wktString, 0);
    }

    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(
        @DataTypeHint("String") String wktString, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return Constructors.geogFromWKT(wktString, srid);
    }
  }

  public static class ST_GeogFromText extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("String") String wktString) throws ParseException {
      return Constructors.geogFromWKT(wktString, 0);
    }

    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(
        @DataTypeHint("String") String wktString, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return Constructors.geogFromWKT(wktString, srid);
    }
  }

  public static class ST_GeogFromEWKT extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("String") String ewktString) throws ParseException {
      return Constructors.geogFromEWKT(ewktString);
    }
  }

  public static class ST_GeogCollFromText extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("String") String wktString) throws ParseException {
      return Constructors.geogCollFromText(wktString, 0);
    }

    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(
        @DataTypeHint("String") String wktString, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return Constructors.geogCollFromText(wktString, srid);
    }
  }

  public static class ST_GeogFromWKB extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("Bytes") byte[] wkb) throws ParseException {
      return Constructors.geogFromWKB(wkb, 0);
    }

    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("Bytes") byte[] wkb, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return Constructors.geogFromWKB(wkb, srid);
    }
  }

  public static class ST_GeogFromEWKB extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("Bytes") byte[] wkb) throws ParseException {
      return Constructors.geogFromWKB(wkb);
    }
  }

  public static class ST_GeogFromGeoHash extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(
        @DataTypeHint("String") String geoHash, @DataTypeHint("Int") Integer precision) {
      return Constructors.geogFromGeoHash(geoHash, precision);
    }

    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(@DataTypeHint("String") String geoHash) {
      return Constructors.geogFromGeoHash(geoHash, null);
    }
  }

  public static class ST_GeogToGeometry extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeometryTypeSerializer.class,
        bridgedTo = Geometry.class)
    public Geometry eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = Geography.class)
            Object o) {
      return Constructors.geogToGeometry((Geography) o);
    }
  }

  public static class ST_GeomToGeography extends ScalarFunction {
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeographyTypeSerializer.class,
        bridgedTo = Geography.class)
    public Geography eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      return Constructors.geomToGeography((Geometry) o);
    }
  }
}
