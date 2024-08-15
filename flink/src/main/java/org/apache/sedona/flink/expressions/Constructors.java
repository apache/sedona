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
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.utils.FormatUtils;
import org.apache.sedona.common.utils.GeoHashDecoder;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.gml2.GMLReader;
import org.locationtech.jts.io.kml.KMLReader;

public class Constructors {

  private static Geometry getGeometryByType(
      String geom, String inputDelimiter, GeometryType geometryType) throws ParseException {
    FileDataSplitter delimiter =
        inputDelimiter == null
            ? FileDataSplitter.CSV
            : FileDataSplitter.getFileDataSplitter(inputDelimiter);
    FormatUtils<Geometry> formatUtils = new FormatUtils<>(delimiter, false, geometryType);
    return formatUtils.readGeometry(geom);
  }

  public static class ST_Point extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("Double") Double x, @DataTypeHint("Double") Double y)
        throws ParseException {
      return org.apache.sedona.common.Constructors.point(x, y);
    }
  }

  public static class ST_PointZ extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double z)
        throws ParseException {
      return eval(x, y, z, 0);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double z,
        @DataTypeHint("Integer") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.pointZ(x, y, z, srid);
    }
  }

  public static class ST_PointM extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double m)
        throws ParseException {
      return eval(x, y, m, 0);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double m,
        @DataTypeHint("Integer") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.pointM(x, y, m, srid);
    }
  }

  public static class ST_PointZM extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double z,
        @DataTypeHint("Double") Double m)
        throws ParseException {
      return eval(x, y, z, m, 0);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double z,
        @DataTypeHint("Double") Double m,
        @DataTypeHint("Integer") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.pointZM(x, y, z, m, srid);
    }
  }

  public static class ST_PointFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String s, @DataTypeHint("String") String inputDelimiter)
        throws ParseException {
      return getGeometryByType(s, inputDelimiter, GeometryType.POINT);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String s) throws ParseException {
      return eval(s, null);
    }
  }

  public static class ST_MakePointM extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double m)
        throws ParseException {
      return org.apache.sedona.common.Constructors.makePointM(x, y, m);
    }
  }

  public static class ST_MakePoint extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("Double") Double x, @DataTypeHint("Double") Double y)
        throws ParseException {
      return org.apache.sedona.common.Constructors.makePoint(x, y, null, null);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double z)
        throws ParseException {
      return org.apache.sedona.common.Constructors.makePoint(x, y, z, null);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double x,
        @DataTypeHint("Double") Double y,
        @DataTypeHint("Double") Double z,
        @DataTypeHint("Double") Double m)
        throws ParseException {
      return org.apache.sedona.common.Constructors.makePoint(x, y, z, m);
    }
  }

  public static class ST_LineFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String lineString, @DataTypeHint("String") String inputDelimiter)
        throws ParseException {
      // The default delimiter is comma. Otherwise, use the delimiter given by the user
      return getGeometryByType(lineString, inputDelimiter, GeometryType.LINESTRING);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String lineString) throws ParseException {
      return eval(lineString, null);
    }
  }

  public static class ST_LineStringFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String lineString, @DataTypeHint("String") String inputDelimiter)
        throws ParseException {
      // The default delimiter is comma. Otherwise, use the delimiter given by the user
      return new ST_LineFromText().eval(lineString, inputDelimiter);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String lineString) throws ParseException {
      return eval(lineString, null);
    }
  }

  public static class ST_PolygonFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String polygonString, @DataTypeHint("String") String inputDelimiter)
        throws ParseException {
      // The default delimiter is comma. Otherwise, use the delimiter given by the user
      return getGeometryByType(polygonString, inputDelimiter, GeometryType.POLYGON);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String polygonString) throws ParseException {
      return eval(polygonString, null);
    }
  }

  public static class ST_PolygonFromEnvelope extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("Double") Double minX,
        @DataTypeHint("Double") Double minY,
        @DataTypeHint("Double") Double maxX,
        @DataTypeHint("Double") Double maxY) {
      Coordinate[] coordinates = new Coordinate[5];
      coordinates[0] = new Coordinate(minX, minY);
      coordinates[1] = new Coordinate(minX, maxY);
      coordinates[2] = new Coordinate(maxX, maxY);
      coordinates[3] = new Coordinate(maxX, minY);
      coordinates[4] = coordinates[0];
      GeometryFactory geometryFactory = new GeometryFactory();
      return geometryFactory.createPolygon(coordinates);
    }
  }

  private static Geometry getGeometryByFileData(String wktString, FileDataSplitter dataSplitter)
      throws ParseException {
    FormatUtils<Geometry> formatUtils = new FormatUtils<>(dataSplitter, false);
    return formatUtils.readGeometry(wktString);
  }

  public static class ST_GeomFromWKT extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wktString) throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKT(wktString, 0);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String wktString, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKT(wktString, srid);
    }
  }

  public static class ST_GeomFromEWKT extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wktString) throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromEWKT(wktString);
    }
  }

  public static class ST_GeometryFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wktString) throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKT(wktString, 0);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String wktString, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKT(wktString, srid);
    }
  }

  public static class ST_GeomFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wktString) throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKT(wktString, 0);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String wktString, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKT(wktString, srid);
    }
  }

  public static class ST_GeomFromWKB extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString) throws ParseException {
      return getGeometryByFileData(wkbString, FileDataSplitter.WKB);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb) throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKB(wkb);
    }
  }

  public static class ST_GeomFromEWKB extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString) throws ParseException {
      return getGeometryByFileData(wkbString, FileDataSplitter.WKB);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb) throws ParseException {
      return org.apache.sedona.common.Constructors.geomFromWKB(wkb);
    }
  }

  public static class ST_PointFromWKB extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString) throws ParseException {
      Geometry geometry = getGeometryByFileData(wkbString, FileDataSplitter.WKB);
      if (geometry instanceof Point) {
        return geometry;
      }
      return null; // Return null if geometry is not a Point
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString, int srid) throws ParseException {
      Geometry geometry = getGeometryByFileData(wkbString, FileDataSplitter.WKB);
      if (geometry instanceof Point) {
        geometry = Functions.setSRID(geometry, srid);
        return geometry;
      }
      return null; // Return null if geometry is not a Point
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb) throws ParseException {
      return org.apache.sedona.common.Constructors.pointFromWKB(wkb);
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb, int srid) throws ParseException {
      return org.apache.sedona.common.Constructors.pointFromWKB(wkb, srid);
    }
  }

  public static class ST_LineFromWKB extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString) throws ParseException {
      Geometry geometry = getGeometryByFileData(wkbString, FileDataSplitter.WKB);
      if (geometry instanceof LineString) {
        return geometry;
      }
      return null; // Return null if geometry is not a Point
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString, int srid) throws ParseException {
      Geometry geometry = getGeometryByFileData(wkbString, FileDataSplitter.WKB);
      if (geometry instanceof LineString) {
        geometry = Functions.setSRID(geometry, srid);
        return geometry;
      }
      return null; // Return null if geometry is not a Linestring
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb) throws ParseException {
      return org.apache.sedona.common.Constructors.lineFromWKB(wkb);
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb, int srid) throws ParseException {
      return org.apache.sedona.common.Constructors.lineFromWKB(wkb, srid);
    }
  }

  public static class ST_LinestringFromWKB extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString) throws ParseException {
      Geometry geometry = getGeometryByFileData(wkbString, FileDataSplitter.WKB);
      if (geometry instanceof LineString) {
        return geometry;
      }
      return null; // Return null if geometry is not a Linestring
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("String") String wkbString, int srid) throws ParseException {
      Geometry geometry = getGeometryByFileData(wkbString, FileDataSplitter.WKB);
      if (geometry instanceof LineString) {
        geometry = Functions.setSRID(geometry, srid);
        return geometry;
      }
      return null; // Return null if geometry is not a Linestring
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb) throws ParseException {
      return org.apache.sedona.common.Constructors.lineFromWKB(wkb);
    }

    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry eval(@DataTypeHint("Bytes") byte[] wkb, int srid) throws ParseException {
      return org.apache.sedona.common.Constructors.lineFromWKB(wkb, srid);
    }
  }

  public static class ST_GeomFromGeoJSON extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String geoJson) throws ParseException {
      return getGeometryByFileData(geoJson, FileDataSplitter.GEOJSON);
    }
  }

  public static class ST_GeomFromGeoHash extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String value, @DataTypeHint("Int") Integer precision)
        throws ParseException, GeoHashDecoder.InvalidGeoHashException {
      // The default precision is the geohash length. Otherwise, use the precision given by the user
      return GeoHashDecoder.decode(value, precision);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String value)
        throws ParseException, GeoHashDecoder.InvalidGeoHashException {
      return eval(value, null);
    }
  }

  public static class ST_PointFromGeoHash extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint("String") String value, @DataTypeHint("Int") Integer precision) {
      // The default precision is the geohash length. Otherwise, use the precision given by the user
      return org.apache.sedona.common.Constructors.pointFromGeoHash(value, precision);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String value) {
      return eval(value, null);
    }
  }

  public static class ST_GeomFromGML extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String gml) throws ParseException {
      GMLReader reader = new GMLReader();
      try {
        return reader.read(gml, new GeometryFactory());
      } catch (Exception e) {
        throw new ParseException(e);
      }
    }
  }

  public static class ST_GeomFromKML extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint("String") String kml) throws ParseException {
      return new KMLReader().read(kml);
    }
  }

  public static class ST_MPolyFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint(value = "String") String wkt, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.mPolyFromText(wkt, srid);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint(value = "String") String wkt) throws ParseException {
      return org.apache.sedona.common.Constructors.mPolyFromText(wkt, 0);
    }
  }

  public static class ST_MLineFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint(value = "String") String wkt, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.mLineFromText(wkt, srid);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint(value = "String") String wkt) throws ParseException {
      return org.apache.sedona.common.Constructors.mLineFromText(wkt, 0);
    }
  }

  public static class ST_MPointFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint(value = "String") String wkt, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.mPointFromText(wkt, srid);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint(value = "String") String wkt) throws ParseException {
      return org.apache.sedona.common.Constructors.mPointFromText(wkt, 0);
    }
  }

  public static class ST_GeomCollFromText extends ScalarFunction {
    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(
        @DataTypeHint(value = "String") String wkt, @DataTypeHint("Int") Integer srid)
        throws ParseException {
      return org.apache.sedona.common.Constructors.geomCollFromText(wkt, srid);
    }

    @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
    public Geometry eval(@DataTypeHint(value = "String") String wkt) throws ParseException {
      return org.apache.sedona.common.Constructors.geomCollFromText(wkt, 0);
    }
  }
}
