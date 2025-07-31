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
package org.apache.sedona.common;

import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.utils.S2GeographyFormatUtils;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;

public class S2Constructors {

  public static S2Geography geogFromWKT(String wkt, int srid) throws ParseException {
    if (wkt == null) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static S2Geography geogFromEWKT(String ewkt) throws ParseException {
    if (ewkt == null) {
      return null;
    }
    int SRID = 0;
    String wkt = ewkt;

    int index = ewkt.indexOf("SRID=");
    if (index != -1) {
      int semicolonIndex = ewkt.indexOf(';', index);
      if (semicolonIndex != -1) {
        SRID = Integer.parseInt(ewkt.substring(index + 5, semicolonIndex));
        wkt = ewkt.substring(semicolonIndex + 1);
      } else {
        throw new ParseException("Invalid EWKT string");
      }
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), SRID);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static S2Geography geogFromWKB(byte[] wkb) throws ParseException {
    // return geogFromWKB(wkb, -1);
    return new WKBReader().read(wkb);
  }

  public static S2Geography geogFromWKB(byte[] wkb, int srid) throws ParseException {
    S2Geography geog = geogFromWKB(wkb, srid);
    return geog;
  }

  public static S2Geography pointFromWKB(byte[] wkb) throws ParseException {
    return pointFromWKB(wkb, -1);
  }

  public static S2Geography pointFromWKB(byte[] wkb, int srid) throws ParseException {
    S2Geography geog = geogFromWKB(wkb, srid);
    if (!(geog instanceof SinglePointGeography)) {
      return null;
    }
    return geog;
  }

  public static S2Geography lineFromWKB(byte[] wkb) throws ParseException {
    return lineFromWKB(wkb, -1);
  }

  public static S2Geography lineFromWKB(byte[] wkb, int srid) throws ParseException {
    S2Geography geom = geogFromWKB(wkb, srid);
    if (!(geom instanceof SinglePolylineGeography)) {
      return null;
    }
    return geom;
  }

  public static S2Geography mPointFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTIPOINT")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static S2Geography mLineFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTILINESTRING")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static S2Geography mPolyFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTIPOLYGON")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static S2Geography geogCollFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("GEOMETRYCOLLECTION")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static S2Geography geogFromText(
      String geomString, String geomFormat, S2Geography.GeographyKind geographyKind) {
    FileDataSplitter fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat);
    S2GeographyFormatUtils<S2Geography> formatMapper =
        new S2GeographyFormatUtils<>(fileDataSplitter, false, geographyKind);
    try {
      return formatMapper.readGeometry(geomString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static S2Geography geogFromText(String geogString, FileDataSplitter fileDataSplitter) {
    S2GeographyFormatUtils<S2Geography> formatMapper =
        new S2GeographyFormatUtils<>(fileDataSplitter, false);
    try {
      return formatMapper.readWkb(geogString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static S2Geography pointFromText(String geomString, String geomFormat) {
    return geogFromText(geomString, geomFormat, S2Geography.GeographyKind.SINGLEPOINT);
  }

  public static S2Geography polygonFromText(String geomString, String geomFormat) {
    return geogFromText(geomString, geomFormat, S2Geography.GeographyKind.POLYGON);
  }

  public static S2Geography lineStringFromText(String geomString, String geomFormat) {
    return geogFromText(geomString, geomFormat, S2Geography.GeographyKind.SINGLEPOLYLINE);
  }

  public static S2Geography lineFromText(String geomString) {
    FileDataSplitter fileDataSplitter = FileDataSplitter.WKT;
    S2Geography geography = S2Constructors.geogFromText(geomString, fileDataSplitter);
    if (geography.toString().contains("LineString")) {
      return geography;
    } else {
      return null;
    }
  }
}
