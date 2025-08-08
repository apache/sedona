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
package org.apache.sedona.common.geography;

import org.apache.sedona.common.S2Geography.*;
import org.locationtech.jts.io.ParseException;

public class Constructors {

  public static Geography geogFromWKB(byte[] wkb) throws ParseException {
    return new WKBReader().read(wkb);
  }

  public static Geography geogFromWKB(String wkb) throws ParseException {
    return new WKBReader().read(WKBReader.hexToBytes(wkb));
  }

  public static Geography geogFromWKB(byte[] wkb, int SRID) throws ParseException {
    Geography geog = geogFromWKB(wkb);
    geog.setSRID(SRID);
    return geog;
  }

  public static Geography geogFromWKB(String wkb, int SRID) throws ParseException {
    Geography geog = geogFromWKB(wkb);
    geog.setSRID(SRID);
    return geog;
  }

  public static Geography geogFromWKT(String wkt, int srid) throws ParseException {
    Geography geog = new WKTReader().read(wkt);
    geog.setSRID(srid);
    return geog;
  }

  public static Geography geogCollFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("GEOMETRYCOLLECTION")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography pointFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("POINT")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography mPointFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTIPOINT")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography lineFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("LINESTRING")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography lineStringFromText(String wkt, int srid) throws ParseException {
    return lineFromText(wkt, srid);
  }

  public static Geography mLineFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTLINESTRING")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography polygonFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("POLYGON")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography mPolyFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTIPOLYGON")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography lineFromWKB(byte[] wkb) throws ParseException {
    return lineFromWKB(wkb, 0);
  }

  public static Geography lineFromWKB(byte[] wkb, int srid) throws ParseException {
    return geogFromWKB(wkb, srid);
  }

  public static Geography pointFromWKB(byte[] wkb) throws ParseException {
    return pointFromWKB(wkb, 0);
  }

  public static Geography pointFromWKB(byte[] wkb, int srid) throws ParseException {
    return geogFromWKB(wkb, srid);
  }

  public static Geography lineFromWKB(String wkb) throws ParseException {
    return lineFromWKB(wkb, 0);
  }

  public static Geography lineFromWKB(String wkb, int srid) throws ParseException {
    Geography geog = geogFromWKB(wkb, srid);
    if (Geography.GeographyKind.fromKind(geog.getKind())
        == Geography.GeographyKind.SINGLEPOLYLINE) {
      return geog;
    }
    return null;
  }

  public static Geography pointFromWKB(String wkb) throws ParseException {
    return pointFromWKB(wkb, 0);
  }

  public static Geography pointFromWKB(String wkb, int srid) throws ParseException {
    Geography geog = geogFromWKB(wkb, srid);
    if (Geography.GeographyKind.fromKind(geog.getKind()) == Geography.GeographyKind.SINGLEPOINT) {
      return geog;
    }
    return null;
  }
}
