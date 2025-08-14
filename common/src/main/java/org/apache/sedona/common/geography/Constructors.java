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

import java.io.IOException;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.WKBReader;
import org.apache.sedona.common.S2Geography.WKTReader;
import org.apache.sedona.common.utils.GeoHashDecoder;
import org.locationtech.jts.io.ParseException;

public class Constructors {

  public static Geography geogFromWKB(byte[] wkb) throws ParseException {
    return new WKBReader().read(wkb);
  }

  public static Geography geogFromWKB(byte[] wkb, int SRID) throws ParseException {
    Geography geog = geogFromWKB(wkb);
    geog.setSRID(SRID);
    return geog;
  }

  public static Geography geogFromWKT(String wkt, int srid) throws ParseException {
    Geography geog = new WKTReader().read(wkt);
    geog.setSRID(srid);
    return geog;
  }

  public static Geography geogFromEWKT(String ewkt) throws ParseException {
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
    return geogFromWKT(wkt, SRID);
  }

  public static Geography geogCollFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("GEOMETRYCOLLECTION")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography tryToGeography(String geogString) {
    try {
      return toGeography(geogString);
    } catch (Exception e) {
      return null;
    }
  }

  public static Geography toGeography(String geogString) throws ParseException {
    if (geogString == null || geogString.trim().isEmpty()) {
      return null;
    }
    geogString = geogString.trim();

    // EWKT format (e.g., "SRID=4326;POINT(...)")
    if (geogString.toUpperCase().startsWith("SRID")) {
      return Constructors.geogFromEWKT(geogString);
    }
    // WKB hex string (only hex digits)
    if (isHex(geogString)) {
      byte[] wkbBytes = WKBReader.hexToBytes(geogString);
      return Constructors.geogFromWKB(wkbBytes);
    }
    // GeoHash (short alphanumeric string without spaces)
    if (isGeoHash(geogString)) {
      return Constructors.geogFromGeoHash(geogString, 16);
    }
    // Default: WKT
    return Constructors.geogFromWKT(geogString, 0);
  }

  private static boolean isHex(String s) {
    // (?i) = case-insensitive; matches only 0-9 A-F
    return s.matches("(?i)^[0-9a-f]+$");
  }

  private static boolean isGeoHash(String s) {
    // GeoHash Base32: digits + bcdefghjkmnpqrstuvwxyz (no a, i, l, o)
    // (?i) = case-insensitive so upper/lower both pass
    return s.matches("(?i)^[0-9bcdefghjkmnpqrstuvwxyz]+$");
  }

  public static Geography geogFromGeoHash(String geoHash, Integer precision) {
    try {
      return GeoHashDecoder.decodeGeog(geoHash, precision);
    } catch (GeoHashDecoder.InvalidGeoHashException e) {
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
