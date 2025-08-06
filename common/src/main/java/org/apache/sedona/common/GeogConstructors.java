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

import org.apache.sedona.common.S2Geography.S2Geography;
import org.apache.sedona.common.S2Geography.SinglePointGeography;
import org.apache.sedona.common.S2Geography.WKBReader;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.geometryObjects.Geography;
import org.locationtech.jts.io.ParseException;

public class GeogConstructors {

  public static Geography geogFromWKB(byte[] wkb) throws ParseException {
    S2Geography raw = new WKBReader().read(wkb);
    return new Geography(raw);
  }

  public static Geography geogFromWKB(byte[] wkb, int SRID) throws ParseException {
    return geogFromWKB(wkb);
  }

  public static Geography pointFromWKB(byte[] wkb) throws ParseException {
    return pointFromWKB(wkb, -1);
  }

  public static Geography pointFromWKB(byte[] wkb, int srid) throws ParseException {
    Geography geog = geogFromWKB(wkb, srid);
    if (!(geog.getDelegate() instanceof SinglePointGeography)) {
      return null;
    }
    return geog;
  }

  public static Geography lineFromWKB(byte[] wkb) throws ParseException {
    return lineFromWKB(wkb, -1);
  }

  public static Geography lineFromWKB(byte[] wkb, int srid) throws ParseException {
    Geography geog = geogFromWKB(wkb, srid);
    if (!(geog.getDelegate() instanceof SinglePointGeography)) {
      return null;
    }
    return geog;
  }

  public static Geography geogFromText(
          String geogString,
          String geogFormat,
          S2Geography.GeographyKind geographyKind
  ) throws ParseException {
    FileDataSplitter splitter = FileDataSplitter.getFileDataSplitter(geogFormat);
    if (splitter == FileDataSplitter.WKB) {
      byte[] bytes = WKBReader.hexToBytes(geogString);
      return geogFromWKB(bytes);
    } else {
      throw new UnsupportedOperationException(
              "Only hex-encoded WKB format is supported for text input"
      );
    }
  }

  public static Geography geogFromText(
          String geogString,
          FileDataSplitter fileDataSplitter
  ) throws ParseException {
    if (fileDataSplitter == FileDataSplitter.WKB) {
      byte[] bytes = WKBReader.hexToBytes(geogString);
      return geogFromWKB(bytes);
    } else {
      throw new UnsupportedOperationException(
              "Only hex-encoded WKB format is supported for text input"
      );
    }
  }

  public static Geography pointFromText(String geogString, String geogFormat) throws ParseException {
    return geogFromText(geogString, geogFormat, S2Geography.GeographyKind.SINGLEPOINT);
  }

  public static Geography polygonFromText(String geogString, String geogFormat) throws ParseException {
    return geogFromText(geogString, geogFormat, S2Geography.GeographyKind.POLYGON);
  }

  public static Geography lineStringFromText(String geogString, String geogFormat) throws ParseException {
    return geogFromText(geogString, geogFormat, Geography.GeographyKind.SINGLEPOLYLINE);
  }

}
