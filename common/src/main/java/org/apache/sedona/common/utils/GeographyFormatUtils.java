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
package org.apache.sedona.common.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;
import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeographyFormatUtils<T extends S2Geography> implements Serializable {
  static final Logger logger = LoggerFactory.getLogger(GeographyFormatUtils.class);
  /** The start offset. */
  protected final int startOffset;
  /** The end offset. */
  /* If the initial value is negative, Sedona will consider each field as a spatial attribute if the target object is LineString or Polygon. */
  protected final int endOffset;
  /** The splitter. */
  protected final FileDataSplitter splitter;
  /** The carry input data. */
  protected final boolean carryInputData;
  /** Non-spatial attributes in each input row will be concatenated to a tab separated string */
  protected String otherAttributes = "";

  protected S2Geography.GeographyKind geographyKind = null;
  /** The factory. */
  protected transient GeometryFactory factory = new GeometryFactory();

  protected transient WKTReader wktReader = new WKTReader();
  /** Allow mapping of invalid geometries. */
  public boolean allowTopologicallyInvalidGeometries;
  // For some unknown reasons, the wkb reader cannot be used in transient variable like the wkt
  // reader.
  /** Crash on syntactically invalid geometries or skip them. */
  public boolean skipSyntacticallyInvalidGeometries;

  /**
   * Instantiates a new format mapper.
   *
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @param splitter the splitter
   * @param carryInputData the carry input data
   */
  public GeographyFormatUtils(
      int startOffset,
      int endOffset,
      FileDataSplitter splitter,
      boolean carryInputData,
      S2Geography.GeographyKind geographyKind) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.splitter = splitter;
    this.carryInputData = carryInputData;
    this.geographyKind = geographyKind;
    this.allowTopologicallyInvalidGeometries = true;
    this.skipSyntacticallyInvalidGeometries = false;
    // Only the following formats are allowed to use this format mapper because each input has the
    // geometry type definition
    assert geographyKind != null
        || splitter == FileDataSplitter.WKB
        || splitter == FileDataSplitter.WKT;
  }

  /**
   * Instantiates a new format mapper. This is extensively used in SedonaSQL.
   *
   * @param splitter
   * @param carryInputData
   */
  public GeographyFormatUtils(FileDataSplitter splitter, boolean carryInputData) {
    this(0, -1, splitter, carryInputData, null);
  }

  /**
   * This format mapper is used in SedonaSQL.
   *
   * @param splitter
   * @param carryInputData
   * @param geographyKind
   */
  public GeographyFormatUtils(
      FileDataSplitter splitter, boolean carryInputData, S2Geography.GeographyKind geographyKind) {
    this(0, -1, splitter, carryInputData, geographyKind);
  }

  private void readObject(ObjectInputStream inputStream)
      throws IOException, ClassNotFoundException {
    inputStream.defaultReadObject();
    factory = new GeometryFactory();
    wktReader = new WKTReader();
  }

  private void handleNonSpatialDataToGeometry(
      S2Geography geometry, List<String> splittedGeometryData) {
    LinkedList<String> splittedGeometryDataList = new LinkedList<String>(splittedGeometryData);
    if (carryInputData) {
      if (this.splitter != FileDataSplitter.GEOJSON) {
        // remove spatial data position
        splittedGeometryDataList.remove(this.startOffset);
      }
    }
  }

  public S2Geography readWkt(String line) throws ParseException {
    final String[] columns = line.split(splitter.getDelimiter());
    S2Geography geography = null;

    try {
      geography = wktReader.read(columns[this.startOffset]);
    } catch (Exception e) {
      logger.error("[Sedona] " + e.getMessage());
    }
    if (geography == null) {
      return null;
    }
    handleNonSpatialDataToGeometry(geography, Arrays.asList(columns));
    return geography;
  }

  public S2Geography readWkb(String line) throws ParseException {
    final String[] columns = line.split(splitter.getDelimiter());
    final byte[] aux = WKBReader.hexToBytes(columns[this.startOffset]);
    // For some unknown reasons, the wkb reader cannot be used in transient variable like the wkt
    // reader.
    WKBReader wkbReader = new WKBReader();
    return wkbReader.read(aux);
  }

  public S2Geography readGeography(String line) throws ParseException {
    S2Geography geography = null;
    try {
      switch (this.splitter) {
        case WKT:
          geography = readWkt(line);
          break;
        case WKB:
          geography = readWkb(line);
          break;
        default:
          {
            throw new IllegalArgumentException(
                "[Sedona][FormatMapper] You must specify GeometryType when you use delimiter rather than WKB, WKT or GeoJSON");
          }
      }
    } catch (Exception e) {
      logger.error("[Sedona] " + e.getMessage());
      if (skipSyntacticallyInvalidGeometries == false) {
        throw e;
      }
    }
    if (geography == null) {
      return null;
    }
    return geography;
  }

  public <T extends S2Geography> void addMultiGeography(
      GeographyCollection multiGeography, List<T> result) {
    for (int i = 0; i < multiGeography.getFeatures().size(); i++) {
      T geography = (T) multiGeography.getFeatures().get(i);
      result.add(geography);
    }
  }

  public <T extends S2Geography> void addMultiPoint(PointGeography pointGeography, List<T> result) {
    for (int i = 0; i < pointGeography.getPoints().size(); i++) {
      T geography = (T) new SinglePointGeography(pointGeography.getPoints().get(i));
      result.add(geography);
    }
  }

  public <T extends S2Geography> void addMultiPolyline(
      PolylineGeography polylineGeography, List<T> result) {
    for (int i = 0; i < polylineGeography.getPolylines().size(); i++) {
      T geography = (T) new SinglePolylineGeography(polylineGeography.getPolylines().get(i));
      result.add(geography);
    }
  }

  protected void addGeography(S2Geography geography, List<T> result) {
    if (geography == null) {
      return;
    }
    if (geography instanceof PointGeography) {
      addMultiPoint((PointGeography) geography, result);
    } else if (geography instanceof PolylineGeography) {
      addMultiPolyline((PolylineGeography) geography, result);
    } else if (geography instanceof MultiPolygonGeography) {
      addMultiGeography((MultiPolygonGeography) geography, result);
    } else {
      result.add((T) geography);
    }
  }
}
