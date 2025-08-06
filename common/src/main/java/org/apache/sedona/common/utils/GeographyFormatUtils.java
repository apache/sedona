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

import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polyline;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;
import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.geometryObjects.Geography;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeographyFormatUtils<T extends Geography> implements Serializable {
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

  protected Geography.GeographyKind geographyKind = null;
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
      Geography.GeographyKind geographyKind) {
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

  private void handleNonSpatialDataToGeography(
      Geography geography, List<String> splittedGeographyData) {
    LinkedList<String> splittedGeographyDataList = new LinkedList<String>(splittedGeographyData);
    if (carryInputData) {
      if (this.splitter != FileDataSplitter.GEOJSON) {
        // remove spatial data position
        splittedGeographyDataList.remove(this.startOffset);
      }
    }
  }

  public Geography readWkt(String line) throws ParseException {
    final String[] columns = line.split(splitter.getDelimiter());
    Geography geography = null;

    try {
      geography = (Geography) wktReader.read(columns[this.startOffset]);
    } catch (Exception e) {
      logger.error("[Sedona] " + e.getMessage());
    }
    if (geography == null) {
      return null;
    }
    handleNonSpatialDataToGeography(geography, Arrays.asList(columns));
    return geography;
  }

  public Geography readWkb(String line) throws ParseException {
    final String[] columns = line.split(splitter.getDelimiter());
    final byte[] aux = WKBReader.hexToBytes(columns[this.startOffset]);
    // For some unknown reasons, the wkb reader cannot be used in transient variable like the wkt
    // reader.
    WKBReader wkbReader = new WKBReader();
    S2Geography rawGeography = wkbReader.read(aux);
    return new Geography(rawGeography);
  }

  public Geography readGeography(String line) throws ParseException {
    Geography geography = null;
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

  public void addMultiGeography(
      GeographyCollection multiGeography, List<? super Geography> result) {
    if (multiGeography == null) return;
    for (int i = 0; i < multiGeography.getFeatures().size(); i++) {
      addGeography((Geography) multiGeography.getFeatures().get(i), result);
    }
  }

  // 2) Break a PointGeography into its single points
  public void addMultiPoint(PointGeography multi, List<? super SinglePointGeography> result) {
    if (multi == null) return;
    for (S2Point pt : multi.getPoints()) {
      result.add(new SinglePointGeography(pt));
    }
  }

  // 3) Break a PolylineGeography into its single polylines
  public void addMultiPolyline(
      PolylineGeography multi, List<? super SinglePolylineGeography> result) {
    if (multi == null) return;
    for (S2Polyline line : multi.getPolylines()) {
      result.add(new SinglePolylineGeography(line));
    }
  }

  protected void addGeography(Geography geography, List<? super Geography> result) {
    if (geography == null) {
      return;
    }
    S2Geography geog = geography.getDelegate();

    if (geog instanceof PointGeography && !(geog instanceof SinglePointGeography)) {
      addMultiPoint((PointGeography) geog, (List<? super SinglePointGeography>) result);

    } else if (geog instanceof PolylineGeography && !(geog instanceof SinglePolylineGeography)) {
      addMultiPolyline((PolylineGeography) geog, (List<? super SinglePolylineGeography>) result);

    } else if (geog instanceof MultiPolygonGeography) {
      addMultiGeography((MultiPolygonGeography) geog, result);

    } else if (geog instanceof GeographyCollection) {
      addMultiGeography((GeographyCollection) geog, result);

    } else {
      // already a “single” Geography of some kind
      result.add((Geography) geog);
    }
  }
}
