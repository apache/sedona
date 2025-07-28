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
import org.apache.sedona.common.S2Geography.WKTReader;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

/** This format mapper is isolated on purpose for the sake of sharing across Spark and Flink */
public class S2GeographyFormatUtils<T extends S2Geography> implements Serializable {
  static final Logger logger = LoggerFactory.getLogger(S2GeographyFormatUtils.class);
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

  protected transient GeoJSONReader geoJSONReader = new GeoJSONReader();
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
  public S2GeographyFormatUtils(
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
        || splitter == FileDataSplitter.WKT
        || splitter == FileDataSplitter.GEOJSON;
  }

  /**
   * Instantiates a new format mapper. This is extensively used in SedonaSQL.
   *
   * @param splitter
   * @param carryInputData
   */
  public S2GeographyFormatUtils(FileDataSplitter splitter, boolean carryInputData) {
    this(0, -1, splitter, carryInputData, null);
  }

  /**
   * This format mapper is used in SedonaSQL.
   *
   * @param splitter
   * @param carryInputData
   * @param geographyKind
   */
  public S2GeographyFormatUtils(
      FileDataSplitter splitter, boolean carryInputData, S2Geography.GeographyKind geographyKind) {
    this(0, -1, splitter, carryInputData, geographyKind);
  }

  public static List<String> readGeoJsonPropertyNames(String geoJson) {
    if (geoJson == null) {
      logger.warn("The given GeoJson record is null and cannot be used to find property names");
      return null;
    }
    List<String> propertyList = new ArrayList<>();
    if (geoJson.contains("Feature") || geoJson.contains("feature") || geoJson.contains("FEATURE")) {
      Feature feature = (Feature) GeoJSONFactory.create(geoJson);
      if (!Objects.isNull(feature.getId())) {
        propertyList.add("id");
      }
      Map<String, Object> properties = feature.getProperties();
      if (properties != null) {
        propertyList.addAll(properties.keySet());
      }
    }
    return propertyList.size() > 0 ? propertyList : null;
  }

  private void readObject(ObjectInputStream inputStream)
      throws IOException, ClassNotFoundException {
    inputStream.defaultReadObject();
    factory = new GeometryFactory();
    wktReader = new WKTReader();
    geoJSONReader = new GeoJSONReader();
  }

  private void handleNonSpatialDataToGeometry(
      S2Geography geometry, List<String> splittedGeometryData) {
    LinkedList<String> splittedGeometryDataList = new LinkedList<String>(splittedGeometryData);
    if (carryInputData) {
      if (this.splitter != FileDataSplitter.GEOJSON) {
        // remove spatial data position
        splittedGeometryDataList.remove(this.startOffset);
      }
      // geometry.setUserData(String.join("\t", splittedGeometryDataList));
    }
  }
  //
  //  public Geometry readGeoJSON(String geoJson) {
  //    final Geometry geometry;
  //    if (geoJson.contains("Feature")) {
  //      Feature feature = (Feature) GeoJSONFactory.create(geoJson);
  //      ArrayList<String> nonSpatialData = new ArrayList<>();
  //      Map<String, Object> featurePropertiesproperties = feature.getProperties();
  //      if (feature.getId() != null) {
  //        nonSpatialData.add(feature.getId().toString());
  //      }
  //      if (featurePropertiesproperties != null) {
  //        for (Object property : featurePropertiesproperties.values()) {
  //          if (property == null) {
  //            nonSpatialData.add("");
  //          } else {
  //            nonSpatialData.add(property.toString());
  //          }
  //        }
  //      }
  //      geometry = geoJSONReader.read(feature.getGeometry());
  //      handleNonSpatialDataToGeometry(geometry, nonSpatialData);
  //    } else {
  //      geometry = geoJSONReader.read(geoJson);
  //    }
  //    return geometry;
  //  }
  //
  //  public List<String> readPropertyNames(String geoString) {
  //    switch (splitter) {
  //      case GEOJSON:
  //        return readGeoJsonPropertyNames(geoString);
  //      default:
  //        return null;
  //    }
  //  }

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
    S2Geography geography = wkbReader.read(aux);
    //    if (geometry.getSRID() != geometry.getFactory().getSRID()) {
    //      // Make sure that the geometry factory has the correct SRID when the parsed WKB
    //      // contains a non-zero SRID (EWKB)
    //      geometry = Functions.setSRID(geometry, geometry.getSRID());
    //    }
    // handleNonSpatialDataToGeometry(geography, Arrays.asList(columns));
    return geography;
  }

  //  public Coordinate[] readCoordinates(String line) {
  //    final String[] columns = line.split(splitter.getDelimiter());
  //    final int actualEndOffset =
  //        this.endOffset >= 0
  //            ? this.endOffset
  //            : (this.geometryType == GeometryType.POINT ? startOffset + 1 : columns.length - 1);
  //    final Coordinate[] coordinates = new Coordinate[(actualEndOffset - startOffset + 1) / 2];
  //    for (int i = this.startOffset; i <= actualEndOffset; i += 2) {
  //      coordinates[(i - startOffset) / 2] =
  //          new Coordinate(Double.parseDouble(columns[i]), Double.parseDouble(columns[i + 1]));
  //    }
  //    if (carryInputData) {
  //      boolean firstColumnFlag = true;
  //      otherAttributes = "";
  //      for (int i = 0; i < this.startOffset; i++) {
  //        if (firstColumnFlag) {
  //          otherAttributes += columns[i];
  //          firstColumnFlag = false;
  //        } else {
  //          otherAttributes += "\t" + columns[i];
  //        }
  //      }
  //      for (int i = actualEndOffset + 1; i < columns.length; i++) {
  //        if (firstColumnFlag) {
  //          otherAttributes += columns[i];
  //          firstColumnFlag = false;
  //        } else {
  //          otherAttributes += "\t" + columns[i];
  //        }
  //      }
  //    }
  //    return coordinates;
  //  }

  public S2Geography readGeometry(String line) throws ParseException {
    S2Geography geometry = null;
    try {
      switch (this.splitter) {
        case WKT:
          geometry = readWkt(line);
          break;
        case WKB:
          geometry = readWkb(line);
          break;
        default:
          {
            if (this.geographyKind == null) {
              throw new IllegalArgumentException(
                  "[Sedona][FormatMapper] You must specify GeometryType when you use delimiter rather than WKB, WKT or GeoJSON");
            } else {
              // geometry = createGeometry(readCoordinates(line), geometryType);
            }
          }
      }
    } catch (Exception e) {
      logger.error("[Sedona] " + e.getMessage());
      if (skipSyntacticallyInvalidGeometries == false) {
        throw e;
      }
    }
    if (geometry == null) {
      return null;
    }
    //    if (allowTopologicallyInvalidGeometries == false) {
    //      IsValidOp isvalidop = new IsValidOp(geometry);
    //      if (isvalidop.isValid() == false) {
    //        geometry = null;
    //      }
    //    }

    return geometry;
  }
  //
  //  private Geometry createGeometry(Coordinate[] coordinates, GeometryType geometryType) {
  //    GeometryFactory geometryFactory = new GeometryFactory();
  //    Geometry geometry = null;
  //    switch (geometryType) {
  //      case POINT:
  //        geometry = geometryFactory.createPoint(coordinates[0]);
  //        break;
  //      case POLYGON:
  //        geometry = geometryFactory.createPolygon(coordinates);
  //        break;
  //      case LINESTRING:
  //        geometry = geometryFactory.createLineString(coordinates);
  //        break;
  //      case RECTANGLE:
  //        // The rectangle mapper reads two coordinates from the input line. The two coordinates
  // are
  //        // the two on the diagonal.
  //        assert coordinates.length == 2;
  //        Coordinate[] polyCoordinates = new Coordinate[5];
  //        polyCoordinates[0] = coordinates[0];
  //        polyCoordinates[1] = new Coordinate(coordinates[0].x, coordinates[1].y);
  //        polyCoordinates[2] = coordinates[1];
  //        polyCoordinates[3] = new Coordinate(coordinates[1].x, coordinates[0].y);
  //        polyCoordinates[4] = polyCoordinates[0];
  //        geometry = factory.createPolygon(polyCoordinates);
  //        break;
  //        // Read string to point if no geometry type specified but Sedona should never reach here
  //      default:
  //        geometry = geometryFactory.createPoint(coordinates[0]);
  //    }
  //    if (carryInputData) {
  //      geometry.setUserData(otherAttributes);
  //    }
  //    return geometry;
  //  }
  //

  public <T extends S2Geography> void addMultiGeometry(
      GeographyCollection multiGeometry, List<T> result) {
    for (int i = 0; i < multiGeometry.getFeatures().size(); i++) {
      T geometry = (T) multiGeometry.getFeatures().get(i);
      // geometry.setUserData(multiGeometry.getUserData());
      result.add(geometry);
    }
  }

  protected void addGeometry(S2Geography geometry, List<T> result) {
    if (geometry == null) {
      return;
    }
    //        if (geometry instanceof PointGeography) {
    //          addMultiGeometry((PointGeography) geometry, result);
    //        } else if (geometry instanceof PolylineGeography) {
    //          addMultiGeometry((PolylineGeography) geometry, result);
    //        } else if (geometry instanceof MultiPolygonGeography) {
    //          addMultiGeometry((MultiPolygonGeography) geometry, result);
    //        } else {
    //          result.add((T) geometry);
    //        }
    result.add((T) geometry);
  }

  private void addMultiGeometry(PointGeography point, List<T> result) {
    result.add((T) point);
  }
}
