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

import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.utils.FormatUtils;
import org.apache.sedona.common.utils.GeoHashDecoder;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.gml2.GMLReader;
import org.locationtech.jts.io.kml.KMLReader;
import org.xml.sax.SAXException;

public class Constructors {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public static Geometry geomFromWKT(String wkt, int srid) throws ParseException {
    if (wkt == null) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static Geometry geomFromEWKT(String ewkt) throws ParseException {
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

  public static Geometry geomFromWKB(byte[] wkb) throws ParseException {
    return geomFromWKB(wkb, -1);
  }

  public static Geometry geomFromWKB(byte[] wkb, int SRID) throws ParseException {
    Geometry geom = new WKBReader().read(wkb);
    if (geom.getFactory().getSRID() != geom.getSRID() || (SRID >= 0 && geom.getSRID() != SRID)) {
      // Make sure that the geometry and the geometry factory have the correct SRID
      if (SRID < 0) {
        SRID = geom.getSRID();
      }
      return Functions.setSRID(geom, SRID);
    } else {
      return geom;
    }
  }

  public static Geometry pointFromWKB(byte[] wkb) throws ParseException {
    return pointFromWKB(wkb, -1);
  }

  public static Geometry pointFromWKB(byte[] wkb, int srid) throws ParseException {
    Geometry geom = geomFromWKB(wkb, srid);
    if (!(geom instanceof Point)) {
      return null;
    }
    return geom;
  }

  public static Geometry lineFromWKB(byte[] wkb) throws ParseException {
    return lineFromWKB(wkb, -1);
  }

  public static Geometry lineFromWKB(byte[] wkb, int srid) throws ParseException {
    Geometry geom = geomFromWKB(wkb, srid);
    if (!(geom instanceof LineString)) {
      return null;
    }
    return geom;
  }

  public static Geometry mPointFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTIPOINT")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static Geometry mLineFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTILINESTRING")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static Geometry mPolyFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("MULTIPOLYGON")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  public static Geometry geomCollFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("GEOMETRYCOLLECTION")) {
      return null;
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return new WKTReader(geometryFactory).read(wkt);
  }

  /**
   * Creates a point from the given coordinate. ST_Point in Sedona Spark API took an optional z
   * value before v1.4.0. This was removed to avoid confusion with other GIS implementations where
   * the optional third argument is srid.
   *
   * <p>A future version of Sedona will add a srid parameter once enough users have upgraded and
   * hence are forced to use ST_PointZ for 3D points.
   *
   * @param x the x value
   * @param y the y value
   * @return The point geometry
   */
  public static Geometry point(double x, double y) {
    // See srid parameter discussion in https://issues.apache.org/jira/browse/SEDONA-234
    return GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
  }

  public static Geometry makePointM(double x, double y, double m) {
    return GEOMETRY_FACTORY.createPoint(new CoordinateXYM(x, y, m));
  }

  public static Geometry makePoint(Double x, Double y, Double z, Double m) {
    GeometryFactory geometryFactory = GEOMETRY_FACTORY;
    if (x == null || y == null) {
      return null;
    }
    if (z == null && m == null) {
      return geometryFactory.createPoint(new Coordinate(x, y));
    }
    if (z != null && m == null) {
      return geometryFactory.createPoint(new Coordinate(x, y, z));
    }
    if (z == null) {
      return geometryFactory.createPoint(new CoordinateXYZM(x, y, 0, m));
    }
    return geometryFactory.createPoint(new CoordinateXYZM(x, y, z, m));
  }

  /**
   * Creates a point from the given coordinate.
   *
   * @param x the x value
   * @param y the y value
   * @param z the z value
   * @param srid Set to 0 if unknown
   * @return The point geometry
   */
  public static Geometry pointZ(double x, double y, double z, int srid) {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return geometryFactory.createPoint(new Coordinate(x, y, z));
  }

  /**
   * Creates a point from the given coordinate.
   *
   * @param x the x value
   * @param y the y value
   * @param m the m value
   * @param srid Set to 0 if unknown
   * @return The point geometry
   */
  public static Geometry pointM(double x, double y, double m, int srid) {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return geometryFactory.createPoint(new CoordinateXYZM(x, y, 0, m));
  }

  /**
   * Creates a point from the given coordinate.
   *
   * @param x the x value
   * @param y the y value
   * @param z the z value
   * @param m the m value
   * @param srid Set to 0 if unknown
   * @return The point geometry
   */
  public static Geometry pointZM(double x, double y, double z, double m, int srid) {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return geometryFactory.createPoint(new CoordinateXYZM(x, y, z, m));
  }

  public static Geometry geomFromText(
      String geomString, String geomFormat, GeometryType geometryType) {
    FileDataSplitter fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat);
    FormatUtils<Geometry> formatMapper = new FormatUtils<>(fileDataSplitter, false, geometryType);
    try {
      return formatMapper.readGeometry(geomString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static Geometry geomFromText(String geomString, FileDataSplitter fileDataSplitter) {
    FormatUtils<Geometry> formatMapper = new FormatUtils<>(fileDataSplitter, false);
    try {
      return formatMapper.readGeometry(geomString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static Geometry pointFromText(String geomString, String geomFormat) {
    return geomFromText(geomString, geomFormat, GeometryType.POINT);
  }

  public static Geometry polygonFromText(String geomString, String geomFormat) {
    return geomFromText(geomString, geomFormat, GeometryType.POLYGON);
  }

  public static Geometry lineStringFromText(String geomString, String geomFormat) {
    return geomFromText(geomString, geomFormat, GeometryType.LINESTRING);
  }

  public static Geometry lineFromText(String geomString) {
    FileDataSplitter fileDataSplitter = FileDataSplitter.WKT;
    Geometry geometry = Constructors.geomFromText(geomString, fileDataSplitter);
    if (geometry.getGeometryType().contains("LineString")) {
      return geometry;
    } else {
      return null;
    }
  }

  public static Geometry polygonFromEnvelope(double minX, double minY, double maxX, double maxY) {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(minX, minY);
    coordinates[1] = new Coordinate(minX, maxY);
    coordinates[2] = new Coordinate(maxX, maxY);
    coordinates[3] = new Coordinate(maxX, minY);
    coordinates[4] = coordinates[0];
    return GEOMETRY_FACTORY.createPolygon(coordinates);
  }

  public static Geometry geomFromGeoHash(String geoHash, Integer precision) {
    try {
      return GeoHashDecoder.decode(geoHash, precision);
    } catch (GeoHashDecoder.InvalidGeoHashException e) {
      return null;
    }
  }

  public static Geometry pointFromGeoHash(String geoHash, Integer precision) {
    return geomFromGeoHash(geoHash, precision).getCentroid();
  }

  public static Geometry geomFromGML(String gml)
      throws IOException, ParserConfigurationException, SAXException {
    return new GMLReader().read(gml, GEOMETRY_FACTORY);
  }

  public static Geometry geomFromKML(String kml) throws ParseException {
    return new KMLReader().read(kml);
  }
}
