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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.geometryObjects.Box2D;
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
    return polygonFromEnvelope(minX, minY, maxX, maxY, GEOMETRY_FACTORY);
  }

  public static Geometry polygonFromEnvelope(
      double minX, double minY, double maxX, double maxY, GeometryFactory factory) {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(minX, minY);
    coordinates[1] = new Coordinate(minX, maxY);
    coordinates[2] = new Coordinate(maxX, maxY);
    coordinates[3] = new Coordinate(maxX, minY);
    coordinates[4] = coordinates[0];
    return factory.createPolygon(coordinates);
  }

  public static Geometry makeEnvelope(
      double minX, double minY, double maxX, double maxY, int srid) {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    return polygonFromEnvelope(minX, minY, maxX, maxY, geometryFactory);
  }

  public static Geometry makeEnvelope(double minX, double minY, double maxX, double maxY) {
    return makeEnvelope(minX, minY, maxX, maxY, 0);
  }

  /**
   * Convert a {@link Box2D} to a Geometry. Mirrors PostGIS {@code box2d::geometry}: dispatches on
   * dimensionality so the result matches what {@code ST_Envelope(geom)} would have produced for the
   * source geometry. Degenerate boxes return:
   *
   * <ul>
   *   <li>{@code POINT} when {@code xmin == xmax && ymin == ymax}
   *   <li>{@code LINESTRING} when exactly one of the X / Y intervals collapses
   *   <li>{@code POLYGON} otherwise
   * </ul>
   *
   * Returns NULL on null input.
   */
  public static Geometry geomFromBox2D(Box2D box) {
    if (box == null) {
      return null;
    }
    double xmin = box.getXMin();
    double ymin = box.getYMin();
    double xmax = box.getXMax();
    double ymax = box.getYMax();
    boolean xCollapsed = xmin == xmax;
    boolean yCollapsed = ymin == ymax;
    if (xCollapsed && yCollapsed) {
      return GEOMETRY_FACTORY.createPoint(new Coordinate(xmin, ymin));
    }
    if (xCollapsed || yCollapsed) {
      return GEOMETRY_FACTORY.createLineString(
          new Coordinate[] {new Coordinate(xmin, ymin), new Coordinate(xmax, ymax)});
    }
    return polygonFromEnvelope(xmin, ymin, xmax, ymax);
  }

  /**
   * Build a {@link Box2D} from two corner points. The corners are taken verbatim — no swapping or
   * validation of ordering — so {@code xmin > xmax} or {@code ymin > ymax} are preserved as
   * supplied. NULL or empty point inputs return NULL.
   */
  public static Box2D makeBox2D(Geometry lowerLeft, Geometry upperRight) {
    if (lowerLeft == null || upperRight == null) {
      return null;
    }
    if (!(lowerLeft instanceof Point) || !(upperRight instanceof Point)) {
      throw new IllegalArgumentException("ST_MakeBox2D requires two POINT geometries");
    }
    if (lowerLeft.isEmpty() || upperRight.isEmpty()) {
      return null;
    }
    Point ll = (Point) lowerLeft;
    Point ur = (Point) upperRight;
    return new Box2D(ll.getX(), ll.getY(), ur.getX(), ur.getY());
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

  public static Geometry geomFromMySQL(byte[] binary) throws ParseException {
    ByteBuffer buffer = ByteBuffer.wrap(binary);

    buffer.order(ByteOrder.LITTLE_ENDIAN);
    int srid = buffer.getInt();

    byte[] wkb = new byte[buffer.remaining()];

    buffer.get(wkb);

    return geomFromWKB(wkb, srid);
  }
}
