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
package org.apache.sedona.common.S2Geography;

/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
import com.google.common.geometry.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.*;

/** This code is a custom modified version of JTS WKBReader */
public class WKBReader {
  /**
   * Converts a hexadecimal string to a byte array. The hexadecimal digit symbols are
   * case-insensitive.
   *
   * @param hex a string containing hex digits
   * @return an array of bytes with the value of the hex string
   */
  public static byte[] hexToBytes(String hex) {
    int byteLen = hex.length() / 2;
    byte[] bytes = new byte[byteLen];

    for (int i = 0; i < hex.length() / 2; i++) {
      int i2 = 2 * i;
      if (i2 + 1 > hex.length()) throw new IllegalArgumentException("Hex string has odd length");

      int nib1 = hexToInt(hex.charAt(i2));
      int nib0 = hexToInt(hex.charAt(i2 + 1));
      byte b = (byte) ((nib1 << 4) + (byte) nib0);
      bytes[i] = b;
    }
    return bytes;
  }

  private static int hexToInt(char hex) {
    int nib = Character.digit(hex, 16);
    if (nib < 0) throw new IllegalArgumentException("Invalid hex digit: '" + hex + "'");
    return nib;
  }

  private static final String INVALID_GEOM_TYPE_MSG = "Invalid geometry type encountered in ";

  private static final String FIELD_NUMCOORDS = "numCoords";

  private static final String FIELD_NUMRINGS = "numRings";

  private static final String FIELD_NUMELEMS = "numElems";

  private GeometryFactory factory;
  private CoordinateSequenceFactory csFactory;
  private PrecisionModel precisionModel;
  // default dimension - will be set on read
  private int inputDimension = 2;
  /**
   * true if structurally invalid input should be reported rather than repaired. At some point this
   * could be made client-controllable.
   */
  private boolean isStrict = false;

  private ByteOrderDataInStream dis = new ByteOrderDataInStream();
  private double[] ordValues;

  private int maxNumFieldValue;

  /** Default constructor uses a JTS Array-based sequence factory. */
  public WKBReader() {
    this(new GeometryFactory());
  }

  public WKBReader(GeometryFactory geometryFactory) {
    this.factory = geometryFactory;
    precisionModel = factory.getPrecisionModel();
    csFactory = factory.getCoordinateSequenceFactory();
  }

  /**
   * Reads a single {@link org.apache.sedona.common.S2Geography.Geography} in WKB format from a byte
   * array.
   *
   * @param bytes the byte array to read from
   * @return the geometry read
   * @throws ParseException if the WKB is ill-formed
   */
  public Geography read(byte[] bytes) throws ParseException {
    try {
      // Use a very high limit to avoid malformed input OOM
      return read(new ByteArrayInStream(bytes), Integer.MAX_VALUE);
    } catch (IOException ex) {
      throw new RuntimeException("I/O error reading WKB: " + ex.getMessage(), ex);
    }
  }

  /**
   * Reads a {@link org.apache.sedona.common.S2Geography.Geography} in binary WKB format from an
   * {@link InStream}.
   *
   * @param is the stream to read from
   * @return the Geometry read
   * @throws IOException if the underlying stream creates an error
   * @throws ParseException if the WKB is ill-formed
   */
  public Geography read(InStream is) throws IOException, ParseException {
    // can't tell size of InStream, but MAX_VALUE should be safe
    return read(is, Integer.MAX_VALUE);
  }

  private Geography read(InStream is, int maxCoordNum) throws IOException, ParseException {
    /**
     * This puts an upper bound on the allowed value in coordNum fields. It avoids OOM exceptions
     * due to malformed input.
     */
    this.maxNumFieldValue = maxCoordNum;
    dis.setInStream(is);
    return readGeometry(0);
  }

  private int readNumField(String fieldName) throws IOException, ParseException {
    // num field is unsigned int, but Java has only signed int
    int num = dis.readInt();
    if (num < 0 || num > maxNumFieldValue) {
      throw new ParseException(fieldName + " value is too large");
    }
    return num;
  }

  private Geography readGeometry(int SRID) throws IOException, ParseException {

    // determine byte order
    byte byteOrderWKB = dis.readByte();

    // always set byte order, since it may change from geometry to geometry
    if (byteOrderWKB == WKBConstants.wkbNDR) {
      dis.setOrder(ByteOrderValues.LITTLE_ENDIAN);
    } else if (byteOrderWKB == WKBConstants.wkbXDR) {
      dis.setOrder(ByteOrderValues.BIG_ENDIAN);
    } else if (isStrict) {
      throw new ParseException("Unknown geometry byte order (not NDR or XDR): " + byteOrderWKB);
    }
    // if not strict and not XDR or NDR, then we just use the dis default set at the
    // start of the geometry (if a multi-geometry).  This  allows WBKReader to work
    // with Spatialite native BLOB WKB, as well as other WKB variants that might just
    // specify endian-ness at the start of the multigeometry.

    int typeInt = dis.readInt();

    /**
     * To get geometry type mask out EWKB flag bits, and use only low 3 digits of type word. This
     * supports both EWKB and ISO/OGC.
     */
    int geometryType = (typeInt & 0xffff) % 1000;

    // handle 3D and 4D WKB geometries
    // geometries with Z coordinates have the 0x80 flag (postgis EWKB)
    // or are in the 1000 range (Z) or in the 3000 range (ZM) of geometry type (ISO/OGC 06-103r4)
    boolean hasZ =
        ((typeInt & 0x80000000) != 0
            || (typeInt & 0xffff) / 1000 == 1
            || (typeInt & 0xffff) / 1000 == 3);
    // geometries with M coordinates have the 0x40 flag (postgis EWKB)
    // or are in the 1000 range (M) or in the 3000 range (ZM) of geometry type (ISO/OGC 06-103r4)
    boolean hasM =
        ((typeInt & 0x40000000) != 0
            || (typeInt & 0xffff) / 1000 == 2
            || (typeInt & 0xffff) / 1000 == 3);
    // System.out.println(typeInt + " - " + geometryType + " - hasZ:" + hasZ);
    inputDimension = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);

    EnumSet<Ordinate> ordinateFlags = EnumSet.of(Ordinate.X, Ordinate.Y);
    if (hasZ) {
      ordinateFlags.add(Ordinate.Z);
    }
    if (hasM) {
      ordinateFlags.add(Ordinate.M);
    }

    // determine if SRIDs are present (EWKB only)
    boolean hasSRID = (typeInt & 0x20000000) != 0;
    if (hasSRID) {
      SRID = dis.readInt();
    }

    // only allocate ordValues buffer if necessary
    if (ordValues == null || ordValues.length < inputDimension)
      ordValues = new double[inputDimension];

    org.apache.sedona.common.S2Geography.Geography geog = null;
    switch (geometryType) {
      case WKBConstants.wkbPoint:
        geog = readPoint(ordinateFlags);
        break;
      case WKBConstants.wkbLineString:
        geog = readPolyline(ordinateFlags);
        break;
      case WKBConstants.wkbPolygon:
        geog = readPolygon(ordinateFlags);
        break;
      case WKBConstants.wkbMultiPoint:
        geog = readMultiPoint(SRID);
        break;
      case WKBConstants.wkbMultiLineString:
        geog = readMultiPolyline(SRID);
        break;
      case WKBConstants.wkbMultiPolygon:
        geog = readMultiPolygon(SRID);
        break;
      case WKBConstants.wkbGeometryCollection:
        geog = readGeographyCollection(SRID);
        break;
      default:
        throw new ParseException("Unknown WKB type " + geometryType);
    }
    setSRID(geog, SRID);
    return geog;
  }

  /**
   * Sets the SRID, if it was specified in the WKB
   *
   * @param g the geometry to update
   * @return the geometry with an updated SRID value, if required
   */
  private org.apache.sedona.common.S2Geography.Geography setSRID(
      org.apache.sedona.common.S2Geography.Geography g, int SRID) {
    if (SRID != 0) g.setSRID(SRID);
    return g;
  }

  private SinglePointGeography readPoint(EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    CoordinateSequence pts = readCoordinateSequence(1, ordinateFlags);
    // If X and Y are NaN create a empty point
    if (pts.size() <= 0 || Double.isNaN(pts.getX(0)) || Double.isNaN(pts.getY(0))) {
      return new SinglePointGeography();
    }
    double lon = pts.getX(0);
    double lat = pts.getY(0);
    S2Point s2Point = S2LatLng.fromDegrees(lat, lon).toPoint();

    // Build via S2Builder + S2PointVectorLayer
    S2Builder builder = new S2Builder.Builder().build();
    S2PointVectorLayer layer = new S2PointVectorLayer();
    builder.startLayer(layer);
    builder.addPoint(s2Point);

    // must call build() before reading out the points
    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("Failed to build S2 point layer: " + error.text());
    }
    // Extract the resulting points
    List<S2Point> points = layer.getPointVector();
    if (points.isEmpty()) {
      return new SinglePointGeography();
    }
    return new SinglePointGeography(points.get(0));
  }

  private SinglePolylineGeography readPolyline(EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    int size = readNumField(FIELD_NUMCOORDS);
    CoordinateSequence seq = readCoordinateSequenceLineString(size, ordinateFlags);
    if (seq.size() < 2) {
      // empty or extended-but-all-NaN → empty geography
      return new SinglePolylineGeography();
    }

    List<S2Point> pts = new ArrayList<>(seq.size());
    for (int i = 0; i < seq.size(); i++) {
      double lon = seq.getX(i);
      double lat = seq.getY(i);
      pts.add(S2LatLng.fromDegrees(lat, lon).toPoint());
    }

    S2Builder builder = new S2Builder.Builder().build();
    S2PolylineLayer layer = new S2PolylineLayer();
    builder.startLayer(layer);

    builder.addPolyline(new S2Polyline(pts));

    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("Failed to build S2 polyline: " + error.text());
    }
    S2Polyline s2poly = layer.getPolyline();
    return new SinglePolylineGeography(s2poly);
  }

  private PolygonGeography readPolygon(EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    int numRings = readNumField(FIELD_NUMRINGS);
    if (numRings <= 0) {
      return new PolygonGeography();
    }

    List<S2Loop> loops = new ArrayList<>(numRings);
    for (int r = 0; r < numRings; r++) {
      int coordCount = readNumField(FIELD_NUMCOORDS);
      CoordinateSequence seq = readCoordinateSequenceRing(coordCount, ordinateFlags);
      // build the loop
      List<S2Point> pts = new ArrayList<>(seq.size());
      for (int i = 0; i < seq.size(); i++) {
        pts.add(S2LatLng.fromDegrees(seq.getY(i), seq.getX(i)).toPoint());
      }
      loops.add(new S2Loop(pts));
    }
    if (loops.isEmpty()) {
      return new PolygonGeography();
    }

    // Now feed those loops into S2Builder + S2PolygonLayer:
    S2Builder builder = new S2Builder.Builder().build();
    S2PolygonLayer polyLayer = new S2PolygonLayer();
    builder.startLayer(polyLayer);

    // add shell + holes
    for (S2Loop loop : loops) {
      builder.addLoop(loop);
    }

    // build
    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("S2Builder failed: " + error.text());
    }

    // extract the stitched polygon
    S2Polygon s2poly = polyLayer.getPolygon();

    // wrap in your PolygonGeography
    return new PolygonGeography(s2poly);
  }

  private PointGeography readMultiPoint(int SRID) throws IOException, ParseException {
    int numGeom = readNumField(FIELD_NUMELEMS);
    List<S2Point> pts = new ArrayList<>(numGeom);
    for (int i = 0; i < numGeom; i++) {
      org.apache.sedona.common.S2Geography.Geography point = readGeometry(SRID);
      if (!(point instanceof PointGeography)) {
        throw new ParseException(INVALID_GEOM_TYPE_MSG + "MultiPoint");
      }
      pts.addAll(((PointGeography) point).getPoints());
    }
    return new PointGeography(pts);
  }

  private PolylineGeography readMultiPolyline(int SRID) throws IOException, ParseException {
    int numGeom = readNumField(FIELD_NUMELEMS);
    List<S2Polyline> polylines = new ArrayList<>(numGeom);
    for (int i = 0; i < numGeom; i++) {
      org.apache.sedona.common.S2Geography.Geography polyline = readGeometry(SRID);
      if (!(polyline instanceof PolylineGeography)) {
        throw new ParseException(INVALID_GEOM_TYPE_MSG + "MultiPolyline");
      }
      polylines.addAll(((PolylineGeography) polyline).getPolylines());
    }
    return new PolylineGeography(polylines);
  }

  private MultiPolygonGeography readMultiPolygon(int SRID) throws IOException, ParseException {
    int numGeom = readNumField(FIELD_NUMELEMS);
    List<S2Polygon> polygons = new ArrayList<>(numGeom);
    for (int i = 0; i < numGeom; i++) {
      org.apache.sedona.common.S2Geography.Geography geom = readGeometry(SRID);
      polygons.add(((PolygonGeography) geom).polygon);
    }
    return new MultiPolygonGeography(Geography.GeographyKind.MULTIPOLYGON, polygons);
  }

  private GeographyCollection readGeographyCollection(int SRID) throws IOException, ParseException {
    int numGeom = readNumField(FIELD_NUMELEMS);
    org.apache.sedona.common.S2Geography.Geography[] geoms =
        new org.apache.sedona.common.S2Geography.Geography[numGeom];
    for (int i = 0; i < numGeom; i++) {
      geoms[i] = readGeometry(SRID);
    }
    return new GeographyCollection(List.of(geoms));
  }

  private CoordinateSequence readCoordinateSequence(int size, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    CoordinateSequence seq =
        csFactory.create(size, inputDimension, ordinateFlags.contains(Ordinate.M) ? 1 : 0);
    int targetDim = seq.getDimension();
    if (targetDim > inputDimension) targetDim = inputDimension;
    for (int i = 0; i < size; i++) {
      readCoordinate();
      for (int j = 0; j < targetDim; j++) {
        seq.setOrdinate(i, j, ordValues[j]);
      }
    }
    return seq;
  }

  private CoordinateSequence readCoordinateSequenceLineString(
      int size, EnumSet<Ordinate> ordinateFlags) throws IOException, ParseException {
    CoordinateSequence seq = readCoordinateSequence(size, ordinateFlags);
    if (isStrict) return seq;
    if (seq.size() == 0 || seq.size() >= 2) return seq;
    return CoordinateSequences.extend(csFactory, seq, 2);
  }

  private CoordinateSequence readCoordinateSequenceRing(int size, EnumSet<Ordinate> ordinateFlags)
      throws IOException, ParseException {
    CoordinateSequence seq = readCoordinateSequence(size, ordinateFlags);
    if (isStrict) return seq;
    if (CoordinateSequences.isRing(seq)) return seq;
    return CoordinateSequences.ensureValidRing(csFactory, seq);
  }

  /**
   * Reads a coordinate value with the specified dimensionality. Makes the X and Y ordinates precise
   * according to the precision model in use.
   *
   * @throws ParseException
   */
  private void readCoordinate() throws IOException, ParseException {
    for (int i = 0; i < inputDimension; i++) {
      if (i <= 1) {
        try {
          double v = dis.readDouble();
          ordValues[i] = precisionModel.makePrecise(v);
        } catch (ParseException pe) {
          return;
        }
      } else {
        ordValues[i] = dis.readDouble();
      }
    }
  }
}
