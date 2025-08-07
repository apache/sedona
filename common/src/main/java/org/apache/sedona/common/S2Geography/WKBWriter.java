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

import com.google.common.geometry.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.*;

public class WKBWriter {
  /** A custom modified version based on JTS WKBWriter */
  /**
   * Converts a byte array to a hexadecimal string.
   *
   * @param bytes
   * @return a string of hexadecimal digits
   * @deprecated
   */
  public static String bytesToHex(byte[] bytes) {
    return toHex(bytes);
  }

  /**
   * Converts a byte array to a hexadecimal string.
   *
   * @param bytes a byte array
   * @return a string of hexadecimal digits
   */
  public static String toHex(byte[] bytes) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      buf.append(toHexDigit((b >> 4) & 0x0F));
      buf.append(toHexDigit(b & 0x0F));
    }
    return buf.toString();
  }

  private static char toHexDigit(int n) {
    if (n < 0 || n > 15) throw new IllegalArgumentException("Nibble value out of range: " + n);
    if (n <= 9) return (char) ('0' + n);
    return (char) ('A' + (n - 10));
  }

  private EnumSet<Ordinate> outputOrdinates;
  private int outputDimension = 2;
  private int byteOrder;
  private boolean includeSRID = false;
  private ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
  private OutStream byteArrayOutStream = new OutputStreamOutStream(byteArrayOS);
  // holds output data values
  private byte[] buf = new byte[8];

  /**
   * Creates a writer that writes {@link Geometry}s with output dimension = 2 and BIG_ENDIAN byte
   * order
   */
  public WKBWriter() {
    this(2, ByteOrderValues.BIG_ENDIAN);
  }

  /**
   * Creates a writer that writes {@link Geometry}s with the given dimension (2 or 3) for output
   * coordinates and {@link ByteOrderValues#BIG_ENDIAN} byte order. If the input geometry has a
   * small coordinate dimension, coordinates will be padded with {@link Coordinate#NULL_ORDINATE}.
   *
   * @param outputDimension the coordinate dimension to output (2 or 3)
   */
  public WKBWriter(int outputDimension) {
    this(outputDimension, ByteOrderValues.BIG_ENDIAN);
  }

  public WKBWriter(int outputDimension, boolean includeSRID) {
    this(outputDimension, ByteOrderValues.BIG_ENDIAN, includeSRID);
  }

  public WKBWriter(int outputDimension, int byteOrder) {
    this(outputDimension, byteOrder, false);
  }

  public WKBWriter(int outputDimension, int byteOrder, boolean includeSRID) {
    this.outputDimension = outputDimension;
    this.byteOrder = byteOrder;
    this.includeSRID = includeSRID;

    if (outputDimension < 2 || outputDimension > 4)
      throw new IllegalArgumentException("Output dimension must be 2 to 4");

    this.outputOrdinates = EnumSet.of(Ordinate.X, Ordinate.Y);
    if (outputDimension > 2) outputOrdinates.add(Ordinate.Z);
    if (outputDimension > 3) outputOrdinates.add(Ordinate.M);
  }

  /**
   * Sets the {@link Ordinate} that are to be written. Possible members are:
   *
   * <ul>
   *   <li>{@link Ordinate#X}
   *   <li>{@link Ordinate#Y}
   *   <li>{@link Ordinate#Z}
   *   <li>{@link Ordinate#M}
   * </ul>
   *
   * Values of {@link Ordinate#X} and {@link Ordinate#Y} are always assumed and not particularly
   * checked for.
   *
   * @param outputOrdinates A set of {@link Ordinate} values
   */
  public void setOutputOrdinates(EnumSet<Ordinate> outputOrdinates) {

    this.outputOrdinates.remove(Ordinate.Z);
    this.outputOrdinates.remove(Ordinate.M);

    if (this.outputDimension == 3) {
      if (outputOrdinates.contains(Ordinate.Z)) this.outputOrdinates.add(Ordinate.Z);
      else if (outputOrdinates.contains(Ordinate.M)) this.outputOrdinates.add(Ordinate.M);
    }
    if (this.outputDimension == 4) {
      if (outputOrdinates.contains(Ordinate.Z)) this.outputOrdinates.add(Ordinate.Z);
      if (outputOrdinates.contains(Ordinate.M)) this.outputOrdinates.add(Ordinate.M);
    }
  }

  /**
   * Gets a bit-pattern defining which ordinates should be
   *
   * @return an ordinate bit-pattern
   * @see #setOutputOrdinates(EnumSet)
   */
  public EnumSet<Ordinate> getOutputOrdinates() {
    return this.outputOrdinates;
  }

  /**
   * Writes a {@link org.apache.sedona.common.S2Geography.Geography} into a byte array.
   *
   * @param geog the geometry to write
   * @return the byte array containing the WKB
   */
  public byte[] write(Geography geog) {
    try {
      byteArrayOS.reset();
      write(geog, byteArrayOutStream);
    } catch (IOException ex) {
      throw new RuntimeException("Unexpected IO exception: " + ex.getMessage());
    }
    return byteArrayOS.toByteArray();
  }

  /**
   * Writes a {@link Geography} to an {@link OutStream}.
   *
   * @param geogIn the geography to write
   * @param os the out stream to write to
   * @throws IOException if an I/O error occurs
   */
  public void write(Geography geogIn, OutStream os) throws IOException {
    org.apache.sedona.common.S2Geography.Geography geog = geogIn;
    if (geog instanceof SinglePointGeography) {
      writePoint(WKBConstants.wkbPoint, (SinglePointGeography) geog, os);
    } else if (geog instanceof PointGeography) {
      writeMultiPoint(WKBConstants.wkbMultiPoint, (PointGeography) geog, os);
    } else if (geog instanceof SinglePolylineGeography) {
      writePolyline(WKBConstants.wkbLineString, (SinglePolylineGeography) geog, os);
    } else if (geog instanceof PolylineGeography) {
      writeMultiPolyline(WKBConstants.wkbMultiLineString, (PolylineGeography) geog, os);
    } else if (geog instanceof PolygonGeography) {
      writePolygon(WKBConstants.wkbPolygon, (PolygonGeography) geog, os);
    } else if (geog instanceof MultiPolygonGeography) {
      writeMultiPolygon(WKBConstants.wkbMultiPolygon, (MultiPolygonGeography) geog, os);
    } else if (geog instanceof GeographyCollection) {
      writeGeographyCollection(WKBConstants.wkbGeometryCollection, (GeographyCollection) geog, os);
    }
  }

  private void writePoint(int geometryType, SinglePointGeography pt, OutStream os)
      throws IOException {
    writeByteOrder(os);
    writeGeometryType(geometryType, pt, os);
    if (pt.numShapes() == 0) {
      writeNaNs(outputDimension, os);
    } else {
      S2Point p = pt.getPoints().get(0);
      S2LatLng ll = new S2LatLng(p);

      double lon = ll.lngDegrees();
      double lat = ll.latDegrees();

      ByteOrderValues.putDouble(lon, buf, byteOrder);
      os.write(buf, 8);
      ByteOrderValues.putDouble(lat, buf, byteOrder);
      os.write(buf, 8);
    }
  }

  private void writeMultiPoint(int geometryType, PointGeography mp, OutStream os)
      throws IOException {
    // 1) write the outer MultiPoint header
    writeByteOrder(os);
    writeGeometryType(WKBConstants.wkbMultiPoint, mp, os);
    writeInt(mp.numShapes(), os); // count = number of points

    // 2) temporarily disable SRID on the nested shapes
    boolean oldIncludeSRID = this.includeSRID;
    this.includeSRID = false;

    // 3) for each point, write a proper Point WKB
    for (int i = 0; i < mp.numShapes(); i++) {
      S2Point p = mp.getPoints().get(i);
      S2LatLng ll = new S2LatLng(p);
      double lon = ll.lngDegrees();
      double lat = ll.latDegrees();

      // nested Point header
      writeByteOrder(os);
      writeGeometryType(WKBConstants.wkbPoint, mp, os);

      // coords
      ByteOrderValues.putDouble(lon, buf, byteOrder);
      os.write(buf, 8);
      ByteOrderValues.putDouble(lat, buf, byteOrder);
      os.write(buf, 8);
    }

    // 4) restore SRID flag
    this.includeSRID = oldIncludeSRID;
  }

  private void writePolyline(int geometryType, SinglePolylineGeography polyline, OutStream os)
      throws IOException {
    writeByteOrder(os);
    writeGeometryType(geometryType, polyline, os);
    List<S2Polyline> s2line = polyline.getPolylines();
    for (S2Polyline s2 : s2line) {
      List<S2Point> verts = s2.vertices();
      writeInt(verts.size(), os);
      for (S2Point p : verts) {
        // round to 6 decimal places
        S2LatLng ll = new S2LatLng(p);
        double lon = ll.lngDegrees();
        double lat = ll.latDegrees();

        ByteOrderValues.putDouble(lon, buf, byteOrder);
        os.write(buf, 8);
        ByteOrderValues.putDouble(lat, buf, byteOrder);
        os.write(buf, 8);
      }
    }
  }

  private void writeMultiPolyline(int geometryType, PolylineGeography polyline, OutStream os)
      throws IOException {
    // 1) Outer MultiLineString header
    writeByteOrder(os);
    writeGeometryType(WKBConstants.wkbMultiLineString, polyline, os);

    List<S2Polyline> lines = polyline.getPolylines();
    // 2) Write the number of LineStrings
    writeInt(lines.size(), os);

    // Temporarily disable SRID on nested shapes
    boolean oldIncludeSRID = this.includeSRID;
    this.includeSRID = false;

    // 3) For each LineString:
    for (S2Polyline s2line : lines) {
      List<S2Point> verts = s2line.vertices();

      // 3a) Nested LineString header
      writeByteOrder(os);
      writeGeometryType(WKBConstants.wkbLineString, polyline, os);

      // 3b) Vertex count
      writeInt(verts.size(), os);

      // 3c) Coordinates
      for (S2Point p : verts) {
        S2LatLng ll = new S2LatLng(p);
        double lon = ll.lngDegrees();
        double lat = ll.latDegrees();

        ByteOrderValues.putDouble(lon, buf, byteOrder);
        os.write(buf, 8);
        ByteOrderValues.putDouble(lat, buf, byteOrder);
        os.write(buf, 8);
      }
    }

    // 4) Restore SRID flag
    this.includeSRID = oldIncludeSRID;
  }

  private void writePolygon(int geometryType, PolygonGeography poly, OutStream os)
      throws IOException {
    writeByteOrder(os);
    writeGeometryType(geometryType, poly, os);
    S2Polygon s2poly = poly.polygon;
    List<S2Loop> loops = s2poly.getLoops();
    writeInt(loops.size(), os);
    for (S2Loop loop : loops) {
      int n = loop.numVertices();
      writeInt(n, os);
      for (int i = 0; i < n; i++) {
        S2LatLng ll = new S2LatLng(loop.vertex(i));
        double lon = ll.lngDegrees();
        double lat = ll.latDegrees();

        ByteOrderValues.putDouble(lon, buf, byteOrder);
        os.write(buf, 8);
        ByteOrderValues.putDouble(lat, buf, byteOrder);
        os.write(buf, 8);
      }
    }
  }

  private void writeMultiPolygon(int geometryType, MultiPolygonGeography multiPoly, OutStream os)
      throws IOException {
    // 1) Outer MultiPolygon header
    writeByteOrder(os);
    writeGeometryType(WKBConstants.wkbMultiPolygon, multiPoly, os);

    // 2) Number of polygons
    List<org.apache.sedona.common.S2Geography.Geography> polys =
        multiPoly.getFeatures(); // however you expose each sub-polygon
    writeInt(polys.size(), os);

    // 3) Disable SRID on nested shapes if you include SRID in the outer header
    boolean oldIncludeSRID = this.includeSRID;
    this.includeSRID = false;

    // 4) For each polygon, write a full Polygon WKB
    for (org.apache.sedona.common.S2Geography.Geography pg : polys) {
      // 4a) Nested Polygon header
      writeByteOrder(os);
      writeGeometryType(WKBConstants.wkbPolygon, pg, os);

      // 4b) Number of rings
      PolygonGeography s2poly = (PolygonGeography) pg;
      List<S2Loop> loops = s2poly.polygon.getLoops();
      writeInt(loops.size(), os);

      // 4c) For each ring, write vertex count + coords
      for (S2Loop loop : loops) {
        int n = loop.numVertices();
        writeInt(n, os);
        for (int i = 0; i < n; i++) {
          S2LatLng ll = new S2LatLng(loop.vertex(i));
          double lon = ll.lngDegrees();
          double lat = ll.latDegrees();

          // X (lon) then Y (lat)—matches Point/LineString ordering
          ByteOrderValues.putDouble(lon, buf, byteOrder);
          os.write(buf, 8);
          ByteOrderValues.putDouble(lat, buf, byteOrder);
          os.write(buf, 8);
        }
      }
    }

    // 5) Restore SRID flag
    this.includeSRID = oldIncludeSRID;
  }

  private void writeGeographyCollection(int geometryType, GeographyCollection gc, OutStream os)
      throws IOException {
    writeByteOrder(os);
    writeGeometryType(geometryType, gc, os);
    writeInt(gc.numShapes(), os);
    boolean originalIncludeSRID = this.includeSRID;
    this.includeSRID = false;
    for (int i = 0; i < gc.numShapes(); i++) {
      write(gc.getFeatures().get(i), os);
    }
    this.includeSRID = originalIncludeSRID;
  }

  private void writeByteOrder(OutStream os) throws IOException {
    if (byteOrder == ByteOrderValues.LITTLE_ENDIAN) buf[0] = WKBConstants.wkbNDR;
    else buf[0] = WKBConstants.wkbXDR;
    os.write(buf, 1);
  }

  private void writeGeometryType(
      int geometryType, org.apache.sedona.common.S2Geography.Geography g, OutStream os)
      throws IOException {
    int ordinals = 0;
    if (outputOrdinates.contains(Ordinate.Z)) {
      ordinals = ordinals | 0x80000000;
    }

    if (outputOrdinates.contains(Ordinate.M)) {
      ordinals = ordinals | 0x40000000;
    }

    int flag3D = (outputDimension > 2) ? ordinals : 0;
    int typeInt = geometryType | flag3D;
    typeInt |= includeSRID ? 0x20000000 : 0;
    writeInt(typeInt, os);
    if (includeSRID) {
      writeInt(g.getSRID(), os);
    }
  }

  private void writeInt(int intValue, OutStream os) throws IOException {
    ByteOrderValues.putInt(intValue, buf, byteOrder);
    os.write(buf, 4);
  }

  private void writeCoordinateSequence(CoordinateSequence seq, boolean writeSize, OutStream os)
      throws IOException {
    if (writeSize) writeInt(seq.size(), os);

    for (int i = 0; i < seq.size(); i++) {
      writeCoordinate(seq, i, os);
    }
  }

  private void writeCoordinate(CoordinateSequence seq, int index, OutStream os) throws IOException {
    ByteOrderValues.putDouble(seq.getX(index), buf, byteOrder);
    os.write(buf, 8);
    ByteOrderValues.putDouble(seq.getY(index), buf, byteOrder);
    os.write(buf, 8);

    // only write 3rd dim if caller has requested it for this writer
    if (outputDimension >= 3) {
      // if 3rd dim is requested, only write it if the CoordinateSequence provides it
      double ordVal = seq.getOrdinate(index, 2);
      ByteOrderValues.putDouble(ordVal, buf, byteOrder);
      os.write(buf, 8);
    }
    // only write 4th dim if caller has requested it for this writer
    if (outputDimension == 4) {
      // if 4th dim is requested, only write it if the CoordinateSequence provides it
      double ordVal = seq.getOrdinate(index, 3);
      ByteOrderValues.putDouble(ordVal, buf, byteOrder);
      os.write(buf, 8);
    }
  }

  private void writeNaNs(int numNaNs, OutStream os) throws IOException {
    for (int i = 0; i < numNaNs; i++) {
      ByteOrderValues.putDouble(Double.NaN, buf, byteOrder);
      os.write(buf, 8);
    }
  }
}
