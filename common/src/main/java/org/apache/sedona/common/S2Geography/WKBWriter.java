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
   * Writes a {@link S2Geography} into a byte array.
   *
   * @param geog the geometry to write
   * @return the byte array containing the WKB
   */
  public byte[] write(S2Geography geog) {
    try {
      byteArrayOS.reset();
      write(geog, byteArrayOutStream);
    } catch (IOException ex) {
      throw new RuntimeException("Unexpected IO exception: " + ex.getMessage());
    }
    return byteArrayOS.toByteArray();
  }

  /**
   * Writes a {@link S2Geography} to an {@link OutStream}.
   *
   * @param geog the geometry to write
   * @param os the out stream to write to
   * @throws IOException if an I/O error occurs
   */
  public void write(S2Geography geog, OutStream os) throws IOException {
    int dim = geog.dimension(); //  0=point, 1=linestring, 2=polygon, -1=mixed/empty
    int count = geog.numShapes(); //  1=single, >1=multi

    String s =
        "Cannot write geometry with dimension="
            + dim
            + " and shape count="
            + count
            + " (type: "
            + geog.getClass().getName()
            + ")";

    // 1) Mixed or empty collection
    if (dim == -1) {
      if (geog instanceof GeographyCollection) {
        writeGeographyCollection(
            WKBConstants.wkbGeometryCollection, (GeographyCollection) geog, os);
        return;
      }
      throw new IllegalArgumentException(s);
    }

    // 2) Dispatch on the real dimensions
    switch (dim) {
      case 0: // POINT or MULTI_POINT
        if (count == 1) {
          // exactly one point
          writePoint(WKBConstants.wkbPoint, (SinglePointGeography) geog, os);
        } else {
          // more than one → treat as multipoint
          writePoint(WKBConstants.wkbMultiPoint, (PointGeography) geog, os);
        }
        return;

      case 1: // LINESTRING or MULTI_LINESTRING
        if (count == 1) {
          writePolyline(WKBConstants.wkbLineString, (SinglePolylineGeography) geog, os);
        } else {
          writePolyline(WKBConstants.wkbMultiLineString, (PolylineGeography) geog, os);
        }
        return;

      case 2: // POLYGON or MULTI_POLYGON
        if (count == 1) {
          writePolygon(WKBConstants.wkbPolygon, (PolygonGeography) geog, os);
        } else {
          writeGeographyCollection(WKBConstants.wkbMultiPolygon, (MultiPolygonGeography) geog, os);
        }
        return;

      default:
        // Should never happen once dim ∈ {0,1,2,-1} is enforced
        throw new IllegalArgumentException(s);
    }
  }

  private void writePoint(int geometryType, PointGeography pt, OutStream os) throws IOException {
    writeByteOrder(os);
    writeGeometryType(geometryType, pt, os);
    if (pt.numShapes() == 0) {
      writeNaNs(outputDimension, os);
    } else {
      S2Point p = pt.getPoints().get(0);
      S2LatLng ll = new S2LatLng(p);

      // round to 6 decimal places //TODO: 16 decimal
      double lon = Math.rint(ll.lngDegrees() * 1e16) / 1e16;
      double lat = Math.rint(ll.latDegrees() * 1e16) / 1e16;

      ByteOrderValues.putDouble(lon, buf, byteOrder);
      os.write(buf, 8);
      ByteOrderValues.putDouble(lat, buf, byteOrder);
      os.write(buf, 8);
    }
  }

  private void writePolyline(int geometryType, PolylineGeography polyline, OutStream os)
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
        double lon = Math.rint(ll.lngDegrees() * 1e16) / 1e16;
        double lat = Math.rint(ll.latDegrees() * 1e16) / 1e16;

        ByteOrderValues.putDouble(lon, buf, byteOrder);
        os.write(buf, 8);
        ByteOrderValues.putDouble(lat, buf, byteOrder);
        os.write(buf, 8);
      }
    }
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
        double lon = Math.rint(ll.lngDegrees() * 1e16) / 1e16;
        double lat = Math.rint(ll.latDegrees() * 1e16) / 1e16;

        ByteOrderValues.putDouble(lat, buf, byteOrder);
        os.write(buf, 8);
        ByteOrderValues.putDouble(lon, buf, byteOrder);
        os.write(buf, 8);
      }
    }
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

  private void writeGeometryType(int geometryType, S2Geography g, OutStream os) throws IOException {
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
