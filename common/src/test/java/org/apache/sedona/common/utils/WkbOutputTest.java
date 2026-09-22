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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.geometrySerde.GeometrySerializer;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

public class WkbOutputTest {
  private static final GeometryFactory FACTORY = new GeometryFactory();
  private static final Layout XY = new Layout(2, 0, 0);
  private static final Layout XYZ = new Layout(3, 0, 0x80000000);
  private static final Layout XYM = new Layout(3, 1, 0x40000000);
  private static final Layout XYZM = new Layout(4, 1, 0xc0000000);

  @Test
  public void emptyPrimitiveLayoutsSurviveWkbOutputAfterSerialization() throws ParseException {
    for (Layout layout : new Layout[] {XY, XYZ, XYM, XYZM}) {
      for (int primitive = 1; primitive <= 3; primitive++) {
        assertOutputs(roundTrip(readPrimitive(primitive, layout, true)), primitive, layout, true);
      }
    }
  }

  @Test
  public void allNanZAndMLayoutsSurviveWkbOutputAfterSerialization() throws ParseException {
    for (Layout layout : new Layout[] {XY, XYZ, XYM, XYZM}) {
      assertOutputs(roundTrip(readPrimitive(1, layout, false)), 1, layout, false);
    }
  }

  @Test
  public void measuredValuesAreWrittenInTheirOwnOrdinate() throws ParseException {
    for (Layout layout : new Layout[] {XYM, XYZM}) {
      Point source = (Point) readPrimitive(1, layout, false);
      source.getCoordinateSequence().setOrdinate(0, layout.dimension - 1, 9);
      if (layout == XYZM) source.getCoordinateSequence().setOrdinate(0, 2, 3);
      byte[] bytes = GeomUtils.getEWKB(roundTrip(source));
      assertHeader(bytes, 1 | layout.flags | 0x20000000, 4326);
      Point output = (Point) Constructors.geomFromWKB(bytes);
      assertSequence(output.getCoordinateSequence(), layout);
      assertEquals(9, output.getCoordinateSequence().getM(0), 0);
      if (layout == XYZM) assertEquals(3, output.getCoordinateSequence().getZ(0), 0);
      else assertTrue(Double.isNaN(output.getCoordinateSequence().getZ(0)));
    }
  }

  @Test
  public void measuredWktRetainsMInEveryOutputFormat() throws ParseException {
    String[] inputs = {
      "POINT M (1 2 9)", "POINT ZM (1 2 3 9)", "LINESTRING M (1 2 9, 4 5 6)", "POINT M EMPTY"
    };
    Layout[] layouts = {XYM, XYZM, XYM, XYM};
    for (int i = 0; i < inputs.length; i++) {
      Geometry source = new WKTReader().read(inputs[i]);
      source.setSRID(4326);
      CoordinateSequence expected = coordinateSequence(source);
      for (Geometry geometry : new Geometry[] {source, roundTrip(source)}) {
        byte[][] outputs = outputVariants(geometry);
        for (int format = 0; format < outputs.length; format++) {
          int srid = format == 0 ? 0 : 4326;
          int type = source instanceof Point ? 1 : 2;
          assertHeader(
              outputs[format], type | layouts[i].flags | (srid == 0 ? 0 : 0x20000000), srid);
          Geometry output = Constructors.geomFromWKB(outputs[format]);
          assertEquals(source.isEmpty(), output.isEmpty());
          CoordinateSequence actual = coordinateSequence(output);
          assertSequence(actual, layouts[i]);
          assertEquals(expected.size(), actual.size());
          for (int point = 0; point < expected.size(); point++) {
            for (int ordinate = 0; ordinate < layouts[i].dimension; ordinate++) {
              assertEquals(
                  expected.getOrdinate(point, ordinate), actual.getOrdinate(point, ordinate), 0);
            }
          }
        }
      }
    }
  }

  @Test
  public void multipartOutputUsesSharedLayoutForEmptyMembers() throws ParseException {
    String[] inputs = {
      "MULTIPOINT Z (EMPTY, (1 2 3))",
      "MULTILINESTRING Z (EMPTY, (0 0 1, 1 1 2))",
      "MULTIPOLYGON Z (EMPTY, ((0 0 1, 1 0 2, 1 1 3, 0 0 1)))"
    };
    for (int i = 0; i < inputs.length; i++) {
      Geometry source = new WKTReader().read(inputs[i]);
      source.setSRID(4326);
      byte[][] outputs = outputVariants(source);
      for (int format = 0; format < outputs.length; format++) {
        int srid = format == 0 ? 0 : 4326;
        assertHeader(outputs[format], (4 + i) | XYZ.flags | (srid == 0 ? 0 : 0x20000000), srid);
        Geometry output = roundTrip(Constructors.geomFromWKB(outputs[format]));
        assertEquals(srid, output.getSRID());
        assertEquals(2, output.getNumGeometries());
        assertTrue(output.getGeometryN(0).isEmpty());
        assertSequence(coordinateSequence(output.getGeometryN(0)), XYZ);
        CoordinateSequence expected = coordinateSequence(source.getGeometryN(1));
        CoordinateSequence actual = coordinateSequence(output.getGeometryN(1));
        assertSequence(actual, XYZ);
        assertEquals(expected.size(), actual.size());
        for (int point = 0; point < expected.size(); point++) {
          for (int ordinate = 0; ordinate < 3; ordinate++) {
            assertEquals(
                expected.getOrdinate(point, ordinate), actual.getOrdinate(point, ordinate), 0);
          }
        }
      }
    }
  }

  @Test
  public void outputFindsZBeyondFirstCoordinateAndKeepsOrdinaryCoordinatesXy()
      throws ParseException {
    LineString source =
        FACTORY.createLineString(new Coordinate[] {new Coordinate(1, 2), new Coordinate(3, 4, 5)});
    for (Geometry line : new Geometry[] {source, roundTrip(source)}) {
      byte[] bytes = GeomUtils.getWKB(line);
      assertHeader(bytes, 2 | XYZ.flags, 0);
      CoordinateSequence sequence =
          ((LineString) Constructors.geomFromWKB(bytes)).getCoordinateSequence();
      assertEquals(3, sequence.getDimension());
      assertTrue(Double.isNaN(sequence.getZ(0)));
      assertEquals(5, sequence.getZ(1), 0);
    }
    Geometry ordinary = FACTORY.createPoint(new Coordinate(1, 2));
    assertHeader(GeomUtils.getWKB(ordinary), 1, 0);
    assertHeader(GeomUtils.getEWKB(roundTrip(ordinary)), 1, 0);
  }

  @Test
  public void geometryCollectionMembersRetainTheirOwnLayouts() throws ParseException {
    Geometry collection =
        roundTrip(
            FACTORY.createGeometryCollection(
                new Geometry[] {
                  readPrimitive(1, XY, false),
                  readPrimitive(1, XYZ, true),
                  FACTORY.createGeometryCollection(new Geometry[] {readPrimitive(1, XYM, false)})
                }));
    byte[] bytes = GeomUtils.getWKB(collection);
    assertHeader(bytes, 7 | XYZM.flags, 0);
    Geometry output = Constructors.geomFromWKB(bytes);
    assertSequence(((Point) output.getGeometryN(0)).getCoordinateSequence(), XY);
    assertSequence(((Point) output.getGeometryN(1)).getCoordinateSequence(), XYZ);
    assertTrue(output.getGeometryN(1).isEmpty());
    assertSequence(((Point) output.getGeometryN(2).getGeometryN(0)).getCoordinateSequence(), XYM);
  }

  @Test
  public void nullOutputAndExplicitTwoDimensionalWriterKeepTheirBehavior() throws ParseException {
    assertNull(GeomUtils.getWKB(null));
    assertNull(GeomUtils.getEWKB(null));
    WKBWriter writer = GeomUtils.createWKBWriter(2, true);
    assertHeader(writer.write(readPrimitive(1, XYZM, false)), 0x20000001, 4326);
  }

  private static void assertOutputs(Geometry geometry, int primitive, Layout layout, boolean empty)
      throws ParseException {
    byte[] wkb = GeomUtils.getWKB(geometry);
    byte[] ewkb = GeomUtils.getEWKB(geometry);
    int nativeOrder = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0;
    assertEquals(nativeOrder, wkb[0]);
    assertEquals(nativeOrder, ewkb[0]);
    assertOutput(wkb, primitive, layout, 0, empty);
    assertOutput(ewkb, primitive, layout, 4326, empty);
    for (int endian : new int[] {ByteOrderValues.LITTLE_ENDIAN, ByteOrderValues.BIG_ENDIAN}) {
      byte[] bytes = WKBReader.hexToBytes(GeomUtils.getHexEWKB(geometry, endian));
      assertEquals(endian == ByteOrderValues.LITTLE_ENDIAN ? 1 : 0, bytes[0]);
      assertOutput(bytes, primitive, layout, 4326, empty);
    }
  }

  private static void assertOutput(
      byte[] bytes, int primitive, Layout layout, int srid, boolean empty) throws ParseException {
    assertHeader(bytes, primitive | layout.flags | (srid == 0 ? 0 : 0x20000000), srid);
    assertEquals(
        5 + (srid == 0 ? 0 : 4) + (primitive == 1 ? layout.dimension * 8 : 4), bytes.length);
    Geometry output = Constructors.geomFromWKB(bytes);
    assertEquals(srid, output.getSRID());
    assertEquals(empty, output.isEmpty());
    CoordinateSequence sequence = coordinateSequence(output);
    assertSequence(sequence, layout);
    if (!empty) {
      assertEquals(1, sequence.getX(0), 0);
      assertEquals(2, sequence.getY(0), 0);
      for (int ordinate = 2; ordinate < layout.dimension; ordinate++) {
        assertTrue(Double.isNaN(sequence.getOrdinate(0, ordinate)));
      }
    }
  }

  private static void assertHeader(byte[] bytes, int type, int srid) {
    ByteBuffer buffer =
        ByteBuffer.wrap(bytes)
            .order(bytes[0] == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    assertEquals(type, buffer.getInt(1));
    if (srid != 0) assertEquals(srid, buffer.getInt(5));
  }

  private static void assertSequence(CoordinateSequence sequence, Layout layout) {
    assertEquals(layout.dimension, sequence.getDimension());
    assertEquals(layout.measures, sequence.getMeasures());
  }

  private static CoordinateSequence coordinateSequence(Geometry geometry) {
    if (geometry instanceof Point) return ((Point) geometry).getCoordinateSequence();
    if (geometry instanceof LineString) return ((LineString) geometry).getCoordinateSequence();
    return ((Polygon) geometry).getExteriorRing().getCoordinateSequence();
  }

  private static byte[][] outputVariants(Geometry geometry) {
    return new byte[][] {
      GeomUtils.getWKB(geometry),
      GeomUtils.getEWKB(geometry),
      WKBReader.hexToBytes(GeomUtils.getHexEWKB(geometry, ByteOrderValues.LITTLE_ENDIAN)),
      WKBReader.hexToBytes(GeomUtils.getHexEWKB(geometry, ByteOrderValues.BIG_ENDIAN))
    };
  }

  private static Geometry readPrimitive(int primitive, Layout layout, boolean empty)
      throws ParseException {
    ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put((byte) 1).putInt(primitive | layout.flags | 0x20000000).putInt(4326);
    if (primitive == 1) {
      buffer.putDouble(empty ? Double.NaN : 1).putDouble(empty ? Double.NaN : 2);
      for (int ordinate = 2; ordinate < layout.dimension; ordinate++) buffer.putDouble(Double.NaN);
    } else {
      buffer.putInt(0);
    }
    return Constructors.geomFromWKB(Arrays.copyOf(buffer.array(), buffer.position()));
  }

  private static Geometry roundTrip(Geometry geometry) {
    return GeometrySerializer.deserialize(GeometrySerializer.serialize(geometry));
  }

  private static final class Layout {
    final int dimension;
    final int measures;
    final int flags;

    Layout(int dimension, int measures, int flags) {
      this.dimension = dimension;
      this.measures = measures;
      this.flags = flags;
    }
  }
}
