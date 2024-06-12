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
package org.apache.sedona.common.geometrySerde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

@RunWith(Parameterized.class)
public class GeometryBufferTest {

  private final String bufferType;

  public GeometryBufferTest(String bufferType) {
    this.bufferType = bufferType;
  }

  @Parameterized.Parameters
  public static Collection<String> testParams() {
    return Arrays.asList("bytebuffer", "unsafe");
  }

  @Test
  public void testPutGetByte() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 1);
    buffer.putByte(0, (byte) 1);
    assertEquals((byte) 1, buffer.getByte(0));
  }

  @Test
  public void testPutGetBytes() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 4);
    buffer.putBytes(2, new byte[] {3, 4});
    buffer.putBytes(0, new byte[] {1, 2});
    byte[] bytes = new byte[4];
    buffer.getBytes(bytes, 2, 2);
    assertEquals((byte) 3, bytes[0]);
    assertEquals((byte) 4, bytes[1]);
    buffer.getBytes(bytes, 0, 4);
    assertEquals((byte) 1, bytes[0]);
    assertEquals((byte) 2, bytes[1]);
    assertEquals((byte) 3, bytes[2]);
    assertEquals((byte) 4, bytes[3]);
  }

  @Test
  public void testPutGetInt() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 4);
    buffer.putInt(0, 1);
    assertEquals(1, buffer.getInt(0));
  }

  @Test
  public void testPutGetCoordinate() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 16);
    buffer.putCoordinate(0, new Coordinate(1, 2));
    CoordinateSequence coordinates = buffer.getCoordinate(0);
    assertEquals(1, coordinates.size());
    Coordinate coordinate = coordinates.getCoordinate(0);
    assertEquals(1, coordinate.x, 1e-6);
    assertEquals(2, coordinate.y, 1e-6);
  }

  @Test
  public void testPutGetCoordinateXYZ() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 24);
    buffer.setCoordinateType(CoordinateType.XYZ);
    buffer.putCoordinate(0, new Coordinate(1, 2, 3));
    CoordinateSequence coordinates = buffer.getCoordinate(0);
    assertEquals(1, coordinates.size());
    Coordinate coordinate = coordinates.getCoordinate(0);
    assertEquals(1, coordinate.x, 1e-6);
    assertEquals(2, coordinate.y, 1e-6);
    assertEquals(3, coordinate.getZ(), 1e-6);
  }

  @Test
  public void testPutGetCoordinateXYM() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 24);
    buffer.setCoordinateType(CoordinateType.XYM);
    buffer.putCoordinate(0, new CoordinateXYM(1, 2, 3));
    CoordinateSequence coordinates = buffer.getCoordinate(0);
    assertEquals(1, coordinates.size());
    Coordinate coordinate = coordinates.getCoordinate(0);
    assertEquals(1, coordinate.x, 1e-6);
    assertEquals(2, coordinate.y, 1e-6);
    assertEquals(3, coordinate.getM(), 1e-6);
  }

  @Test
  public void testPutGetCoordinateXYZM() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 32);
    buffer.setCoordinateType(CoordinateType.XYZM);
    buffer.putCoordinate(0, new CoordinateXYZM(1, 2, 3, 4));
    CoordinateSequence coordinates = buffer.getCoordinate(0);
    assertEquals(1, coordinates.size());
    Coordinate coordinate = coordinates.getCoordinate(0);
    assertEquals(1, coordinate.x, 1e-6);
    assertEquals(2, coordinate.y, 1e-6);
    assertEquals(3, coordinate.getZ(), 1e-6);
    assertEquals(4, coordinate.getM(), 1e-6);
  }

  @Test
  public void testPutGetCoordinateWithUnmatchedCoordType() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 24);
    buffer.setCoordinateType(CoordinateType.XYZ);
    buffer.putCoordinate(0, new CoordinateXYM(1, 2, 3));
    CoordinateSequence coordinates = buffer.getCoordinate(0);
    assertEquals(1, coordinates.size());
    Coordinate coordinate = coordinates.getCoordinate(0);
    assertEquals(1, coordinate.x, 1e-6);
    assertEquals(2, coordinate.y, 1e-6);
    assertTrue(Double.isNaN(coordinate.getZ()));
  }

  @Test
  public void testPutGetCoordinates() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 32);
    CoordinateSequence coordinates =
        new CoordinateArraySequence(new Coordinate[] {new Coordinate(1, 2), new Coordinate(3, 4)});
    buffer.putCoordinates(0, coordinates);
    CoordinateSequence coordinates2 = buffer.getCoordinates(0, 2);
    assertEquals(1, coordinates2.getCoordinate(0).x, 1e-6);
    assertEquals(2, coordinates2.getCoordinate(0).y, 1e-6);
    assertEquals(3, coordinates2.getCoordinate(1).x, 1e-6);
    assertEquals(4, coordinates2.getCoordinate(1).y, 1e-6);
  }

  @Test
  public void testPutGetCoordinatesXYZ() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 48);
    buffer.setCoordinateType(CoordinateType.XYZ);
    CoordinateSequence coordinates =
        new CoordinateArraySequence(
            new Coordinate[] {new Coordinate(1, 2, 3), new Coordinate(4, 5, 6)});
    buffer.putCoordinates(0, coordinates);
    CoordinateSequence coordinates2 = buffer.getCoordinates(0, 2);
    assertEquals(1, coordinates2.getCoordinate(0).x, 1e-6);
    assertEquals(2, coordinates2.getCoordinate(0).y, 1e-6);
    assertEquals(3, coordinates2.getCoordinate(0).getZ(), 1e-6);
    assertEquals(4, coordinates2.getCoordinate(1).x, 1e-6);
    assertEquals(5, coordinates2.getCoordinate(1).y, 1e-6);
    assertEquals(6, coordinates2.getCoordinate(1).getZ(), 1e-6);
  }

  @Test
  public void testPutGetCoordinatesXYM() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 48);
    buffer.setCoordinateType(CoordinateType.XYM);
    CoordinateSequence coordinates =
        new CoordinateArraySequence(
            new Coordinate[] {new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6)});
    buffer.putCoordinates(0, coordinates);
    CoordinateSequence coordinates2 = buffer.getCoordinates(0, 2);
    assertEquals(1, coordinates2.getCoordinate(0).x, 1e-6);
    assertEquals(2, coordinates2.getCoordinate(0).y, 1e-6);
    assertEquals(3, coordinates2.getCoordinate(0).getM(), 1e-6);
    assertEquals(4, coordinates2.getCoordinate(1).x, 1e-6);
    assertEquals(5, coordinates2.getCoordinate(1).y, 1e-6);
    assertEquals(6, coordinates2.getCoordinate(1).getM(), 1e-6);
  }

  @Test
  public void testPutGetCoordinatesXYZM() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 64);
    buffer.setCoordinateType(CoordinateType.XYZM);
    CoordinateSequence coordinates =
        new CoordinateArraySequence(
            new Coordinate[] {new CoordinateXYZM(1, 2, 3, 4), new CoordinateXYZM(5, 6, 7, 8)});
    buffer.putCoordinates(0, coordinates);
    CoordinateSequence coordinates2 = buffer.getCoordinates(0, 2);
    assertEquals(1, coordinates2.getCoordinate(0).x, 1e-6);
    assertEquals(2, coordinates2.getCoordinate(0).y, 1e-6);
    assertEquals(3, coordinates2.getCoordinate(0).getZ(), 1e-6);
    assertEquals(4, coordinates2.getCoordinate(0).getM(), 1e-6);
    assertEquals(5, coordinates2.getCoordinate(1).x, 1e-6);
    assertEquals(6, coordinates2.getCoordinate(1).y, 1e-6);
    assertEquals(7, coordinates2.getCoordinate(1).getZ(), 1e-6);
    assertEquals(8, coordinates2.getCoordinate(1).getM(), 1e-6);
  }

  @Test
  public void testPutGetCoordinatesWithUnmatchedCoordType() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 48);
    buffer.setCoordinateType(CoordinateType.XYZ);
    CoordinateSequence coordinates =
        new CoordinateArraySequence(
            new Coordinate[] {new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6)});
    buffer.putCoordinates(0, coordinates);
    CoordinateSequence coordinates2 = buffer.getCoordinates(0, 2);
    assertEquals(1, coordinates2.getCoordinate(0).x, 1e-6);
    assertEquals(2, coordinates2.getCoordinate(0).y, 1e-6);
    assertTrue(Double.isNaN(coordinates2.getCoordinate(0).getZ()));
    assertEquals(4, coordinates2.getCoordinate(1).x, 1e-6);
    assertEquals(5, coordinates2.getCoordinate(1).y, 1e-6);
    assertTrue(Double.isNaN(coordinates2.getCoordinate(1).getZ()));
  }

  @Test
  public void testSlice() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 16);
    buffer.putBytes(0, new byte[] {1, 2, 3, 4});
    GeometryBuffer slice = buffer.slice(0);
    assertEquals((byte) 1, slice.getByte(0));
    assertEquals((byte) 2, slice.getByte(1));
    assertEquals((byte) 3, slice.getByte(2));
    assertEquals((byte) 4, slice.getByte(3));
    slice = slice.slice(1);
    assertEquals((byte) 2, slice.getByte(0));
    assertEquals((byte) 4, slice.getByte(2));
    slice = slice.slice(1);
    assertEquals((byte) 3, slice.getByte(0));
    assertEquals((byte) 4, slice.getByte(1));
    assertEquals(14, slice.getLength());
  }

  @Test
  public void testToByteArray() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 2);
    buffer.putByte(0, (byte) 1);
    buffer.putByte(1, (byte) 2);
    byte[] bytes = buffer.toByteArray();
    assertEquals(2, bytes.length);
    assertEquals((byte) 1, bytes[0]);
    assertEquals((byte) 2, bytes[1]);
  }

  @Test
  public void testMixedDataTypes() {
    GeometryBuffer buffer = GeometryBufferFactory.create(bufferType, 28);
    buffer.putByte(0, (byte) 1);
    buffer.putInt(4, 100);
    buffer.putCoordinate(8, new Coordinate(10, 20));
    buffer.putInt(24, 200);
    assertEquals((byte) 1, buffer.getByte(0));
    assertEquals(100, buffer.getInt(4));
    assertEquals(10, buffer.getCoordinate(8).getCoordinate(0).x, 1e-6);
    assertEquals(20, buffer.getCoordinate(8).getCoordinate(0).y, 1e-6);
    assertEquals(200, buffer.getInt(24));
  }

  @Test
  public void testWrap() {
    byte[] bytes = new byte[] {1, 2, 3, 4};
    GeometryBuffer buffer = GeometryBufferFactory.wrap(bufferType, bytes);
    assertEquals(4, buffer.getLength());
    assertEquals((byte) 1, buffer.getByte(0));
    assertEquals((byte) 2, buffer.getByte(1));
    assertEquals((byte) 3, buffer.getByte(2));
    assertEquals((byte) 4, buffer.getByte(3));
  }
}
