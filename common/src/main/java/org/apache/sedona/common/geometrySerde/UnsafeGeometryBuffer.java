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

import java.lang.reflect.Field;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import sun.misc.Unsafe;

class UnsafeGeometryBuffer implements GeometryBuffer {
  private static final Unsafe UNSAFE;
  private static final long BYTE_ARRAY_BASE_OFFSET;

  static {
    Unsafe unsafe;
    long byteArrayOffset = 0;
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      unsafe = (Unsafe) theUnsafe.get(null);
      byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);
    } catch (IllegalAccessException | NoSuchFieldException | SecurityException e) {
      // Unsafe is not available
      unsafe = null;
    }
    UNSAFE = unsafe;
    BYTE_ARRAY_BASE_OFFSET = byteArrayOffset;
  }

  public static boolean isUnsafeAvailable() {
    return UNSAFE != null;
  }

  private CoordinateType coordinateType = CoordinateType.XY;
  private final byte[] bytes;
  private final long baseOffset;
  private int markOffset = 0;

  public UnsafeGeometryBuffer(int bufferSize) {
    bytes = new byte[bufferSize];
    baseOffset = BYTE_ARRAY_BASE_OFFSET;
  }

  public UnsafeGeometryBuffer(byte[] bytes, int offset) {
    this.bytes = bytes;
    baseOffset = offset + BYTE_ARRAY_BASE_OFFSET;
  }

  public UnsafeGeometryBuffer(byte[] bytes) {
    this.bytes = bytes;
    baseOffset = BYTE_ARRAY_BASE_OFFSET;
  }

  @Override
  public CoordinateType getCoordinateType() {
    return coordinateType;
  }

  @Override
  public void setCoordinateType(CoordinateType coordinateType) {
    this.coordinateType = coordinateType;
  }

  @Override
  public int getLength() {
    return (int) (bytes.length - baseOffset + BYTE_ARRAY_BASE_OFFSET);
  }

  @Override
  public void mark(int offset) {
    markOffset = offset;
  }

  @Override
  public int getMark() {
    return markOffset;
  }

  @Override
  public void putByte(int offset, byte value) {
    UNSAFE.putByte(bytes, baseOffset + offset, value);
  }

  @Override
  public byte getByte(int offset) {
    assert baseOffset + offset < bytes.length + BYTE_ARRAY_BASE_OFFSET;
    return UNSAFE.getByte(bytes, baseOffset + offset);
  }

  @Override
  public void putBytes(int offset, byte[] inBytes) {
    assert baseOffset + offset + inBytes.length <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    UNSAFE.copyMemory(inBytes, BYTE_ARRAY_BASE_OFFSET, bytes, baseOffset + offset, inBytes.length);
  }

  @Override
  public void getBytes(byte[] outBytes, int offset, int length) {
    assert baseOffset + offset + length <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    UNSAFE.copyMemory(bytes, baseOffset + offset, outBytes, BYTE_ARRAY_BASE_OFFSET, length);
  }

  @Override
  public void putInt(int offset, int value) {
    assert baseOffset + offset + 4 <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    UNSAFE.putInt(bytes, baseOffset + offset, value);
  }

  @Override
  public int getInt(int offset) {
    assert baseOffset + offset + 4 <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    return UNSAFE.getInt(bytes, baseOffset + offset);
  }

  @Override
  public void putCoordinate(int offset, Coordinate coordinate) {
    long coordOffset = baseOffset + offset;
    assert coordOffset + coordinateType.bytes <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    switch (coordinateType) {
      case XY:
        UNSAFE.putDouble(bytes, coordOffset, coordinate.x);
        UNSAFE.putDouble(bytes, coordOffset + 8, coordinate.y);
        break;
      case XYZ:
        UNSAFE.putDouble(bytes, coordOffset, coordinate.x);
        UNSAFE.putDouble(bytes, coordOffset + 8, coordinate.y);
        UNSAFE.putDouble(bytes, coordOffset + 16, coordinate.getZ());
        break;
      case XYM:
        UNSAFE.putDouble(bytes, coordOffset, coordinate.x);
        UNSAFE.putDouble(bytes, coordOffset + 8, coordinate.y);
        UNSAFE.putDouble(bytes, coordOffset + 16, coordinate.getM());
        break;
      case XYZM:
        UNSAFE.putDouble(bytes, coordOffset, coordinate.x);
        UNSAFE.putDouble(bytes, coordOffset + 8, coordinate.y);
        UNSAFE.putDouble(bytes, coordOffset + 16, coordinate.getZ());
        UNSAFE.putDouble(bytes, coordOffset + 24, coordinate.getM());
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public CoordinateSequence getCoordinate(int offset) {
    long coordOffset = baseOffset + offset;
    assert coordOffset + coordinateType.bytes <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    double x = UNSAFE.getDouble(bytes, coordOffset);
    double y = UNSAFE.getDouble(bytes, coordOffset + 8);
    double z;
    double m;
    Coordinate[] coordinates = new Coordinate[1];
    switch (coordinateType) {
      case XY:
        coordinates[0] = new CoordinateXY(x, y);
        return new CoordinateArraySequence(coordinates, 2, 0);
      case XYZ:
        z = UNSAFE.getDouble(bytes, coordOffset + 16);
        coordinates[0] = new Coordinate(x, y, z);
        return new CoordinateArraySequence(coordinates, 3, 0);
      case XYM:
        m = UNSAFE.getDouble(bytes, coordOffset + 16);
        coordinates[0] = new CoordinateXYM(x, y, m);
        return new CoordinateArraySequence(coordinates, 3, 1);
      case XYZM:
        z = UNSAFE.getDouble(bytes, coordOffset + 16);
        m = UNSAFE.getDouble(bytes, coordOffset + 24);
        coordinates[0] = new CoordinateXYZM(x, y, z, m);
        return new CoordinateArraySequence(coordinates, 4, 1);
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public void putCoordinates(int offset, CoordinateSequence coordinates) {
    long coordOffset = baseOffset + offset;
    int numCoordinates = coordinates.size();
    assert coordOffset + (long) coordinateType.bytes * numCoordinates
        <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    switch (coordinateType) {
      case XY:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(bytes, coordOffset, coord.x);
          UNSAFE.putDouble(bytes, coordOffset + 8, coord.y);
          coordOffset += 16;
        }
        break;
      case XYZ:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(bytes, coordOffset, coord.x);
          UNSAFE.putDouble(bytes, coordOffset + 8, coord.y);
          UNSAFE.putDouble(bytes, coordOffset + 16, coord.getZ());
          coordOffset += 24;
        }
        break;
      case XYM:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(bytes, coordOffset, coord.x);
          UNSAFE.putDouble(bytes, coordOffset + 8, coord.y);
          UNSAFE.putDouble(bytes, coordOffset + 16, coord.getM());
          coordOffset += 24;
        }
        break;
      case XYZM:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          UNSAFE.putDouble(bytes, coordOffset, coord.x);
          UNSAFE.putDouble(bytes, coordOffset + 8, coord.y);
          UNSAFE.putDouble(bytes, coordOffset + 16, coord.getZ());
          UNSAFE.putDouble(bytes, coordOffset + 24, coord.getM());
          coordOffset += 32;
        }
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public CoordinateSequence getCoordinates(int offset, int numCoordinates) {
    long coordOffset = baseOffset + offset;
    assert coordOffset + (long) coordinateType.bytes * numCoordinates
        <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    Coordinate[] coordinates = new Coordinate[numCoordinates];
    int dimension = 2;
    int measures = 0;
    switch (coordinateType) {
      case XY:
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(bytes, coordOffset);
          double y = UNSAFE.getDouble(bytes, coordOffset + 8);
          coordinates[k] = new CoordinateXY(x, y);
          coordOffset += 16;
        }
        break;
      case XYZ:
        dimension = 3;
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(bytes, coordOffset);
          double y = UNSAFE.getDouble(bytes, coordOffset + 8);
          double z = UNSAFE.getDouble(bytes, coordOffset + 16);
          coordinates[k] = new Coordinate(x, y, z);
          coordOffset += 24;
        }
        break;
      case XYM:
        dimension = 3;
        measures = 1;
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(bytes, coordOffset);
          double y = UNSAFE.getDouble(bytes, coordOffset + 8);
          double m = UNSAFE.getDouble(bytes, coordOffset + 16);
          coordinates[k] = new CoordinateXYM(x, y, m);
          coordOffset += 24;
        }
        break;
      case XYZM:
        dimension = 4;
        measures = 1;
        for (int k = 0; k < numCoordinates; k++) {
          double x = UNSAFE.getDouble(bytes, coordOffset);
          double y = UNSAFE.getDouble(bytes, coordOffset + 8);
          double z = UNSAFE.getDouble(bytes, coordOffset + 16);
          double m = UNSAFE.getDouble(bytes, coordOffset + 24);
          coordinates[k] = new CoordinateXYZM(x, y, z, m);
          coordOffset += 32;
        }
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
    return new CoordinateArraySequence(coordinates, dimension, measures);
  }

  @Override
  public GeometryBuffer slice(int offset) {
    assert baseOffset + offset <= bytes.length + BYTE_ARRAY_BASE_OFFSET;
    int bytesOffset = (int) (baseOffset + offset - BYTE_ARRAY_BASE_OFFSET);
    return new UnsafeGeometryBuffer(bytes, bytesOffset);
  }

  @Override
  public byte[] toByteArray() {
    if (baseOffset == BYTE_ARRAY_BASE_OFFSET) {
      return bytes;
    } else {
      int length = (int) (bytes.length - baseOffset + BYTE_ARRAY_BASE_OFFSET);
      byte[] copy = new byte[(int) length];
      UNSAFE.copyMemory(this.bytes, baseOffset, copy, BYTE_ARRAY_BASE_OFFSET, length);
      return copy;
    }
  }
}
