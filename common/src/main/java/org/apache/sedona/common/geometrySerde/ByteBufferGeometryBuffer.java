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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

class ByteBufferGeometryBuffer implements GeometryBuffer {
  private CoordinateType coordinateType = CoordinateType.XY;
  private final ByteBuffer byteBuffer;
  private int markOffset = 0;

  public ByteBufferGeometryBuffer(int bufferSize) {
    byteBuffer = ByteBuffer.allocate(bufferSize);
    byteBuffer.order(ByteOrder.nativeOrder());
  }

  public ByteBufferGeometryBuffer(byte[] bytes) {
    byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
  }

  public ByteBufferGeometryBuffer(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer.order(ByteOrder.nativeOrder());
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
    return byteBuffer.capacity();
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
    byteBuffer.put(offset, value);
  }

  @Override
  public byte getByte(int offset) {
    return byteBuffer.get(offset);
  }

  @Override
  public void putBytes(int offset, byte[] bytes) {
    byteBuffer.position(offset);
    byteBuffer.put(bytes, 0, bytes.length);
  }

  @Override
  public void getBytes(byte[] bytes, int offset, int length) {
    byteBuffer.position(offset);
    byteBuffer.get(bytes, 0, length);
  }

  @Override
  public void putInt(int offset, int value) {
    byteBuffer.putInt(offset, value);
  }

  @Override
  public int getInt(int offset) {
    return byteBuffer.getInt(offset);
  }

  @Override
  public void putCoordinate(int offset, Coordinate coordinate) {
    byteBuffer.putDouble(offset, coordinate.x);
    byteBuffer.putDouble(offset + 8, coordinate.y);
    offset += 16;
    if (coordinateType.hasZ) {
      byteBuffer.putDouble(offset, coordinate.getZ());
      offset += 8;
    }
    if (coordinateType.hasM) {
      byteBuffer.putDouble(offset, coordinate.getM());
    }
  }

  @Override
  public CoordinateSequence getCoordinate(int offset) {
    double x = byteBuffer.getDouble(offset);
    double y = byteBuffer.getDouble(offset + 8);
    double z;
    double m;
    Coordinate[] coordinates = new Coordinate[1];
    switch (coordinateType) {
      case XY:
        coordinates[0] = new CoordinateXY(x, y);
        return new CoordinateArraySequence(coordinates, 2, 0);
      case XYZ:
        z = byteBuffer.getDouble(offset + 16);
        coordinates[0] = new Coordinate(x, y, z);
        return new CoordinateArraySequence(coordinates, 3, 0);
      case XYM:
        m = byteBuffer.getDouble(offset + 16);
        coordinates[0] = new CoordinateXYM(x, y, m);
        return new CoordinateArraySequence(coordinates, 3, 1);
      case XYZM:
        z = byteBuffer.getDouble(offset + 16);
        m = byteBuffer.getDouble(offset + 24);
        coordinates[0] = new CoordinateXYZM(x, y, z, m);
        return new CoordinateArraySequence(coordinates, 4, 1);
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public void putCoordinates(int offset, CoordinateSequence coordinates) {
    int numCoordinates = coordinates.size();
    switch (coordinateType) {
      case XY:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          byteBuffer.putDouble(offset, coord.x);
          byteBuffer.putDouble(offset + 8, coord.y);
          offset += 16;
        }
        break;
      case XYZ:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          byteBuffer.putDouble(offset, coord.x);
          byteBuffer.putDouble(offset + 8, coord.y);
          byteBuffer.putDouble(offset + 16, coord.getZ());
          offset += 24;
        }
        break;
      case XYM:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          byteBuffer.putDouble(offset, coord.x);
          byteBuffer.putDouble(offset + 8, coord.y);
          byteBuffer.putDouble(offset + 16, coord.getM());
          offset += 24;
        }
        break;
      case XYZM:
        for (int k = 0; k < numCoordinates; k++) {
          Coordinate coord = coordinates.getCoordinate(k);
          byteBuffer.putDouble(offset, coord.x);
          byteBuffer.putDouble(offset + 8, coord.y);
          byteBuffer.putDouble(offset + 16, coord.getZ());
          byteBuffer.putDouble(offset + 24, coord.getM());
          offset += 32;
        }
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
  }

  @Override
  public CoordinateSequence getCoordinates(int offset, int numCoordinates) {
    Coordinate[] coordinates = new Coordinate[numCoordinates];
    int dimension = 2;
    int measures = 0;
    switch (coordinateType) {
      case XY:
        for (int k = 0; k < numCoordinates; k++) {
          double x = byteBuffer.getDouble(offset);
          double y = byteBuffer.getDouble(offset + 8);
          coordinates[k] = new CoordinateXY(x, y);
          offset += 16;
        }
        break;
      case XYZ:
        dimension = 3;
        for (int k = 0; k < numCoordinates; k++) {
          double x = byteBuffer.getDouble(offset);
          double y = byteBuffer.getDouble(offset + 8);
          double z = byteBuffer.getDouble(offset + 16);
          coordinates[k] = new Coordinate(x, y, z);
          offset += 24;
        }
        break;
      case XYM:
        dimension = 3;
        measures = 1;
        for (int k = 0; k < numCoordinates; k++) {
          double x = byteBuffer.getDouble(offset);
          double y = byteBuffer.getDouble(offset + 8);
          double m = byteBuffer.getDouble(offset + 16);
          coordinates[k] = new CoordinateXYM(x, y, m);
          offset += 24;
        }
        break;
      case XYZM:
        dimension = 4;
        measures = 1;
        for (int k = 0; k < numCoordinates; k++) {
          double x = byteBuffer.getDouble(offset);
          double y = byteBuffer.getDouble(offset + 8);
          double z = byteBuffer.getDouble(offset + 16);
          double m = byteBuffer.getDouble(offset + 24);
          coordinates[k] = new CoordinateXYZM(x, y, z, m);
          offset += 32;
        }
        break;
      default:
        throw new IllegalStateException("coordinateType was not configured properly");
    }
    return new CoordinateArraySequence(coordinates, dimension, measures);
  }

  @Override
  public GeometryBuffer slice(int offset) {
    byteBuffer.position(offset);
    return new ByteBufferGeometryBuffer(byteBuffer.slice());
  }

  @Override
  public byte[] toByteArray() {
    if (byteBuffer.arrayOffset() == 0) {
      return byteBuffer.array();
    } else {
      byte[] bytes = new byte[byteBuffer.capacity()];
      byteBuffer.get(bytes);
      return bytes;
    }
  }
}
