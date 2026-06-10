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
package org.apache.sedona.flink;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.sedona.common.geometryObjects.Box3D;

/**
 * Flink {@link TypeSerializer} for {@link Box3D}. Serializes as a presence byte followed by six
 * doubles (xmin, ymin, zmin, xmax, ymax, zmax). Total payload for a non-null value is 49 bytes.
 */
public class Box3DTypeSerializer extends TypeSerializer<Box3D> {

  private static final long serialVersionUID = 1L;

  public static final Box3DTypeSerializer INSTANCE = new Box3DTypeSerializer();

  public Box3DTypeSerializer() {}

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TypeSerializer<Box3D> duplicate() {
    return this;
  }

  @Override
  public Box3D createInstance() {
    // Box3D has no in-band "empty" sentinel — absence is represented by SQL NULL.
    // Returning null here is the honest default; (0, 0, 0, 0, 0, 0) would alias to a real box at
    // the origin and could silently corrupt any caller that treats the createInstance() result as
    // a usable bbox.
    return null;
  }

  @Override
  public Box3D copy(Box3D from) {
    if (from == null) {
      return null;
    }
    return new Box3D(
        from.getXMin(),
        from.getYMin(),
        from.getZMin(),
        from.getXMax(),
        from.getYMax(),
        from.getZMax());
  }

  @Override
  public Box3D copy(Box3D from, Box3D reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Box3D record, DataOutputView target) throws IOException {
    if (record == null) {
      target.writeByte(0);
    } else {
      target.writeByte(1);
      target.writeDouble(record.getXMin());
      target.writeDouble(record.getYMin());
      target.writeDouble(record.getZMin());
      target.writeDouble(record.getXMax());
      target.writeDouble(record.getYMax());
      target.writeDouble(record.getZMax());
    }
  }

  @Override
  public Box3D deserialize(DataInputView source) throws IOException {
    byte present = source.readByte();
    if (present == 0) {
      return null;
    }
    double xmin = source.readDouble();
    double ymin = source.readDouble();
    double zmin = source.readDouble();
    double xmax = source.readDouble();
    double ymax = source.readDouble();
    double zmax = source.readDouble();
    return new Box3D(xmin, ymin, zmin, xmax, ymax, zmax);
  }

  @Override
  public Box3D deserialize(Box3D reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    byte present = source.readByte();
    target.writeByte(present);
    if (present != 0) {
      target.write(source, 48);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Box3DTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Box3D> snapshotConfiguration() {
    return new Box3DSerializerSnapshot();
  }

  public static final class Box3DSerializerSnapshot implements TypeSerializerSnapshot<Box3D> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getCurrentVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {}

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
        throws IOException {
      if (readVersion != CURRENT_VERSION) {
        throw new IOException(
            "Cannot read snapshot: Incompatible version "
                + readVersion
                + ". Expected version "
                + CURRENT_VERSION);
      }
    }

    @Override
    public TypeSerializer<Box3D> restoreSerializer() {
      return Box3DTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Box3D> resolveSchemaCompatibility(
        TypeSerializer<Box3D> newSerializer) {
      if (newSerializer instanceof Box3DTypeSerializer) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      } else {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
    }
  }
}
