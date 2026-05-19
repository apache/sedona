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
import org.apache.sedona.common.geometryObjects.Box2D;

/**
 * Flink {@link TypeSerializer} for {@link Box2D}. Serializes as a presence byte followed by four
 * doubles (xmin, ymin, xmax, ymax). Total payload for a non-null value is 33 bytes.
 */
public class Box2DTypeSerializer extends TypeSerializer<Box2D> {

  private static final long serialVersionUID = 1L;

  public static final Box2DTypeSerializer INSTANCE = new Box2DTypeSerializer();

  public Box2DTypeSerializer() {}

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TypeSerializer<Box2D> duplicate() {
    return this;
  }

  @Override
  public Box2D createInstance() {
    // Box2D has no in-band "empty" sentinel — absence is represented by SQL NULL.
    // Returning null here is the honest default; (0, 0, 0, 0) would alias to a real point at
    // the equator/prime-meridian and could silently corrupt any caller that treats the
    // createInstance() result as a usable bbox.
    return null;
  }

  @Override
  public Box2D copy(Box2D from) {
    if (from == null) {
      return null;
    }
    return new Box2D(from.getXMin(), from.getYMin(), from.getXMax(), from.getYMax());
  }

  @Override
  public Box2D copy(Box2D from, Box2D reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Box2D record, DataOutputView target) throws IOException {
    if (record == null) {
      target.writeByte(0);
    } else {
      target.writeByte(1);
      target.writeDouble(record.getXMin());
      target.writeDouble(record.getYMin());
      target.writeDouble(record.getXMax());
      target.writeDouble(record.getYMax());
    }
  }

  @Override
  public Box2D deserialize(DataInputView source) throws IOException {
    byte present = source.readByte();
    if (present == 0) {
      return null;
    }
    double xmin = source.readDouble();
    double ymin = source.readDouble();
    double xmax = source.readDouble();
    double ymax = source.readDouble();
    return new Box2D(xmin, ymin, xmax, ymax);
  }

  @Override
  public Box2D deserialize(Box2D reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    byte present = source.readByte();
    target.writeByte(present);
    if (present != 0) {
      target.write(source, 32);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Box2DTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Box2D> snapshotConfiguration() {
    return new Box2DSerializerSnapshot();
  }

  public static final class Box2DSerializerSnapshot implements TypeSerializerSnapshot<Box2D> {
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
    public TypeSerializer<Box2D> restoreSerializer() {
      return Box2DTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Box2D> resolveSchemaCompatibility(
        TypeSerializer<Box2D> newSerializer) {
      if (newSerializer instanceof Box2DTypeSerializer) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      } else {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
    }
  }
}
