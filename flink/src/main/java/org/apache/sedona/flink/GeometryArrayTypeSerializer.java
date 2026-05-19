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
import org.locationtech.jts.geom.Geometry;

public class GeometryArrayTypeSerializer extends TypeSerializer<Geometry[]> {
  private static final long serialVersionUID = 1L;

  public static final GeometryArrayTypeSerializer INSTANCE = new GeometryArrayTypeSerializer();

  public GeometryArrayTypeSerializer() {}

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Geometry[]> duplicate() {
    return this;
  }

  @Override
  public Geometry[] createInstance() {
    return new Geometry[0];
  }

  @Override
  public Geometry[] copy(Geometry[] from) {
    if (from == null) {
      return null;
    }
    Geometry[] newArray = new Geometry[from.length];
    for (int i = 0; i < from.length; i++) {
      if (from[i] != null) {
        newArray[i] = GeometryTypeSerializer.INSTANCE.copy(from[i]);
      } else {
        newArray[i] = null;
      }
    }
    return newArray;
  }

  @Override
  public Geometry[] copy(Geometry[] from, Geometry[] reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Geometry[] record, DataOutputView target) throws IOException {
    if (record == null) {
      target.writeInt(-1);
    } else {
      target.writeInt(record.length);
      for (Geometry geom : record) {
        GeometryTypeSerializer.INSTANCE.serialize(geom, target);
      }
    }
  }

  @Override
  public Geometry[] deserialize(DataInputView source) throws IOException {
    int length = source.readInt();
    if (length == -1) {
      return null;
    }

    Geometry[] array = new Geometry[length];
    for (int i = 0; i < length; i++) {
      array[i] = GeometryTypeSerializer.INSTANCE.deserialize(source);
    }
    return array;
  }

  @Override
  public Geometry[] deserialize(Geometry[] reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int length = source.readInt();
    target.writeInt(length);

    if (length > 0) {
      for (int i = 0; i < length; i++) {
        GeometryTypeSerializer.INSTANCE.copy(source, target);
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof GeometryArrayTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Geometry[]> snapshotConfiguration() {
    return new GeometryArraySerializerSnapshot();
  }

  public static final class GeometryArraySerializerSnapshot
      implements TypeSerializerSnapshot<Geometry[]> {
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
    public TypeSerializer<Geometry[]> restoreSerializer() {
      return GeometryArrayTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Geometry[]> resolveSchemaCompatibility(
        TypeSerializer<Geometry[]> newSerializer) {
      if (newSerializer instanceof GeometryArrayTypeSerializer) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      } else {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
    }
  }
}
