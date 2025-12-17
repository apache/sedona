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
import org.locationtech.jts.geom.GeometryFactory;

public class GeometryTypeSerializer extends TypeSerializer<org.locationtech.jts.geom.Geometry> {

  private static final long serialVersionUID = 1L;

  public static final GeometryTypeSerializer INSTANCE = new GeometryTypeSerializer();

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public GeometryTypeSerializer() {}

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Geometry> duplicate() {
    return this;
  }

  @Override
  public Geometry createInstance() {
    return GEOMETRY_FACTORY.createPoint();
  }

  @Override
  public Geometry copy(Geometry from) {
    return (Geometry) from.copy();
  }

  @Override
  public Geometry copy(Geometry from, Geometry reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Geometry record, DataOutputView target) throws IOException {
    if (record == null) {
      target.writeInt(-1);
    } else {
      byte[] data = org.apache.sedona.common.geometrySerde.GeometrySerializer.serialize(record);
      target.writeInt(data.length);
      target.write(data);
    }
  }

  @Override
  public Geometry deserialize(DataInputView source) throws IOException {
    int length = source.readInt();
    if (length == -1) {
      return null;
    }
    byte[] data = new byte[length];
    source.readFully(data);
    return org.apache.sedona.common.geometrySerde.GeometrySerializer.deserialize(data);
  }

  @Override
  public Geometry deserialize(Geometry reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int length = source.readInt();
    target.writeInt(length);
    if (length > 0) {
      target.write(source, length);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof GeometryTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Geometry> snapshotConfiguration() {
    return new GeometrySerializerSnapshot();
  }

  public static final class GeometrySerializerSnapshot implements TypeSerializerSnapshot<Geometry> {
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
    public TypeSerializer<Geometry> restoreSerializer() {
      return GeometryTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Geometry> resolveSchemaCompatibility(
        TypeSerializer<Geometry> newSerializer) {
      if (newSerializer instanceof GeometryTypeSerializer) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      } else {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
    }
  }
}
