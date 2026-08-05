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
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.sedona.flink.expressions.Accumulators.AccGeometryCollection;

public class AccGeometryCollectionTypeSerializer extends TypeSerializer<AccGeometryCollection> {

  private static final long serialVersionUID = 1L;

  public static final AccGeometryCollectionTypeSerializer INSTANCE =
      new AccGeometryCollectionTypeSerializer();

  public AccGeometryCollectionTypeSerializer() {}

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<AccGeometryCollection> duplicate() {
    return this;
  }

  @Override
  public AccGeometryCollection createInstance() {
    return new AccGeometryCollection();
  }

  @Override
  public AccGeometryCollection copy(AccGeometryCollection from) {
    if (from == null) {
      return null;
    }
    AccGeometryCollection copy = new AccGeometryCollection();
    copy.values = new ArrayList<>(from.values);
    copy.geography = from.geography;
    copy.srid = from.srid;
    return copy;
  }

  @Override
  public AccGeometryCollection copy(AccGeometryCollection from, AccGeometryCollection reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(AccGeometryCollection record, DataOutputView target) throws IOException {
    target.writeInt(record.values.size());
    for (byte[] value : record.values) {
      target.writeInt(value.length);
      target.write(value);
    }
    target.writeBoolean(record.geography != null);
    if (record.geography != null) {
      target.writeBoolean(record.geography);
    }
    target.writeInt(record.srid);
  }

  @Override
  public AccGeometryCollection deserialize(DataInputView source) throws IOException {
    AccGeometryCollection record = new AccGeometryCollection();
    int size = source.readInt();
    List<byte[]> values = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      byte[] value = new byte[source.readInt()];
      source.readFully(value);
      values.add(value);
    }
    record.values = values;
    record.geography = source.readBoolean() ? source.readBoolean() : null;
    record.srid = source.readInt();
    return record;
  }

  @Override
  public AccGeometryCollection deserialize(AccGeometryCollection reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int size = source.readInt();
    target.writeInt(size);
    for (int i = 0; i < size; i++) {
      int length = source.readInt();
      target.writeInt(length);
      byte[] value = new byte[length];
      source.readFully(value);
      target.write(value);
    }
    boolean hasGeography = source.readBoolean();
    target.writeBoolean(hasGeography);
    if (hasGeography) {
      target.writeBoolean(source.readBoolean());
    }
    target.writeInt(source.readInt());
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof AccGeometryCollectionTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<AccGeometryCollection> snapshotConfiguration() {
    return new AccGeometryCollectionSerializerSnapshot();
  }

  public static final class AccGeometryCollectionSerializerSnapshot
      implements TypeSerializerSnapshot<AccGeometryCollection> {
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
    public TypeSerializer<AccGeometryCollection> restoreSerializer() {
      return AccGeometryCollectionTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<AccGeometryCollection> resolveSchemaCompatibility(
        TypeSerializerSnapshot<AccGeometryCollection> oldSerializerSnapshot) {
      if (oldSerializerSnapshot instanceof AccGeometryCollectionSerializerSnapshot) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      } else {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
    }
  }
}
