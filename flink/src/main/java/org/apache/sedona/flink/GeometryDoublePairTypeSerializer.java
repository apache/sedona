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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.locationtech.jts.geom.Geometry;

public class GeometryDoublePairTypeSerializer extends TypeSerializer<Pair<Geometry, Double>> {

  private static final long serialVersionUID = 1L;

  public static final GeometryDoublePairTypeSerializer INSTANCE =
      new GeometryDoublePairTypeSerializer();

  public GeometryDoublePairTypeSerializer() {}

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Pair<Geometry, Double>> duplicate() {
    return this;
  }

  @Override
  public Pair<Geometry, Double> createInstance() {
    return Pair.of(null, 0.0);
  }

  @Override
  public Pair<Geometry, Double> copy(Pair<Geometry, Double> from) {
    if (from == null) {
      return null;
    }
    Geometry geom = from.getLeft() == null ? null : (Geometry) from.getLeft().copy();
    return Pair.of(geom, from.getRight());
  }

  @Override
  public Pair<Geometry, Double> copy(Pair<Geometry, Double> from, Pair<Geometry, Double> reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Pair<Geometry, Double> record, DataOutputView target) throws IOException {
    if (record == null) {
      target.writeBoolean(false);
    } else {
      target.writeBoolean(true);
      GeometryTypeSerializer.INSTANCE.serialize(record.getLeft(), target);
      target.writeDouble(record.getRight());
    }
  }

  @Override
  public Pair<Geometry, Double> deserialize(DataInputView source) throws IOException {
    if (!source.readBoolean()) {
      return null;
    }
    Geometry geom = GeometryTypeSerializer.INSTANCE.deserialize(source);
    double value = source.readDouble();
    return Pair.of(geom, value);
  }

  @Override
  public Pair<Geometry, Double> deserialize(Pair<Geometry, Double> reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    boolean present = source.readBoolean();
    target.writeBoolean(present);
    if (present) {
      GeometryTypeSerializer.INSTANCE.copy(source, target);
      target.writeDouble(source.readDouble());
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof GeometryDoublePairTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Pair<Geometry, Double>> snapshotConfiguration() {
    return new GeometryDoublePairSerializerSnapshot();
  }

  public static final class GeometryDoublePairSerializerSnapshot
      implements TypeSerializerSnapshot<Pair<Geometry, Double>> {
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
    public TypeSerializer<Pair<Geometry, Double>> restoreSerializer() {
      return GeometryDoublePairTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Pair<Geometry, Double>> resolveSchemaCompatibility(
        TypeSerializerSnapshot<Pair<Geometry, Double>> oldSerializerSnapshot) {
      if (oldSerializerSnapshot instanceof GeometryDoublePairSerializerSnapshot) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      } else {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
    }
  }
}
