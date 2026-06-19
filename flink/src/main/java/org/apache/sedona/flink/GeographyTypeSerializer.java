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
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.GeographyWKBSerializer;
import org.apache.sedona.common.S2Geography.SinglePointGeography;

public class GeographyTypeSerializer extends TypeSerializer<Geography> {

  private static final long serialVersionUID = 1L;

  public static final GeographyTypeSerializer INSTANCE = new GeographyTypeSerializer();

  public GeographyTypeSerializer() {}

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Geography> duplicate() {
    return this;
  }

  @Override
  public Geography createInstance() {
    return new SinglePointGeography();
  }

  @Override
  public Geography copy(Geography from) {
    if (from == null) {
      return null;
    }
    // Geography has no copy()/clone(); a serialize/deserialize round-trip is a safe deep copy.
    try {
      return GeographyWKBSerializer.deserialize(GeographyWKBSerializer.serialize(from));
    } catch (IOException e) {
      throw new RuntimeException("Failed to copy Geography", e);
    }
  }

  @Override
  public Geography copy(Geography from, Geography reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Geography record, DataOutputView target) throws IOException {
    if (record == null) {
      target.writeInt(-1);
    } else {
      byte[] data = GeographyWKBSerializer.serialize(record);
      target.writeInt(data.length);
      target.write(data);
    }
  }

  @Override
  public Geography deserialize(DataInputView source) throws IOException {
    int length = source.readInt();
    if (length == -1) {
      return null;
    }
    byte[] data = new byte[length];
    source.readFully(data);
    return GeographyWKBSerializer.deserialize(data);
  }

  @Override
  public Geography deserialize(Geography reuse, DataInputView source) throws IOException {
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
    return obj instanceof GeographyTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Geography> snapshotConfiguration() {
    return new GeographySerializerSnapshot();
  }

  public static final class GeographySerializerSnapshot
      implements TypeSerializerSnapshot<Geography> {
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
    public TypeSerializer<Geography> restoreSerializer() {
      return GeographyTypeSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Geography> resolveSchemaCompatibility(
        TypeSerializer<Geography> newSerializer) {
      if (newSerializer instanceof GeographyTypeSerializer) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      } else {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
    }
  }
}
