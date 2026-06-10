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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.sedona.common.geometryObjects.Box3D;
import org.junit.Test;

public class Box3DTypeSerializerTest {

  private final Box3DTypeSerializer serializer = Box3DTypeSerializer.INSTANCE;

  private byte[] serialize(Box3D box) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    serializer.serialize(box, new DataOutputViewStreamWrapper(out));
    return out.toByteArray();
  }

  private Box3D deserialize(byte[] bytes) throws IOException {
    return serializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
  }

  @Test
  public void roundTripOrderedBox() throws IOException {
    Box3D box = new Box3D(0.0, 0.0, -3.0, 5.0, 10.0, 7.0);
    byte[] bytes = serialize(box);
    // 1 presence byte + 6 doubles.
    assertEquals(49, bytes.length);
    assertEquals(box, deserialize(bytes));
  }

  @Test
  public void roundTripInvertedBoundsVerbatim() throws IOException {
    // The serializer stores bounds as-is; ordering validation lives in the predicates, not here.
    Box3D inverted = new Box3D(5.0, 5.0, 5.0, 0.0, 0.0, 0.0);
    assertEquals(inverted, deserialize(serialize(inverted)));
  }

  @Test
  public void roundTripNegativeAndFractionalBounds() throws IOException {
    Box3D box = new Box3D(-1.5, -2.25, -3.125, 4.5, 6.75, 8.875);
    assertEquals(box, deserialize(serialize(box)));
  }

  @Test
  public void serializeNullWritesSinglePresenceByte() throws IOException {
    byte[] bytes = serialize(null);
    assertEquals(1, bytes.length);
    assertEquals(0, bytes[0]);
    assertNull(deserialize(bytes));
  }

  @Test
  public void rawCopyTransfersBytesForNonNull() throws IOException {
    Box3D box = new Box3D(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
    byte[] original = serialize(box);

    // Exercise copy(DataInputView, DataOutputView): the raw byte-shuffling path that skips
    // deserialization. The copied bytes must round-trip back to the same Box3D.
    ByteArrayOutputStream copied = new ByteArrayOutputStream();
    serializer.copy(
        new DataInputViewStreamWrapper(new ByteArrayInputStream(original)),
        new DataOutputViewStreamWrapper(copied));

    byte[] copiedBytes = copied.toByteArray();
    assertEquals(original.length, copiedBytes.length);
    assertEquals(box, deserialize(copiedBytes));
  }

  @Test
  public void rawCopyTransfersBytesForNull() throws IOException {
    byte[] original = serialize(null);
    ByteArrayOutputStream copied = new ByteArrayOutputStream();
    serializer.copy(
        new DataInputViewStreamWrapper(new ByteArrayInputStream(original)),
        new DataOutputViewStreamWrapper(copied));
    assertEquals(1, copied.toByteArray().length);
    assertNull(deserialize(copied.toByteArray()));
  }

  @Test
  public void objectCopyProducesEqualInstance() {
    Box3D box = new Box3D(0.0, 0.0, 0.0, 1.0, 1.0, 1.0);
    Box3D copy = serializer.copy(box);
    assertEquals(box, copy);
    assertNull(serializer.copy((Box3D) null));
  }

  @Test
  public void serializerEqualityAndImmutability() {
    assertEquals(serializer, new Box3DTypeSerializer());
    assertEquals(serializer.hashCode(), new Box3DTypeSerializer().hashCode());
    assertTrue(serializer.isImmutableType());
    assertTrue(serializer.duplicate() == serializer);
  }
}
