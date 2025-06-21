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
package org.apache.sedona.common.S2Geography;

import com.google.common.geometry.S2CellId;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.sedona.common.S2Geography.S2Geography.GeographyKind;

public class EncodeTag {
  private GeographyKind kind = GeographyKind.UNINITIALIZED;
  private byte flags = 0;
  private byte coveringSize = 0;
  private byte reserved = 0;

  /** If set, geography has zero shapes. */
  public static final byte FLAG_EMPTY = 1;

  public EncodeTag() {}

  // ——— Write the 4-byte tag header ——————————————————————————————————————

  /** Write exactly 4 bytes: [kind|flags|coveringSize|reserved]. */
  public void encode(DataOutputStream out) throws IOException {
    out.writeByte(kind.getKind());
    out.writeByte(flags);
    out.writeByte(coveringSize);
    out.writeByte(reserved); // <-- this makes it 4 bytes
  }
  // ——— Read it back ————————————————————————————————————————————————

  /** Reads exactly 4 bytes (in the same order) from the stream. */
  public static EncodeTag decode(DataInputStream in) throws IOException {
    EncodeTag tag = new EncodeTag();
    tag.kind = GeographyKind.fromKind(in.readUnsignedByte());
    tag.flags = (byte) in.readUnsignedByte();
    tag.coveringSize = (byte) in.readUnsignedByte();
    int r = in.readUnsignedByte();
    if (r != 0) throw new IOException("Reserved header byte must be 0, was " + r);
    return tag;
  }

  // ——— Helpers for the optional covering list —————————————————————————

  /** Read coveringSize many cell-ids and add them to cellIds. */
  public void decodeCovering(DataInputStream in, List<S2CellId> cellIds) throws IOException {
    int count = coveringSize & 0xFF;
    for (int i = 0; i < count; i++) {
      long id = in.readLong();
      cellIds.add(new S2CellId(id));
    }
  }

  /** Skip over coveringSize many cell-ids in the stream. */
  public void skipCovering(DataInputStream in) throws IOException {
    int count = coveringSize & 0xFF;
    for (int i = 0; i < count; i++) {
      in.readLong();
    }
  }

  /** Ensure we didn’t accidentally write a non-zero reserved byte. */
  public void validate() {
    if (reserved != 0) {
      throw new IllegalStateException("EncodeTag.reserved must be 0, was " + (reserved & 0xFF));
    }
  }

  // ——— Getters / setters ——————————————————————————————————————————

  public GeographyKind getKind() {
    return kind;
  }

  public void setKind(GeographyKind kind) {
    this.kind = kind;
  }

  public byte getFlags() {
    return flags;
  }

  public void setFlags(byte flags) {
    this.flags = flags;
  }

  public byte getCoveringSize() {
    return coveringSize;
  }

  public void setCoveringSize(byte size) {
    this.coveringSize = size;
  }
}
