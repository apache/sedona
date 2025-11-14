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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.google.common.geometry.S2CellId;
import java.io.*;
import java.util.List;
import org.apache.sedona.common.S2Geography.Geography.GeographyKind;

/**
 * A 4 byte prefix for encoded geographies. Builds a 5-byte header (EncodeTag) containing 1 byte:
 * kind 1 byte: flags 1 byte: coveringSize 1 byte: reserved (must be 0)
 */
public class EncodeTag {
  /**
   * Subclass of S2Geography whose decode() method will be invoked. Encoded using a single unsigned
   * byte (represented as an int in Java, range 0–255).
   */
  private GeographyKind kind = GeographyKind.UNINITIALIZED;
  /**
   * Flags for encoding metadata. one flag {@code kFlagEmpty} is supported, which is set if and only
   * if the geography contains zero shapes. second flag {@code FlagCompact}, which is set if user
   * set COMPACT encoding type
   */
  private byte flags = 0;
  // ——— Bit‐masks for our one‐byte flags field ———————————————————
  /** set if geography has zero shapes */
  public static final byte FLAG_EMPTY = 1 << 0;
  /** set if using COMPACT coding; if clear, we’ll treat as FAST */
  public static final byte FLAG_COMPACT = 1 << 1;
  // bits 2–7 are still unused (formerly “reserved”)
  /**
   * Number of S2CellId entries that follow this tag. A value of zero (i.e., an empty covering)
   * means no covering was written, but this does not imply that the geography itself is empty.
   */
  private byte coveringSize = 0;
  /** Reserved byte for future use. Must be set to 0. */
  private byte reserved = 0;

  // ——— Write the 4-byte tag header ——————————————————————————————————————
  public EncodeTag() {}

  public EncodeTag(EncodeOptions opts) {
    if (opts.getCodingHint() == EncodeOptions.CodingHint.COMPACT) {
      flags |= FLAG_COMPACT;
    }
  }
  /** Write exactly 4 bytes: [kind|flags|coveringSize|reserved]. */
  public void encode(Output out) throws IOException {
    out.writeByte(kind.getKind());
    out.writeByte(flags);
    out.writeByte(coveringSize);
    out.writeByte(reserved);
  }
  // ——— Read it back ————————————————————————————————————————————————

  /** Reads exactly 4 bytes (in the same order) from the stream. */
  public static EncodeTag decode(Input in) throws IOException {
    EncodeTag tag = new EncodeTag();
    tag.kind = GeographyKind.fromKind(in.readByte());
    tag.flags = in.readByte();
    tag.coveringSize = in.readByte();
    tag.reserved = in.readByte();
    if (tag.reserved != 0)
      throw new IOException("Reserved header byte must be 0, was " + tag.reserved);
    return tag;
  }

  // ——— Helpers for the optional covering list —————————————————————————

  /** Read coveringSize many cell-ids and add them to cellIds. */
  public void decodeCovering(UnsafeInput in, List<S2CellId> cellIds) throws IOException {
    int count = coveringSize & 0xFF;
    for (int i = 0; i < count; i++) {
      long id = in.readLong();
      cellIds.add(new S2CellId(id));
    }
  }

  /** Skip over coveringSize many cell-ids in the stream. */
  public void skipCovering(UnsafeInput in) throws IOException {
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
    return this.kind;
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

  /** mark or unmark the EMPTY flag */
  public void setEmpty(boolean empty) {
    if (empty) flags |= FLAG_EMPTY;
    else flags &= ~FLAG_EMPTY;
  }

  /** choose COMPACT (true) or FAST (false) */
  public void setCompact(boolean compact) {
    if (compact) flags |= FLAG_COMPACT;
    else flags &= ~FLAG_COMPACT;
  }

  public boolean isEmpty() {
    return (flags & FLAG_EMPTY) != 0;
  }

  public boolean isCompact() {
    return (flags & FLAG_COMPACT) != 0;
  }

  public boolean isFast() {
    return !isCompact();
  }
}
