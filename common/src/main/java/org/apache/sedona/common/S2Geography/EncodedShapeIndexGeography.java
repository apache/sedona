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
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class EncodedShapeIndexGeography extends Geography {
  private static final Logger logger = Logger.getLogger(EncodedShapeIndexGeography.class.getName());

  public S2ShapeIndex shapeIndex;

  /** Build an empty ShapeIndexGeography. */
  public EncodedShapeIndexGeography() {
    super(GeographyKind.ENCODED_SHAPE_INDEX);
    this.shapeIndex = new S2ShapeIndex();
  }

  @Override
  public int dimension() {
    return -1;
  }

  @Override
  public int numShapes() {
    return shapeIndex.getShapes().size();
  }

  @Override
  public S2Shape shape(int id) {
    return shapeIndex.getShapes().get(id);
  }

  @Override
  public S2Region region() {
    return new S2ShapeIndexRegion(shapeIndex);
  }

  /**
   * Index every S2Shape from the given Geography.
   *
   * @return the last shapeId assigned.
   */
  public int addIndex(Geography geog) {
    int lastId = -1;
    for (int i = 0, n = geog.numShapes(); i < n; i++) {
      shapeIndex.add(geog.shape(i));
      // since add() appends to the end, its index is size-1
      lastId = shapeIndex.getShapes().size() - 1;
    }
    return lastId;
  }

  /** Add one raw shape into the index, return its new ID */
  public int addIndex(S2Shape shape) {
    shapeIndex.add(shape);
    return shapeIndex.getShapes().size() - 1;
  }

  @Override
  public void encode(UnsafeOutput os, EncodeOptions opts) throws IOException {
    throw new IOException("Encode() not implemented for EncodedShapeIndexGeography()");
  }

  // decode
  /** This is what decodeTagged() actually calls */
  public static EncodedShapeIndexGeography decode(Input in, EncodeTag tag) throws IOException {
    // cast to UnsafeInput—will work if you always pass a Kryo-backed stream
    if (!(in instanceof UnsafeInput)) {
      throw new IllegalArgumentException("Expected UnsafeInput");
    }
    return decode((UnsafeInput) in, tag);
  }

  public static EncodedShapeIndexGeography decode(UnsafeInput in, EncodeTag tag)
      throws IOException {
    EncodedShapeIndexGeography encodedShapeIndexGeography = new EncodedShapeIndexGeography();

    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty EncodedShapeIndexGeography.");
      return encodedShapeIndexGeography;
    }

    // 2) Skip past any covering cell-IDs written by encodeTagged
    tag.skipCovering(in);

    int length = in.readInt();
    if (length < 0) {
      throw new IOException("Invalid payload length: " + length);
    }

    // 2) read exactly that many bytes
    byte[] payload = new byte[length];
    in.readBytes(payload, 0, length);
    // 3) hand *only* those bytes to S2‐Coder via Bytes adapter
    PrimitiveArrays.Bytes bytes =
        new PrimitiveArrays.Bytes() {
          @Override
          public long length() {
            return payload.length;
          }

          @Override
          public byte get(long i) {
            return payload[(int) i];
          }
        };

    List<S2Shape> s2Shapes = new ArrayList<>();
    if (tag.isCompact()) {
      s2Shapes = VectorCoder.COMPACT_SHAPE.decode(bytes);
    } else {
      s2Shapes = VectorCoder.FAST_SHAPE.decode(bytes);
    }
    for (S2Shape shape : s2Shapes) encodedShapeIndexGeography.addIndex(shape);
    return encodedShapeIndexGeography;
  }
}
