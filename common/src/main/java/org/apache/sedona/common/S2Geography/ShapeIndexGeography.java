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

import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ShapeIndexGeography extends Geography {
  public S2ShapeIndex shapeIndex;

  /** Build an empty ShapeIndexGeography. */
  public ShapeIndexGeography() {
    super(GeographyKind.SHAPE_INDEX);
    this.shapeIndex = new S2ShapeIndex();
  }

  /** Build and immediately add one Geography. */
  public ShapeIndexGeography(Geography geog) {
    super(GeographyKind.SHAPE_INDEX);
    this.shapeIndex = new S2ShapeIndex();
    addIndex(geog);
  }

  /** Create a ShapeIndexGeography with a custom max-edges-per-cell. */
  public ShapeIndexGeography(int maxEdgesPerCell) {
    super(GeographyKind.SHAPE_INDEX);
    S2ShapeIndex.Options options = new S2ShapeIndex.Options();
    options.setMaxEdgesPerCell(maxEdgesPerCell);
    this.shapeIndex = new S2ShapeIndex(options);
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
    S2Shape raw = shapeIndex.getShapes().get(id);
    return raw;
  }

  @Override
  public S2Region region() {
    return new S2ShapeIndexRegion(shapeIndex);
  }

  /** Index every S2Shape from the given Geography. */
  public void addIndex(Geography geog) {
    for (int i = 0, n = geog.numShapes(); i < n; i++) {
      shapeIndex.add(geog.shape(i));
    }
  }

  // encode
  @Override
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    // 0) Prepare a temporary output for the payload
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output tmpOut = new Output(baos);
    switch (opts.getCodingHint()) {
      case FAST:
        VectorCoder.FAST_SHAPE.encode(shapeIndex.getShapes(), tmpOut);
        break;
      case COMPACT:
        VectorCoder.COMPACT_SHAPE.encode(shapeIndex.getShapes(), tmpOut);
    }
    // 2) Finish payload
    tmpOut.flush();
    byte[] payload = baos.toByteArray();

    // 3) Write length-prefix + payload to the real out
    out.writeInt(payload.length, /* optimizePositive= */ false);
    out.writeBytes(payload);

    // 4) Encode the index’s quadtree structure and flush
    S2ShapeIndexCoder.INSTANCE.encode(shapeIndex, out);
    out.flush();
  }

  // decode
  /** This is what decodeTagged() actually calls */
  public static ShapeIndexGeography decode(UnsafeInput in, EncodeTag tag) throws IOException {
    throw new IOException("Decode() not implemented for ShapeIndexGeography()");
  }
}
