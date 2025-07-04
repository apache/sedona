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
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShapeIndexGeography extends S2Geography {
  public S2ShapeIndex shapeIndex;

  /** Build an empty ShapeIndexGeography. */
  public ShapeIndexGeography() {
    super(GeographyKind.SHAPE_INDEX);
    this.shapeIndex = new S2ShapeIndex();
  }

  /** Build and immediately add one Geography. */
  public ShapeIndexGeography(S2Geography geog) {
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
  /**
   * Index every S2Shape from the given Geography.
   *
   * @return the last shapeId assigned.
   */
  public int addIndex(S2Geography geog) {
    int lastId = -1;
    for (int i = 0, n = geog.numShapes(); i < n; i++) {
      shapeIndex.add(geog.shape(i));
      // since add() appends to the end, its index is size-1
      lastId = shapeIndex.getShapes().size() - 1;
    }
    return lastId;
  }

  // encode
  @Override
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    // 0) Prepare a temporary output for the payload
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output tmpOut = new Output(baos);

    // 1) Lazy‐decode mode: write each shape individually into a StringVectorEncoder
    if (opts.isEnableLazyDecode()) {
      if (opts.getCodingHint() == EncodeOptions.CodingHint.FAST) {
        throw new IOException("Lazy output only supported with COMPACT hint");
      }
      // 1) Encode each shape in the index, using our “compact” path when possible
      for (S2Shape rawShape : shapeIndex.getShapes()) {
        // If it's a polygon or 1-chain polyline, do the custom lax encoding…
        if (!customCompactTaggedShapeEncoder(rawShape, tmpOut)) {
          // …otherwise fall back to the usual S2Geography encode
          // (we know every shape in this index is actually an S2Geography)
          ((S2Geography) rawShape).encode((UnsafeOutput) tmpOut, opts);
        }
      }
    } else {
      switch (opts.getCodingHint()) {
        case FAST:
          for (S2Shape rawShape : shapeIndex.getShapes()) {
            S2TaggedShapeCoder.FAST.encode((S2Shape) rawShape, tmpOut);
          }
          break;
        case COMPACT:
          for (S2Shape rawShape : shapeIndex.getShapes()) {
            S2TaggedShapeCoder.COMPACT.encode((S2Shape) rawShape, tmpOut);
          }
      }
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
  public static ShapeIndexGeography decode(Input in, EncodeTag tag) throws IOException {
    // cast to UnsafeInput—will work if you always pass a Kryo-backed stream
    if (!(in instanceof UnsafeInput)) {
      throw new IllegalArgumentException("Expected UnsafeInput");
    }
    return decode((UnsafeInput) in, tag);
  }

  public static ShapeIndexGeography decode(UnsafeInput in, EncodeTag tag) throws IOException {
    throw new IOException("Decode() not implemented for ShapeIndexGeography()");
  }

  /**
   * Encodes a tagged shape in compact form: - Polygons → converted to a lax polygon shape -
   * Single-chain polylines → converted to a lax polyline shape - Others → encoded directly
   */
  public static boolean customCompactTaggedShapeEncoder(S2Shape shape, Output out)
      throws IOException {
    // Polygon case: reify as a lax polygon for compact encoding
    if (shape instanceof S2Polygon.Shape) {
      List<Iterable<S2Point>> loops = new ArrayList<>(shape.numChains());
      for (int i = 0; i < shape.numChains(); i++) {
        loops.add(shape.chain(i));
      }
      S2LaxPolygonShape laxPolygon = S2LaxPolygonShape.create(loops);
      S2LaxPolygonShape.COMPACT_CODER.encode(laxPolygon, out);
      return true;
    }
    // Polyline case: only when exactly one chain
    else if (shape instanceof S2Polyline && shape.numChains() == 1) {
      Iterable<S2Point> verts = shape.chain(0);
      S2LaxPolylineShape laxPolyline = S2LaxPolylineShape.create(verts);
      S2LaxPolylineShape.COMPACT_CODER.encode(laxPolyline, out);
      return true;
    }
    // Fallback: encode the original shape directly
    else {
      return false;
    }
  }
}
