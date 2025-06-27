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

import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class represent S2Geography. Has 6 subtypes of geography: POINT, POLYLINE, POLYGON,
 * GEOGRAPHY_COLLECTION, SHAPE_INDEX, ENCODED_SHAPE_INDEX.
 */
public abstract class S2Geography {
  private static final Logger logger = LoggerFactory.getLogger(S2Geography.class.getName());

  private static final int BUFFER_SIZE = 4 * 1024;

  protected final GeographyKind kind;

  protected S2Geography(GeographyKind kind) {
    this.kind = kind;
  }

  public enum GeographyKind {
    UNINITIALIZED(0),
    POINT(1),
    POLYLINE(2),
    POLYGON(3),
    GEOGRAPHY_COLLECTION(4),
    SHAPE_INDEX(5),
    ENCODED_SHAPE_INDEX(6),
    CELL_CENTER(7);

    private final int kind;

    GeographyKind(int kind) {
      this.kind = kind;
    }

    /** Returns the integer tag for this kind. */
    public int getKind() {
      return kind;
    }
    /**
     * Look up the enum by its integer tag.
     *
     * @throws IllegalArgumentException if no matching kind exists.
     */
    public static GeographyKind fromKind(int kind) {
      for (GeographyKind k : values()) {
        if (k.getKind() == kind) return k;
      }
      throw new IllegalArgumentException("Unknown GeographyKind: " + kind);
    }
  }
  /**
   * @return 0, 1, or 2 if all Shape()s that are returned will have the same dimension (i.e., they
   *     are all points, all lines, or all polygons).
   */
  public abstract int dimension();

  /**
   * Usage of checking all shapes in side collection geography
   *
   * @return
   */
  protected final int computeDimensionFromShapes() {
    if (numShapes() == 0) return -1;
    int dim = shape(0).dimension();
    for (int i = 1; i < numShapes(); ++i) {
      if (dim != shape(i).dimension()) return -1;
    }
    return dim;
  }

  /**
   * @return The number of S2Shape objects needed to represent this Geography
   */
  public abstract int numShapes();

  /**
   * Returns the given S2Shape (where 0 <= id < num_shapes()). The caller retains ownership of the
   * S2Shape but the data pointed to by the object requires that the underlying Geography outlives
   * the returned object.
   *
   * @param id (where 0 <= id < num_shapes())
   * @return the given S2Shape
   */
  public abstract S2Shape shape(int id);

  /**
   * Returns an S2Region that represents the object. The caller retains ownership of the S2Region
   * but the data pointed to by the object requires that the underlying Geography outlives the
   * returned object.
   *
   * @return S2Region
   */
  public abstract S2Region region();

  /**
   * Adds an unnormalized set of S2CellIDs to `cell_ids`. This is intended to be faster than using
   * Region().GetCovering() directly and to return a small number of cells that can be used to
   * compute a possible intersection quickly.
   */
  public void getCellUnionBound(List<S2CellId> cellIds) {
    // Build a shape index of all shapes in this geography
    S2ShapeIndex index = new S2ShapeIndex();
    for (int i = 0; i < numShapes(); i++) {
      index.add(shape(i));
    }
    // Create a region from the index and delegate covering
    S2ShapeIndexRegion region = new S2ShapeIndexRegion(index);
    region.getCellUnionBound(cellIds);
  }

  // ─── Encoding / decoding machinery ────────────────────────────────────────────
  /**
   * Serialize this geography to an encoder. This does not include any encapsulating information
   * (e.g., which geography type or flags). Encode this geography into a stream as: 1) a 5-byte
   * EncodeTag header (see EncodeTag encode / decode) 2) coveringSize × 8-byte cell-ids 3) the raw
   * shape payload (point/polyline/polygon) via the built-in coder
   *
   * @param opts CodingHint.FAST / CodingHint.COMPACT / Include or omit the cell‐union covering
   *     prefix
   */
  public void encodeTagged(OutputStream os, EncodeOptions opts) throws IOException {
    UnsafeOutput out = new UnsafeOutput(os, BUFFER_SIZE);
    EncodeTag tag = new EncodeTag(opts);
    List<S2CellId> cover = new ArrayList<>();

    // EMPTY
    if (this.numShapes() == 0) {
      tag.setKind(GeographyKind.fromKind(this.kind.kind));
      tag.setFlags((byte) (tag.getFlags() | EncodeTag.FLAG_EMPTY));
      tag.setCoveringSize((byte) 0);
      tag.encode(out);
      out.flush();
      return;
    }

    // 1) Get covering if needed
    if (opts.isIncludeCovering()) {
      getCellUnionBound(cover);
      if (cover.size() > 256) {
        cover.clear();
        logger.warn("Covering size too large (> 256) — clear Covering");
      }
    }

    // 2) Write tag header
    tag.setKind(GeographyKind.fromKind(this.kind.kind));
    tag.setCoveringSize((byte) cover.size());
    tag.encode(out);

    // Encode the covering
    for (S2CellId c2 : cover) {
      out.writeLong(c2.id());
    }

    // 3) Write the geography
    this.encode(out, opts);
    out.flush();
  }

  public S2Geography decodeTagged(InputStream is) throws IOException {
    // wrap ONCE
    UnsafeInput kryoIn = new UnsafeInput(is, BUFFER_SIZE);
    EncodeTag topTag = EncodeTag.decode(kryoIn);
    // 1) decode the tag
    return S2Geography.decode(kryoIn, topTag);
  }

  public static S2Geography decode(UnsafeInput in, EncodeTag tag) throws IOException {
    // 2) dispatch to subclass's decode method according to tag.kind
    switch (tag.getKind()) {
      case CELL_CENTER:
      case POINT:
        return PointGeography.decode(in, tag);
      case POLYLINE:
        return PolylineGeography.decode(in, tag);
      case POLYGON:
        return PolygonGeography.decode(in, tag);
        //      case GEOGRAPHY_COLLECTION:
        //        return GeographyCollection.decode(in, tag);
        //      case SHAPE_INDEX:
        //        return EncodedShapeIndexGeography.decode(in, tag);
      default:
        throw new IOException("Unsupported GeographyKind for decoding: " + tag.getKind());
    }
  }

  protected abstract void encode(UnsafeOutput os, EncodeOptions opts) throws IOException;
}
