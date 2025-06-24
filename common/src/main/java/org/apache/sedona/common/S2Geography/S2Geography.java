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

import com.google.common.geometry.*;
import java.io.*;
import java.util.List;

/**
 * An abstract class represent S2Geography. Has 6 subtypes of geography: POINT, POLYLINE, POLYGON,
 * GEOGRAPHY_COLLECTION, SHAPE_INDEX, ENCODED_SHAPE_INDEX.
 */
public abstract class S2Geography {
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
  public abstract void encode(OutputStream os, EncodeOptions opts) throws IOException;

  // public abstract S2Geography decode(DataInputStream in) throws IOException;
}
