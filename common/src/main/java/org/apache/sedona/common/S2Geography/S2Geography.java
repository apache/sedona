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

import static org.apache.sedona.common.S2Geography.S2Geography.GeographyKind.POINT;

import com.google.common.geometry.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * An abstract class represent S2Geography. Has 6 subtypes of geography: POINT, POLYLINE, POLYGON,
 * GEOGRAPHY_COLLECTION, SHAPE_INDEX, ENCODED_SHAPE_INDEX.
 */
public class S2Geography {
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
  public int dimension() {
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
  public int numShapes() {
    return 0;
  }

  /**
   * Returns the given S2Shape (where 0 <= id < num_shapes()). The caller retains ownership of the
   * S2Shape but the data pointed to by the object requires that the underlying Geography outlives
   * the returned object.
   *
   * @param id (where 0 <= id < num_shapes())
   * @return the given S2Shape
   */
  public S2Shape shape(int id) {
    return null;
  }

  /**
   * Returns an S2Region that represents the object. The caller retains ownership of the S2Region
   * but the data pointed to by the object requires that the underlying Geography outlives the
   * returned object.
   *
   * @return S2Region
   */
  public S2Region region() {
    return null;
  }

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
   * (e.g., which geography type or flags). Encode this geography into a stream as: 1) a 4-byte
   * EncodeTag header (see EncodeTag encode / decode) 2) coveringSize × 8-byte cell-ids 3) the raw
   * shape payload (point/polyline/polygon) via the built-in coder
   *
   * @param options CodingHint.FAST / CodingHint.COMPACT
   */
  public void encodeTagged(OutputStream outStream, EncodeOptions options) throws IOException {
    DataOutputStream out = new DataOutputStream(outStream);

    // 1) build + write tag header
    EncodeTag tag = new EncodeTag();
    tag.setKind(kind);
    if (numShapes() == 0) {
      tag.setFlags((byte) (tag.getFlags() | EncodeTag.FLAG_EMPTY));
    }
    // compute covering if requested
    List<S2CellId> cover = new ArrayList<>();
    if (options.isIncludeCovering()) {
      getCellUnionBound(cover);
    }
    tag.setCoveringSize((byte) cover.size());
    tag.encode(out);

    // 2) write each cell-id
    for (S2CellId cid : cover) {
      out.writeLong(cid.id());
    }

    // 3) encode by GeographyKind
    switch (kind) {
      case POINT:
        ((PointGeography) this).encodeTagged(out, options);
        break;

        // TODO: handle POLYLINE, POLYGON, etc.
      default:
        throw new IllegalArgumentException("encodeTagged not implemented for kind=" + kind);
    }

    out.flush();
  }

  /**
   * Reads a tagged geography from the stream (header + covering + payload). Dispatches to the right
   * subclass decoder.
   */
  public static S2Geography decodeTagged(DataInputStream is) throws IOException {
    DataInputStream in = new DataInputStream(is);

    // 1) Read the full 4-byte header:
    int kindVal = in.readUnsignedByte();
    byte flags = (byte) in.readUnsignedByte();
    int coverCount = in.readUnsignedByte();
    int reserved = in.readUnsignedByte();
    if (reserved != 0) {
      throw new IOException("Reserved header byte must be 0, was " + reserved);
    }

    GeographyKind kind = GeographyKind.fromKind(kindVal);
    EncodeTag tag = new EncodeTag();
    tag.setKind(kind);
    tag.setFlags(flags);
    tag.setCoveringSize((byte) coverCount);

    // 2) If CELL_CENTER, read exactly one ID *as* payload, and return immediately:
    if (kind == GeographyKind.CELL_CENTER) {
      long id = in.readLong();
      return new PointGeography(new S2CellId(id).toPoint());
    }

    // 2) Skip any covering IDs
    for (int i = 0; i < coverCount; i++) {
      in.readLong();
    }

    // 3) Dispatch to the payload decoder
    switch (kind) {
      case POINT:
        return PointGeography.decodeTagged(in, tag);
        case POLYLINE:
          return PolylineGeography.decodeTagged(in, tag);
      default:
        throw new IllegalArgumentException("Unsupported kind " + kind);
    }
  }
}
