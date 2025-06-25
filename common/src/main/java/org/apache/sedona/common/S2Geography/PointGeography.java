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
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.*;
import com.google.common.geometry.PrimitiveArrays.Bytes;
import java.io.*;
import java.util.*;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PointGeography extends S2Geography {
  private static final Logger logger = LoggerFactory.getLogger(PointGeography.class.getName());

  private static final int BUFFER_SIZE = 4 * 1024;

  private final List<S2Point> points = new ArrayList<>();

  /** Constructs an empty PointGeography. */
  public PointGeography() {
    super(GeographyKind.POINT);
  }

  /** Constructs especially for CELL_CENTER */
  private PointGeography(GeographyKind kind, S2Point point) {
    super(kind); // can be POINT or CELL_CENTER
    points.add(point);
  }

  /** Constructs a single-point geography. */
  public PointGeography(S2Point point) {
    this();
    points.add(point);
  }

  /** Constructs from a list of points. */
  public PointGeography(List<S2Point> pts) {
    this();
    points.addAll(pts);
  }

  @Override
  public int dimension() {
    return points.isEmpty() ? -1 : 0;
  }

  @Override
  public int numShapes() {
    return points.isEmpty() ? 0 : 1;
  }

  @Override
  public S2Shape shape(int id) {
    return S2Point.Shape.fromList(points);
  }

  @Override
  public S2Region region() {
    if (points.isEmpty()) {
      return S2Cap.empty();
    } else if (points.size() == 1) {
      return new S2PointRegion(points.get(0));
    } else {
      // Union of all point regions
      Collection<S2Region> pointRegionCollection = new ArrayList<>();
      for (S2Point p : points) {
        pointRegionCollection.add(new S2PointRegion(p));
      }
      return new S2RegionUnion(pointRegionCollection);
    }
  }

  @Override
  public void getCellUnionBound(List<S2CellId> cellIds) {
    if (points.size() < 10) {
      // For small point sets, cover each point individually
      for (S2Point p : points) {
        cellIds.add(S2CellId.fromPoint(p));
      }
    } else {
      // Fallback to the default covering logic in S2Geography
      super.getCellUnionBound(cellIds);
    }
  }

  /** Returns an immutable view of the points. */
  public List<S2Point> getPoints() {
    // List.copyOf makes an unmodifiable copy under the hood
    return List.copyOf(points);
  }

  // -------------------------------------------------------
  // EncodeTagged / DecodeTagged
  // -------------------------------------------------------

  @Override
  public void encodeTagged(OutputStream os, EncodeOptions opts) throws IOException {
    UnsafeOutput out = new UnsafeOutput(os, BUFFER_SIZE);
    if (points.size() == 1 && opts.getCodingHint() == EncodeOptions.CodingHint.COMPACT) {
      // Optimized encoding which only uses covering to represent the point
      S2CellId cid = S2CellId.fromPoint(points.get(0));
      // Only encode this for very high levels: because the covering *is* the
      // representation, we will have a very loose covering if the level is low.
      // Level 23 has a cell size of ~1 meter
      // (http://s2geometry.io/resources/s2cell_statistics)
      if (cid.level() >= 23) {
        out.writeByte(GeographyKind.CELL_CENTER.getKind());
        out.writeByte(0); // POINT kind
        out.writeByte(1); // flag
        out.writeByte(0); // coveringSize
        out.writeByte(2); // COMPACT encode type
        out.writeLong(cid.id());
        out.flush();
        return;
      }
      super.encodeTagged(os, opts); // Not exactly encodable as a cell center
    }
    // In other cases, fallback to the default encodeTagged implementation:
    super.encodeTagged(os, opts);
  }

  @Override
  protected void encode(Output out, EncodeOptions opts) throws IOException {
    // Encode point payload using selected hint
    S2Point.Shape shp = S2Point.Shape.fromList(points);
    switch (opts.getCodingHint()) {
      case FAST:
        S2Point.Shape.FAST_CODER.encode(shp, out);
        break;
      case COMPACT:
        S2Point.Shape.COMPACT_CODER.encode(shp, out);
    }
  }

  public static PointGeography decode(Input in, EncodeTag tag) throws IOException {
    PointGeography geo = new PointGeography();

    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.warn("Decoded empty PointGeography.");
      return geo;
    }

    // Optimized 1-point COMPACT situation
    if (tag.getKind() == GeographyKind.CELL_CENTER) {
      long id = in.readLong();
      geo = new PointGeography(new S2CellId(id).toPoint());
      logger.info("Decoded compact single-point geography via cell center.");
      return geo;
    }

    // skip cover
    tag.skipCovering(in);

    // Grab Kryo’s backing buffer & bounds
    Input kryoIn = (Input) in;
    final byte[] backing = kryoIn.getBuffer();
    final int start = kryoIn.position();
    final int end = kryoIn.limit();
    final long length = (long) end - start; // fits in an int normally

    // Zero-copy Bytes view
    Bytes bytes =
        new Bytes() {
          @Override
          public long length() {
            return length;
          }

          @Override
          public byte get(long idx) {
            if (idx < 0 || idx >= length) {
              throw new IndexOutOfBoundsException(idx + " not in [0," + length + ")");
            }
            // safe to cast to int because length <= backing.length
            return backing[start + (int) idx];
          }
        };

    PrimitiveArrays.Cursor cursor = bytes.cursor();
    List<S2Point> points;
    switch (tag.getEncodeType()) {
      case 1:
        points = S2Point.Shape.FAST_CODER.decode(bytes, cursor);
        break;
      case 2:
        points = S2Point.Shape.COMPACT_CODER.decode(bytes, cursor);
        break;
      default:
        throw new IllegalArgumentException("Unknown coding hint");
    }
    geo.points.addAll(points);
    return geo;
  }
}
