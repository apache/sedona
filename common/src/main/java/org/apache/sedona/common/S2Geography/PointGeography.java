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
import com.google.common.geometry.PrimitiveArrays.Bytes;
import com.google.common.geometry.PrimitiveArrays.Cursor;
import java.io.*;
import java.util.*;
import java.util.List;
import java.util.logging.Logger;

public class PointGeography extends S2Geography {
  private static final Logger logger = Logger.getLogger(PointGeography.class.getName());

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
    return Collections.unmodifiableList(points);
  }

  // -------------------------------------------------------
  // EncodeTagged / DecodeTagged
  // -------------------------------------------------------

  @Override
  public void encode(OutputStream os, EncodeOptions opts) throws IOException {
    DataOutputStream out = new DataOutputStream(os);

    // EMPTY
    if (points.isEmpty()) {
      EncodeTag tag = new EncodeTag(opts);
      tag.setKind(GeographyKind.POINT);
      tag.setFlags((byte) (tag.getFlags() | EncodeTag.FLAG_EMPTY));
      tag.setCoveringSize((byte) 0);
      tag.encode(out);
      return;
    }

    if (points.size() == 1 && opts.getCodingHint() == EncodeOptions.CodingHint.COMPACT) {
      S2CellId cid = S2CellId.fromPoint(points.get(0));
      if (cid.level() >= 23) {
        out.writeByte(GeographyKind.CELL_CENTER.getKind());
        out.writeByte(0); // POINT kind
        out.writeByte(1); // flag
        out.writeByte(0); // coveringSize
        out.writeByte(2); // COMPACT encode type
        out.writeLong(cid.id());
        return;
      }
    }

    // Compute covering if requested
    List<S2CellId> cover = new ArrayList<>();
    if (opts.isIncludeCovering()) {
      getCellUnionBound(cover);
      if (cover.size() > 255) {
        cover.clear();
        logger.warning("Covering size too large (> 255) — clear Covering");
      }
    }
    // Write tag and covering
    EncodeTag tag = new EncodeTag(opts);
    tag.setKind(GeographyKind.POINT);
    tag.setCoveringSize((byte) cover.size());
    tag.encode(out);
    for (S2CellId c2 : cover) {
      out.writeLong(c2.id());
    }

    // Encode point payload using selected hint
    S2Point.Shape shp = S2Point.Shape.fromList(points);
    if (opts.getCodingHint() == EncodeOptions.CodingHint.FAST) {
      S2Point.Shape.FAST_CODER.encode(shp, out);
    } else {
      S2Point.Shape.COMPACT_CODER.encode(shp, out);
    }
  }

  public static PointGeography decode(DataInputStream in) throws IOException {
    EncodeTag tag = new EncodeTag();
    tag = EncodeTag.decode(in);
    int coverSize = tag.getCoveringSize() & 0xFF;

    PointGeography geo = new PointGeography();
    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty PointGeography.");
      return geo;
    }

    // Optimized 1-point COMPACT situation
    if (tag.getKind() == GeographyKind.CELL_CENTER && tag.getEncodeType() == 2) {
      long id = in.readLong();
      geo = new PointGeography(new S2CellId(id).toPoint());
      logger.fine("Decoded compact single-point geography via cell center.");
      return geo;
    }

    // skip cover
    for (int i = 0; i < coverSize; i++) {
      in.readLong();
    }

    // Read remaining bytes into buffer
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int len;
    while ((len = in.read(buffer)) != -1) {
      baos.write(buffer, 0, len);
    }

    PrimitiveArrays.Bytes bytes = Bytes.fromByteArray(baos.toByteArray());
    Cursor cursor = bytes.cursor();
    List<S2Point> points = new ArrayList<>();
    if (tag.getEncodeType() == 1) points = S2PointVectorCoder.FAST.decode(bytes, cursor);
    else if (tag.getEncodeType() == 2) points = S2PointVectorCoder.COMPACT.decode(bytes, cursor);
    geo.points.addAll(points);
    return geo;
  }
}
