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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PointGeography extends S2Geography {
  // Underlying list of points
  private final List<S2Point> points = new ArrayList<>();

  /** Constructs an empty PointGeography. */
  public PointGeography() {
    super(GeographyKind.POINT);
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
    // Points are 0-dimensional (or -1 if empty)
    return points.isEmpty() ? -1 : 0;
  }

  @Override
  public int numShapes() {
    // Represent all points as a single composite shape
    return points.isEmpty() ? 0 : 1;
  }

  @Override
  public S2Shape shape(int id) {
    if (numShapes() == 0) {
      throw new IllegalStateException("No shapes in empty PointGeography");
    }
    if (id != 0) {
      throw new IllegalArgumentException("Shape id out of bounds: " + id);
    }
    return new PointShape(points);
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
      S2RegionUnion union = new S2RegionUnion(pointRegionCollection);
      return union;
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
  public void encodeTagged(OutputStream os, EncodeOptions opts) throws IOException {
    DataOutputStream out = new DataOutputStream(os);

    // CELL_CENTER path
    if (points.size() == 1 && opts.getCodingHint() == EncodeOptions.CodingHint.COMPACT) {
      S2CellId cid = S2CellId.fromPoint(points.get(0));
      if (cid.level() >= 23) {
        out.writeByte(S2Geography.GeographyKind.CELL_CENTER.getKind());
        out.writeByte(0);
        out.writeByte(1);
        out.writeByte(0);
        out.writeLong(cid.id());
        return;
      }
    }

    // EMPTY path
    if (points.isEmpty()) {
      EncodeTag tag = new EncodeTag();
      tag.setKind(GeographyKind.POINT);
      tag.setFlags((byte) (tag.getFlags() | EncodeTag.FLAG_EMPTY));
      tag.setCoveringSize((byte) 0);
      tag.encode(out);
      return;
    }

    // header POINT
    List<S2CellId> cover = new ArrayList<>();
    EncodeTag tag = new EncodeTag();
    tag.setKind(GeographyKind.POINT);
    if (opts.isIncludeCovering()) getCellUnionBound(cover);
    tag.setCoveringSize((byte) cover.size());
    tag.encode(out);
    for (var c2 : cover) out.writeLong(c2.id());

    // payload
    S2Point.Shape shp = S2Point.Shape.fromList(points);
    if (opts.getCodingHint() == EncodeOptions.CodingHint.FAST) {
      CountingPointVectorCoder.INSTANCE.encode(shp, out);
    } else {
      PointShapeCoders.COMPACT.encode(shp, out);
    }
  }

  public static PointGeography decodeTagged(DataInputStream in, EncodeTag tag) throws IOException {
    PointGeography geo = new PointGeography();
    // EMPTY?
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      return geo;
    }

    // RAW-vector (FAST) decode: varint count + nPoints × (double x,y,z)
    // FULL-FAST: varint count + n×(x,y,z)
    geo.points.addAll(CountingPointVectorCoder.INSTANCE.decode(in));
    return geo;
  }
}
