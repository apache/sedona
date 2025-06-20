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

import com.google.common.collect.ImmutableList;
import com.google.common.geometry.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PolygonGeography extends S2Geography {
  private final List<S2Polygon> polygons;

  public PolygonGeography() {
    super(GeographyKind.POLYGON);
    this.polygons = new ArrayList<>();
  }

  public PolygonGeography(S2Polygon polygon) {
    super(GeographyKind.POLYGON);
    this.polygons = new ArrayList<>();
    this.polygons.add(polygon);
  }

  public PolygonGeography(List<S2Polygon> polygons) {
    super(GeographyKind.POLYGON);
    this.polygons = new ArrayList<>(polygons);
  }

  @Override
  public int dimension() {
    return 2;
  }

  @Override
  public int numShapes() {
    return polygons.size();
  }

  @Override
  public S2Shape shape(int id) {
    return polygons.get(id).shape();
  }

  @Override
  public S2Region region() {
    Collection<S2Region> regionCollection = new ArrayList<>(polygons);
    return new S2RegionUnion(regionCollection);
  }

  @Override
  public void getCellUnionBound(List<S2CellId> cellIds) {
    super.getCellUnionBound(cellIds);
  }

  public List<S2Polygon> getPolygons() {
    return ImmutableList.copyOf(polygons);
  }

  @Override
  public void encodeTagged(OutputStream os, EncodeOptions opts) throws IOException {
    DataOutputStream leOut = new DataOutputStream(os);
    // 1) Write tag header
    EncodeTag tag = new EncodeTag();
    tag.setKind(GeographyKind.POLYGON);
    tag.encode(leOut);

    // 2) Write covering cells if requested
    if (opts.isIncludeCovering()) {
      List<S2CellId> cover = new ArrayList<>();
      getCellUnionBound(cover);
      for (S2CellId cid : cover) {
        leOut.writeLong(cid.id());
      }
    }

    // 3) Write number of polygons
    leOut.writeInt(polygons.size());

    // 4) Encode each polygon
    for (S2Polygon poly : polygons) {
      poly.encode(leOut);
    }
    leOut.flush();
  }

  public static PolygonGeography decodeTagged(DataInputStream in, EncodeTag tag) throws IOException {
    PolygonGeography geo = new PolygonGeography();

    // EMPTY?
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      return geo;
    }

    // Skip any covering cells
    tag.skipCovering(in);

    // Read polygon count
    if (in.available() < Integer.BYTES) {
      throw new IOException("PolygonGeography.decodeTagged error: insufficient header bytes");
    }
    int count = in.readInt();

    // Decode each polygon
    for (int i = 0; i < count; i++) {
      S2Polygon poly = S2Polygon.decode(in);
      geo.polygons.add(poly);
    }
    return geo;
  }
}
