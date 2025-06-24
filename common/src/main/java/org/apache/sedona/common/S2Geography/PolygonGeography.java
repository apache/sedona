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
import java.util.logging.Logger;

public class PolygonGeography extends S2Geography {
  private static final Logger logger = Logger.getLogger(PolygonGeography.class.getName());

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
    return polygons.isEmpty() ? -1 : 2;
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
  public void encode(OutputStream os, EncodeOptions opts) throws IOException {
    DataOutputStream out = new DataOutputStream(os);

    // 1) Serialize any covering cells (if you have them)
    List<S2CellId> cover = new ArrayList<>();
    if (opts.isIncludeCovering()) {
      getCellUnionBound(cover);
      if (cover.size() > 255) {
        cover.clear();
        logger.warning("Covering size too large (> 255) — clear Covering");
      }
    }

    // 2) Write tag header (unchanged) using leOut.writeByte(...)
    EncodeTag tag = new EncodeTag(opts);
    tag.setKind(GeographyKind.POLYGON);
    tag.setCoveringSize((byte) cover.size());
    tag.encode(out);
    for (S2CellId c2 : cover) {
      out.writeLong(c2.id());
    }

    // 3) Write number of polygons
    out.writeInt(polygons.size());

    // 4) Encode each polygon
    for (S2Polygon poly : polygons) {
      poly.encode(out);
    }
    out.flush();
  }

  public static PolygonGeography decode(DataInputStream in) throws IOException {
    PolygonGeography geo = new PolygonGeography();

    EncodeTag tag = new EncodeTag();
    tag = EncodeTag.decode(in);
    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty PolygonGeography.");
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
