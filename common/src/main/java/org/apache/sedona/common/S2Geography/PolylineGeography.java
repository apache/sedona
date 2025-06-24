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
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

/** A Geography representing zero or more polylines using S2Polyline. */
public class PolylineGeography extends S2Geography {
  private static final Logger logger = Logger.getLogger(PolylineGeography.class.getName());

  private final List<S2Polyline> polylines;

  private static int sizeofInt() {
    return Integer.BYTES;
  }

  public PolylineGeography() {
    super(GeographyKind.POLYLINE);
    this.polylines = new ArrayList<>();
  }

  public PolylineGeography(S2Polyline polyline) {
    super(GeographyKind.POLYLINE);
    this.polylines = new ArrayList<>();
    this.polylines.add(polyline);
  }

  public PolylineGeography(List<S2Polyline> polylines) {
    super(GeographyKind.POLYLINE);
    this.polylines = new ArrayList<>(polylines);
  }

  @Override
  public int dimension() {
    return polylines.isEmpty() ? -1 : 1;
  }

  @Override
  public int numShapes() {
    return polylines.size();
  }

  @Override
  public S2Shape shape(int id) {
    return polylines.get(id);
  }

  @Override
  public S2Region region() {
    Collection<S2Region> polylineRegionCollection = new ArrayList<>();
    polylineRegionCollection.addAll(polylines);
    S2RegionUnion union = new S2RegionUnion(polylineRegionCollection);
    return union;
  }

  @Override
  public void getCellUnionBound(List<S2CellId> cellIds) {
    // Fallback to default Geography logic via shape index region
    super.getCellUnionBound(cellIds);
  }

  public List<S2Polyline> getPolylines() {
    return ImmutableList.copyOf(polylines);
  }

  @Override
  public void encode(OutputStream os, EncodeOptions opts) throws IOException {
    // Wrap your stream in a little-endian DataOutput
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
    tag.setKind(GeographyKind.POLYLINE);
    tag.setCoveringSize((byte) cover.size());
    tag.encode(out);
    for (S2CellId c2 : cover) {
      out.writeLong(c2.id());
    }

    // 3) **Critical**: write the number of polylines in little-endian
    out.writeInt(polylines.size());

    // Encode point payload using selected hint
    // 4) Delegate each polyline’s payload (which itself writes little-endian)
    for (S2Polyline pl : polylines) {
      pl.encode(out);
    }
    out.flush();
  }

  public static PolylineGeography decode(DataInputStream in) throws IOException {
    // 1) Instantiate an empty geography
    PolylineGeography geo = new PolylineGeography();

    EncodeTag tag = new EncodeTag();
    tag = EncodeTag.decode(in);
    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty PointGeography.");
      return geo;
    }

    // 3) Skip past any covering cell-IDs written by encodeTagged
    tag.skipCovering(in);

    // 4) Ensure we have at least 4 bytes for the count
    if (in.available() < Integer.BYTES) {
      throw new IOException("PolylineGeography.decodeTagged error: insufficient header bytes");
    }

    // 5) Read the number of polylines (4-byte little-endian int)
    int count = in.readInt();

    // 6) Loop and decode each polyline
    for (int i = 0; i < count; i++) {
      // This will read the version byte, then dispatch to decodeLossless or decodeCompressed
      S2Polyline pl = S2Polyline.decode(in);
      geo.polylines.add(pl);
    }

    return geo;
  }
}
