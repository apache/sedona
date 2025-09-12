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
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.geometry.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/** A Geography representing zero or more polylines using S2Polyline. */
public class PolylineGeography extends Geography {
  private static final Logger logger = Logger.getLogger(PolylineGeography.class.getName());

  public final List<S2Polyline> polylines;

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

  public PolylineGeography(GeographyKind kind, S2Polyline polyline) {
    super(kind);
    if (kind != GeographyKind.POLYLINE && kind != GeographyKind.SINGLEPOLYLINE) {
      throw new IllegalArgumentException("Invalid GeographyKind for PolylineGeography: " + kind);
    }
    this.polylines = new ArrayList<>();
    this.polylines.add(polyline);
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
    return new S2RegionUnion(polylineRegionCollection);
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
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    // 1) Write number of polylines as a 4-byte Kryo int
    out.writeInt(polylines.size());

    // 2) Encode point payload using selected hint
    boolean useFast = opts.getCodingHint() == EncodeOptions.CodingHint.FAST;
    for (S2Polyline pl : polylines) {
      if (useFast) {
        S2Polyline.FAST_CODER.encode(pl, out);
      } else {
        S2Polyline.COMPACT_CODER.encode(pl, out);
      }
    }
    out.flush();
  }

  /** This is what decodeTagged() actually calls */
  public static PolylineGeography decode(Input in, EncodeTag tag) throws IOException {
    // cast to UnsafeInput—will work if you always pass a Kryo-backed stream
    if (!(in instanceof UnsafeInput)) {
      throw new IllegalArgumentException("Expected UnsafeInput");
    }
    return decode((UnsafeInput) in, tag);
  }

  public static PolylineGeography decode(UnsafeInput in, EncodeTag tag) throws IOException {
    // 1) Instantiate an empty geography
    PolylineGeography geo = new PolylineGeography();

    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty PointGeography.");
      return geo;
    }

    // 2) Skip past any covering cell-IDs written by encodeTagged
    tag.skipCovering(in);

    // 3) Read the number of polylines (4-byte)
    try {
      int count = in.readInt();
      if (count < 0) {
        throw new IOException("PolylineGeography.decodeTagged error: negative count: " + count);
      }
      if (tag.getKind() == GeographyKind.SINGLEPOLYLINE) {
        return new SinglePolylineGeography(S2Polyline.decode(in));
      }

      // 4) For each polyline, read its block and let S2Polyline.decode(InputStream) do the rest
      for (int i = 0; i < count; i++) {
        S2Polyline pl = S2Polyline.decode(in);
        geo.polylines.add(pl);
      }

    } catch (EOFException e) {
      throw new IOException(
          "PolylineGeography.decodeTagged error: insufficient data to decode all parts of the geography.",
          e);
    }
    return geo;
  }

  public CoordinateSequence getCoordinateSequence() {
    // 1) Count total vertices across all polylines
    int totalVerts = 0;
    for (S2Polyline pl : polylines) {
      totalVerts += pl.numVertices();
    }

    // 2) Build a flat array of JTS Coordinates
    Coordinate[] coords = new Coordinate[totalVerts];
    int idx = 0;
    for (S2Polyline pl : polylines) {
      int n = pl.numVertices();
      for (int i = 0; i < n; i++, idx++) {
        S2Point pt = pl.vertex(i);
        S2LatLng ll = new S2LatLng(pt);
        double lat = ll.latDegrees();
        double lon = ll.lngDegrees();
        Coordinate c = new Coordinate(lon, lat);
        coords[idx] = c;
      }
    }

    // 3) Wrap in a CoordinateArraySequence
    return new CoordinateArraySequence(coords);
  }
}
