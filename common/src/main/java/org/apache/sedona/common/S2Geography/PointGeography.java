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
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.*;
import com.google.common.geometry.PrimitiveArrays.Bytes;
import java.io.*;
import java.util.*;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PointGeography extends Geography {
  private static final Logger logger = LoggerFactory.getLogger(PointGeography.class.getName());

  private static final int BUFFER_SIZE = 4 * 1024;

  public final List<S2Point> points = new ArrayList<>();

  /** Constructs an empty PointGeography. */
  public PointGeography() {
    super(GeographyKind.POINT);
  }

  /** Constructs especially for CELL_CENTER */
  PointGeography(GeographyKind kind, S2Point point) {
    super(kind); // can be POINT or CELL_CENTER
    if (kind != GeographyKind.POINT
        && kind != GeographyKind.SINGLEPOINT
        && kind != GeographyKind.CELL_CENTER) {
      throw new IllegalArgumentException("Invalid GeographyKind for PointGeography: " + kind);
    }
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
    return points.size();
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
        EncodeTag tag = new EncodeTag();
        tag.setKind(GeographyKind.CELL_CENTER);
        tag.setCompact(true);
        tag.setCoveringSize((byte) 1);
        tag.encode(out);
        out.writeLong(cid.id());
        out.writeInt(getSRID()); // write the SRID
        out.flush();
        return;
      }
    }
    // In other cases, fallback to the default encodeTagged implementation:
    super.encodeTagged(out, opts);
  }

  @Override
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    // now the *payload* must go into its own buffer:
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output tmpOut = new Output(baos);

    // Encode point payload using selected hint
    S2Point.Shape shp = S2Point.Shape.fromList(points);
    switch (opts.getCodingHint()) {
      case FAST:
        S2Point.Shape.FAST_CODER.encode(shp, tmpOut);
        break;
      case COMPACT:
        S2Point.Shape.COMPACT_CODER.encode(shp, tmpOut);
    }
    tmpOut.flush();

    // grab exactly those bytes:
    byte[] payload = baos.toByteArray();

    // 4) length-prefix + payload
    // use writeInt(len, false) so it's exactly 4 bytes
    out.writeInt(payload.length, /* optimizePositive= */ false);
    out.writeBytes(payload);
    out.flush();
  }

  /** This is what decodeTagged() actually calls */
  public static PointGeography decode(Input in, EncodeTag tag) throws IOException {
    // cast to UnsafeInput—will work if you always pass a Kryo-backed stream
    if (!(in instanceof UnsafeInput)) {
      throw new IllegalArgumentException("Expected UnsafeInput");
    }
    return decode((UnsafeInput) in, tag);
  }

  public static PointGeography decode(UnsafeInput in, EncodeTag tag) throws IOException {
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

    // The S2 Coder interface of Java makes it hard to decode data using streams,
    // we can write an integer indicating the total length of the encoded point before the actual
    // payload in encode.
    // We can read the length and read the entire payload into a byte array, then call the decode
    // function of S2 Coder.
    // TODO: This results in in-compatible encoding format with the C++ implementation,
    // but we can do this for now until we need to exchange data with some native components.
    // 1) read our 4-byte length prefix
    int length = in.readInt(/* optimizePositive= */ false);
    if (length < 0) {
      throw new IOException("Invalid payload length: " + length);
    }

    // 2) read exactly that many bytes
    byte[] payload = new byte[length];
    in.readBytes(payload, 0, length);

    // 3) hand *only* those bytes to S2‐Coder via Bytes adapter
    Bytes bytes =
        new Bytes() {
          @Override
          public long length() {
            return payload.length;
          }

          @Override
          public byte get(long i) {
            return payload[(int) i];
          }
        };
    PrimitiveArrays.Cursor cursor = bytes.cursor();

    // pick the right decoder
    List<S2Point> pts;
    if (tag.isCompact()) {
      pts = S2Point.Shape.COMPACT_CODER.decode(bytes, cursor);
    } else {
      pts = S2Point.Shape.FAST_CODER.decode(bytes, cursor);
    }

    if (tag.getKind() == GeographyKind.SINGLEPOINT) {
      return new SinglePointGeography(pts.get(0));
    }

    geo.points.addAll(pts);
    return geo;
  }

  public CoordinateSequence getCoordinateSequence() {
    List<S2Point> pts = getPoints();
    Coordinate[] coordArray = new Coordinate[pts.size()];
    for (int i = 0; i < pts.size(); i++) {
      S2Point pt = pts.get(i);
      S2LatLng ll = new S2LatLng(pt);
      double lat = ll.latDegrees();
      double lon = ll.lngDegrees();
      Coordinate c = new Coordinate(lon, lat);
      coordArray[i] = c;
    }
    return new CoordinateArraySequence(coordArray);
  }
}
