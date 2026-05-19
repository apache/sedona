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

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2PointRegion;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2Shape;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;

/**
 * A Geography implementation that stores WKB bytes as the primary representation, with lazy-parsed
 * JTS Geometry and S2 Geography caches. This enables zero-parse construction from WKB and deferred
 * S2 parsing only when spherical operations are needed.
 *
 * <p>Key optimizations (per paleolimbot's review): - dimension() reads WKB type byte directly (no
 * S2 parse) - region()/getCellUnionBound() for points read coordinates from WKB (no S2 parse) -
 * shape()/numShapes() use WkbS2Shape for simple types (no S2Polygon construction) - ShapeIndex is
 * built from WkbS2Shape directly (skips Geography layer)
 */
public class WKBGeography extends Geography {

  /**
   * When true, fromWKB() eagerly builds the S2 Geography and ShapeIndex at construction time
   * instead of lazily on first access. This eliminates cold-path overhead for predicate-heavy
   * workloads (ST_Contains, ST_Intersects) at the cost of slower deserialization for metric-only
   * workloads. Set via spark.sedona.geography.eagerShapeIndex or setEagerShapeIndex(). Default
   * false.
   */
  private static volatile boolean eagerShapeIndex = false;

  public static void setEagerShapeIndex(boolean eager) {
    eagerShapeIndex = eager;
  }

  public static boolean isEagerShapeIndex() {
    return eagerShapeIndex;
  }

  private final byte[] wkbBytes;

  // Lazy caches — volatile for thread safety with double-checked locking
  private volatile Geometry jtsGeometry;
  private volatile Geography s2Geography;
  private volatile ShapeIndexGeography shapeIndexGeography;

  private WKBGeography(byte[] wkbBytes, int srid) {
    super(GeographyKind.UNINITIALIZED);
    this.wkbBytes = wkbBytes;
    setSRID(srid);
  }

  /**
   * Create a WKBGeography from raw WKB bytes. When eagerShapeIndex is false (default), this is
   * zero-parse — just wraps the byte array. When eager mode is enabled, this also builds the
   * ShapeIndex upfront.
   */
  public static WKBGeography fromWKB(byte[] wkb, int srid) {
    WKBGeography geog = new WKBGeography(wkb, srid);
    if (eagerShapeIndex) {
      geog.getShapeIndexGeography();
    }
    return geog;
  }

  /** Create a WKBGeography from a JTS Geometry by serializing it to WKB. */
  public static WKBGeography fromJTS(Geometry jts) {
    org.locationtech.jts.io.WKBWriter writer =
        new org.locationtech.jts.io.WKBWriter(
            2, org.locationtech.jts.io.ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(jts);
    WKBGeography geog = new WKBGeography(wkb, jts.getSRID());
    geog.jtsGeometry = jts;
    return geog;
  }

  /** Create a WKBGeography from an existing S2 Geography by converting it to WKB. */
  public static WKBGeography fromS2Geography(Geography s2geog) {
    WKBWriter writer =
        new WKBWriter(2, org.locationtech.jts.io.ByteOrderValues.LITTLE_ENDIAN, false);
    byte[] wkb = writer.write(s2geog);
    WKBGeography geog = new WKBGeography(wkb, s2geog.getSRID());
    geog.s2Geography = s2geog;
    return geog;
  }

  /** Returns the raw WKB bytes. Zero cost. */
  public byte[] getWKBBytes() {
    return wkbBytes;
  }

  /** Returns a JTS Geometry, lazily parsed from WKB on first access. */
  public Geometry getJTSGeometry() {
    Geometry result = jtsGeometry;
    if (result == null) {
      synchronized (this) {
        result = jtsGeometry;
        if (result == null) {
          try {
            org.locationtech.jts.io.WKBReader reader = new org.locationtech.jts.io.WKBReader();
            result = reader.read(wkbBytes);
            result.setSRID(getSRID());
          } catch (ParseException e) {
            throw new RuntimeException("Failed to parse WKB to JTS Geometry", e);
          }
          jtsGeometry = result;
        }
      }
    }
    return result;
  }

  /**
   * Returns a ShapeIndexGeography, lazily built on first access. For simple types (Point,
   * LineString, Polygon), builds the index directly from WkbS2Shape — no S2 Geography construction
   * needed.
   */
  public ShapeIndexGeography getShapeIndexGeography() {
    ShapeIndexGeography result = shapeIndexGeography;
    if (result == null) {
      synchronized (this) {
        result = shapeIndexGeography;
        if (result == null) {
          int type = wkbBaseType();
          if (type >= 1 && type <= 3) {
            // Point/LineString/Polygon: build ShapeIndex from WkbS2Shape
            // Avoids S2Loop/S2Polygon internal index builds
            result = new ShapeIndexGeography();
            result.shapeIndex.add(new WkbS2Shape(wkbBytes));
          } else {
            // Multi-types and collections fall back to full S2 parse
            result = new ShapeIndexGeography(getS2Geography());
          }
          shapeIndexGeography = result;
        }
      }
    }
    return result;
  }

  /** Returns an S2 Geography, lazily parsed from WKB on first access. */
  public Geography getS2Geography() {
    Geography result = s2Geography;
    if (result == null) {
      synchronized (this) {
        result = s2Geography;
        if (result == null) {
          try {
            WKBReader reader = new WKBReader();
            result = reader.read(wkbBytes);
            result.setSRID(getSRID());
          } catch (ParseException e) {
            throw new RuntimeException("Failed to parse WKB to S2 Geography", e);
          }
          s2Geography = result;
        }
      }
    }
    return result;
  }

  // ─── WKB-direct optimizations (no S2 parse needed) ─────────────────────

  /** EWKB SRID flag (PostGIS convention). */
  private static final int EWKB_SRID_FLAG = 0x20000000;
  /** EWKB Z flag (PostGIS convention). */
  private static final int EWKB_Z_FLAG = 0x80000000;
  /** EWKB M flag (PostGIS convention). */
  private static final int EWKB_M_FLAG = 0x40000000;

  /** Read the raw 32-bit WKB type word at offset 1 (after the byte-order byte). */
  private int wkbRawType() {
    boolean le = (wkbBytes[0] == 0x01);
    if (le) {
      return (wkbBytes[1] & 0xFF)
          | ((wkbBytes[2] & 0xFF) << 8)
          | ((wkbBytes[3] & 0xFF) << 16)
          | ((wkbBytes[4] & 0xFF) << 24);
    } else {
      return ((wkbBytes[1] & 0xFF) << 24)
          | ((wkbBytes[2] & 0xFF) << 16)
          | ((wkbBytes[3] & 0xFF) << 8)
          | (wkbBytes[4] & 0xFF);
    }
  }

  /**
   * Returns the base WKB geometry type (1-7) after stripping EWKB flags and ISO Z/M offsets. Uses
   * the same {@code (typeInt & 0xffff) % 1000} pattern as {@link WKBReader}.
   */
  private int wkbBaseType() {
    return (wkbRawType() & 0xffff) % 1000;
  }

  /** Returns true if this WKB has an embedded SRID (EWKB convention). */
  private boolean wkbHasSRID() {
    return (wkbRawType() & EWKB_SRID_FLAG) != 0;
  }

  /**
   * Byte offset where the geometry payload begins (after the 5-byte header, plus 4 bytes if EWKB
   * SRID is embedded).
   */
  private int wkbPayloadOffset() {
    return wkbHasSRID() ? 9 : 5;
  }

  /** Throws if the stored WKB uses Z or M dimensions (EWKB flag or ISO type {@code >= 1000}). */
  private void requireXYOnly() {
    int raw = wkbRawType();
    boolean ewkbZ = (raw & EWKB_Z_FLAG) != 0;
    boolean ewkbM = (raw & EWKB_M_FLAG) != 0;
    boolean isoZM = (raw & 0xffff) >= 1000;
    if (ewkbZ || ewkbM || isoZM) {
      throw new UnsupportedOperationException(
          "WKBGeography only supports 2D WKB; got Z/M type: 0x" + Integer.toHexString(raw));
    }
  }

  /** Returns true if this WKB represents a single Point (type 1). */
  public boolean isPoint() {
    return wkbBaseType() == 1;
  }

  /** Extract the S2Point from a Point WKB without full S2 parse. */
  public S2Point extractPoint() {
    requireXYOnly();
    boolean le = (wkbBytes[0] == 0x01);
    int coordOffset = wkbPayloadOffset();
    ByteBuffer bb =
        ByteBuffer.wrap(wkbBytes).order(le ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    double lon = bb.getDouble(coordOffset);
    double lat = bb.getDouble(coordOffset + 8);
    return S2LatLng.fromDegrees(lat, lon).toPoint();
  }

  // ─── Geography abstract method overrides ───────────────────────────────

  @Override
  public int dimension() {
    // Read directly from WKB type byte — no S2 parse
    int type = wkbBaseType();
    switch (type) {
      case 1:
      case 4:
        return 0; // Point, MultiPoint
      case 2:
      case 5:
        return 1; // LineString, MultiLineString
      case 3:
      case 6:
        return 2; // Polygon, MultiPolygon
      default:
        return -1; // GeometryCollection or unknown
    }
  }

  @Override
  public int numShapes() {
    int type = wkbBaseType();
    if (type >= 1 && type <= 3) return 1;
    return getS2Geography().numShapes();
  }

  @Override
  public S2Shape shape(int id) {
    int type = wkbBaseType();
    if (type >= 1 && type <= 3) {
      return new WkbS2Shape(wkbBytes);
    }
    return getS2Geography().shape(id);
  }

  @Override
  public S2Region region() {
    if (isPoint()) {
      return new S2PointRegion(extractPoint());
    }
    return getS2Geography().region();
  }

  @Override
  public void getCellUnionBound(List<S2CellId> cellIds) {
    if (isPoint()) {
      cellIds.add(S2CellId.fromPoint(extractPoint()));
      return;
    }
    getS2Geography().getCellUnionBound(cellIds);
  }

  @Override
  public String toString() {
    return getS2Geography().toString();
  }

  @Override
  public String toString(PrecisionModel precisionModel) {
    return getS2Geography().toString(precisionModel);
  }

  @Override
  public String toText(PrecisionModel precisionModel) {
    return getS2Geography().toText(precisionModel);
  }

  @Override
  public String toEWKT() {
    Geography s2 = getS2Geography();
    s2.setSRID(getSRID());
    return s2.toEWKT();
  }

  @Override
  public String toEWKT(PrecisionModel precisionModel) {
    Geography s2 = getS2Geography();
    s2.setSRID(getSRID());
    return s2.toEWKT(precisionModel);
  }

  @Override
  public void encode(com.esotericsoftware.kryo.io.UnsafeOutput out, EncodeOptions opts)
      throws java.io.IOException {
    getS2Geography().encode(out, opts);
  }
}
