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

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Shape;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * An S2Shape implementation that reads coordinates directly from WKB bytes, avoiding the
 * construction of S2Polygon/S2Polyline objects. This is the key optimization suggested by
 * paleolimbot: functions that need a ShapeIndex can build it from this lightweight shape without
 * materializing full S2 geography objects.
 *
 * <p>Supports Point (type 1), LineString (type 2), and Polygon (type 3). Multi-types and
 * collections should fall back to the full S2 Geography parse path.
 */
public class WkbS2Shape implements S2Shape {

  private final ByteBuffer buf;
  private final int wkbType;
  private final int dim; // S2 dimension: 0=point, 1=line, 2=polygon

  // Cached structural info (computed once from WKB header)
  private final int totalEdges;
  private final int[] chainStarts; // edge offset for each chain
  private final int[] chainLengths; // edge count for each chain
  private final int[] coordOffsets; // byte offset to first coordinate of each chain

  // For polygon containsOrigin — computed lazily
  private volatile Boolean containsOriginCached;

  public WkbS2Shape(byte[] wkb) {
    boolean le = (wkb[0] == 0x01);
    this.buf = ByteBuffer.wrap(wkb).order(le ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    this.wkbType = buf.getInt(1) & 0xFF; // mask off Z/M/SRID flags

    switch (wkbType) {
      case 1: // Point
        this.dim = 0;
        this.totalEdges = 1; // point = 1 degenerate edge
        this.chainStarts = new int[] {0};
        this.chainLengths = new int[] {1};
        this.coordOffsets = new int[] {5}; // after byte_order(1) + type(4)
        break;

      case 2: // LineString
        {
          this.dim = 1;
          int numCoords = buf.getInt(5);
          this.totalEdges = Math.max(0, numCoords - 1);
          this.chainStarts = new int[] {0};
          this.chainLengths = new int[] {totalEdges};
          this.coordOffsets = new int[] {9}; // after byte_order(1) + type(4) + numCoords(4)
          break;
        }

      case 3: // Polygon
        {
          this.dim = 2;
          int numRings = buf.getInt(5);
          this.chainStarts = new int[numRings];
          this.chainLengths = new int[numRings];
          this.coordOffsets = new int[numRings];

          int edgeCount = 0;
          int byteOffset = 9; // after byte_order(1) + type(4) + numRings(4)
          for (int r = 0; r < numRings; r++) {
            int ringCoords = buf.getInt(byteOffset);
            byteOffset += 4;
            // Polygon rings: last coord = first coord, so edges = coords - 1
            int ringEdges = Math.max(0, ringCoords - 1);
            chainStarts[r] = edgeCount;
            chainLengths[r] = ringEdges;
            coordOffsets[r] = byteOffset;
            edgeCount += ringEdges;
            byteOffset += ringCoords * 16; // 2 doubles per coord
          }
          this.totalEdges = edgeCount;
          break;
        }

      default:
        throw new IllegalArgumentException(
            "WkbS2Shape only supports Point(1), LineString(2), Polygon(3). Got type: " + wkbType);
    }
  }

  @Override
  public int numEdges() {
    return totalEdges;
  }

  @Override
  public void getEdge(int edgeId, MutableEdge result) {
    if (wkbType == 1) {
      // Point: degenerate edge with equal endpoints
      S2Point p = readS2Point(coordOffsets[0]);
      result.a = p;
      result.b = p;
      return;
    }

    // Find which chain this edge belongs to
    int chainId = 0;
    int offset = edgeId;
    for (int i = 0; i < chainStarts.length; i++) {
      if (edgeId < chainStarts[i] + chainLengths[i]) {
        chainId = i;
        offset = edgeId - chainStarts[i];
        break;
      }
    }

    int coordByteOffset = coordOffsets[chainId] + offset * 16;
    result.a = readS2Point(coordByteOffset);
    result.b = readS2Point(coordByteOffset + 16);
  }

  @Override
  public boolean hasInterior() {
    return dim == 2;
  }

  @Override
  public boolean containsOrigin() {
    if (dim != 2) return false;
    Boolean cached = containsOriginCached;
    if (cached != null) return cached;
    synchronized (this) {
      cached = containsOriginCached;
      if (cached != null) return cached;
      // Build S2Loop from first ring to check containsOrigin
      cached = computeContainsOrigin();
      containsOriginCached = cached;
      return cached;
    }
  }

  @Override
  public int numChains() {
    return chainStarts.length;
  }

  @Override
  public int getChainStart(int chainId) {
    return chainStarts[chainId];
  }

  @Override
  public int getChainLength(int chainId) {
    return chainLengths[chainId];
  }

  @Override
  public void getChainEdge(int chainId, int offset, MutableEdge result) {
    if (wkbType == 1) {
      S2Point p = readS2Point(coordOffsets[0]);
      result.a = p;
      result.b = p;
      return;
    }
    int coordByteOffset = coordOffsets[chainId] + offset * 16;
    result.a = readS2Point(coordByteOffset);
    result.b = readS2Point(coordByteOffset + 16);
  }

  @Override
  public void getChainPosition(int edgeId, ChainPosition result) {
    for (int i = 0; i < chainStarts.length; i++) {
      if (edgeId < chainStarts[i] + chainLengths[i]) {
        result.set(i, edgeId - chainStarts[i]);
        return;
      }
    }
  }

  @Override
  public S2Point getChainVertex(int chainId, int edgeOffset) {
    return readS2Point(coordOffsets[chainId] + edgeOffset * 16);
  }

  @Override
  public int dimension() {
    return dim;
  }

  /**
   * Read an S2Point from WKB bytes at the given byte offset. Coordinates are (lon, lat) doubles.
   */
  private S2Point readS2Point(int byteOffset) {
    double lon = buf.getDouble(byteOffset);
    double lat = buf.getDouble(byteOffset + 8);
    return S2LatLng.fromDegrees(lat, lon).toPoint();
  }

  /** Compute containsOrigin for polygon by building S2Loop from first ring. */
  private boolean computeContainsOrigin() {
    // Read first ring coordinates
    int ringCoords = chainLengths[0] + 1; // edges + 1 = coords (including closing point)
    List<S2Point> vertices = new ArrayList<>(chainLengths[0]);
    for (int i = 0; i < chainLengths[0]; i++) {
      vertices.add(readS2Point(coordOffsets[0] + i * 16));
    }
    S2Loop loop = new S2Loop(vertices);
    loop.normalize();
    return loop.containsOrigin();
  }
}
