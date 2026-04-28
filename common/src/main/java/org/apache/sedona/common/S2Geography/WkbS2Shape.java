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

import com.google.common.geometry.S2;
import com.google.common.geometry.S2EdgeUtil;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Predicates;
import com.google.common.geometry.S2Shape;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An S2Shape implementation that reads WKB bytes once, converts all coordinates to S2Points in the
 * constructor, and stores them in an array. This avoids constructing S2Loop/S2Polygon objects
 * (which each build their own internal S2ShapeIndex), while also avoiding repeated trig calls on
 * every getEdge() access.
 *
 * <p>Supports Point (type 1), LineString (type 2), and Polygon (type 3). Multi-types and
 * collections should fall back to the full S2 Geography parse path.
 */
public class WkbS2Shape implements S2Shape {

  private static final int EWKB_SRID_FLAG = 0x20000000;
  private static final int EWKB_Z_FLAG = 0x80000000;
  private static final int EWKB_M_FLAG = 0x40000000;

  private final int dim; // S2 dimension: 0=point, 1=line, 2=polygon
  private final S2Point[] vertices; // all vertices, pre-converted from WKB
  private final int totalEdges;
  private final int[] chainStarts; // edge offset for each chain
  private final int[] chainLengths; // edge count for each chain
  private final int[] vertexOffsets; // index into vertices[] for first vertex of each chain

  // For polygon containsOrigin — computed eagerly at construction for polygons
  private final boolean containsOriginValue;

  public WkbS2Shape(byte[] wkb) {
    boolean le = (wkb[0] == 0x01);
    ByteBuffer buf =
        ByteBuffer.wrap(wkb).order(le ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    int typeInt = buf.getInt(1);
    int wkbType = (typeInt & 0xffff) % 1000;
    if ((typeInt & EWKB_Z_FLAG) != 0
        || (typeInt & EWKB_M_FLAG) != 0
        || (typeInt & 0xffff) >= 1000) {
      throw new UnsupportedOperationException(
          "WkbS2Shape only supports 2D WKB; got Z/M type: 0x" + Integer.toHexString(typeInt));
    }
    // Payload begins after the 5-byte header (byte-order + type). EWKB with SRID inserts a 4-byte
    // SRID immediately after the type, so coordinates/counts start at offset 9 in that case.
    int payloadOffset = ((typeInt & EWKB_SRID_FLAG) != 0) ? 9 : 5;

    switch (wkbType) {
      case 1: // Point
        {
          this.dim = 0;
          double lon = buf.getDouble(payloadOffset);
          double lat = buf.getDouble(payloadOffset + 8);
          S2Point p = S2LatLng.fromDegrees(lat, lon).toPoint();
          this.vertices = new S2Point[] {p};
          this.totalEdges = 1;
          this.chainStarts = new int[] {0};
          this.chainLengths = new int[] {1};
          this.vertexOffsets = new int[] {0};
          this.containsOriginValue = false;
          break;
        }

      case 2: // LineString
        {
          this.dim = 1;
          int numCoords = buf.getInt(payloadOffset);
          this.vertices = readVertices(buf, payloadOffset + 4, numCoords);
          this.totalEdges = Math.max(0, numCoords - 1);
          this.chainStarts = new int[] {0};
          this.chainLengths = new int[] {totalEdges};
          this.vertexOffsets = new int[] {0};
          this.containsOriginValue = false;
          break;
        }

      case 3: // Polygon
        {
          this.dim = 2;
          int numRings = buf.getInt(payloadOffset);
          this.chainStarts = new int[numRings];
          this.chainLengths = new int[numRings];
          this.vertexOffsets = new int[numRings];

          // First pass: count total vertices and compute offsets. Sedona's WKBWriter writes
          // open rings (n unique vertices, no closing duplicate); standard WKB writes closed
          // rings (n+1 coords with last == first). Detect the closing-duplicate case by
          // comparing the first and last (lon, lat) pair so we get the right edge count
          // either way: edges = uniqueVertices = closed ? ringCoords - 1 : ringCoords.
          int totalVerts = 0;
          int edgeCount = 0;
          int byteOffset = payloadOffset + 4;
          int[] ringCoordCounts = new int[numRings];
          int[] ringByteOffsets = new int[numRings];
          boolean[] ringClosed = new boolean[numRings];
          for (int r = 0; r < numRings; r++) {
            int ringCoords = buf.getInt(byteOffset);
            ringCoordCounts[r] = ringCoords;
            ringByteOffsets[r] = byteOffset + 4;
            boolean closed =
                ringCoords >= 2 && firstAndLastEqual(buf, ringByteOffsets[r], ringCoords);
            ringClosed[r] = closed;
            byteOffset += 4 + ringCoords * 16;

            int ringEdges = closed ? Math.max(0, ringCoords - 1) : ringCoords;
            int storedVerts = closed ? ringCoords : ringCoords;
            chainStarts[r] = edgeCount;
            chainLengths[r] = ringEdges;
            vertexOffsets[r] = totalVerts;
            edgeCount += ringEdges;
            totalVerts += storedVerts + (closed ? 0 : 1); // append closing duplicate for open rings
          }
          this.totalEdges = edgeCount;

          // Second pass: read all vertices, appending a closing duplicate for open rings so
          // the rest of the shape interface (getEdge, getChainEdge, computeContainsOrigin)
          // can index `vertexOffsets[r] + (i % chainLengths[r])` uniformly.
          this.vertices = new S2Point[totalVerts];
          int vi = 0;
          for (int r = 0; r < numRings; r++) {
            S2Point[] ringVerts = readVertices(buf, ringByteOffsets[r], ringCoordCounts[r]);
            System.arraycopy(ringVerts, 0, vertices, vi, ringVerts.length);
            vi += ringVerts.length;
            if (!ringClosed[r] && ringVerts.length > 0) {
              vertices[vi++] = ringVerts[0];
            }
          }

          // Eagerly compute containsOrigin from first ring
          this.containsOriginValue = computeContainsOrigin();
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
    if (dim == 0) {
      // Point: degenerate edge
      result.a = vertices[0];
      result.b = vertices[0];
      return;
    }
    // Find chain
    int chainId = findChain(edgeId);
    int offset = edgeId - chainStarts[chainId];
    int vi = vertexOffsets[chainId] + offset;
    result.a = vertices[vi];
    result.b = vertices[vi + 1];
  }

  @Override
  public boolean hasInterior() {
    return dim == 2;
  }

  @Override
  public boolean containsOrigin() {
    return containsOriginValue;
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
    if (dim == 0) {
      result.a = vertices[0];
      result.b = vertices[0];
      return;
    }
    int vi = vertexOffsets[chainId] + offset;
    result.a = vertices[vi];
    result.b = vertices[vi + 1];
  }

  @Override
  public void getChainPosition(int edgeId, ChainPosition result) {
    int chainId = findChain(edgeId);
    result.set(chainId, edgeId - chainStarts[chainId]);
  }

  @Override
  public S2Point getChainVertex(int chainId, int edgeOffset) {
    return vertices[vertexOffsets[chainId] + edgeOffset];
  }

  @Override
  public int dimension() {
    return dim;
  }

  // ─── Internal helpers ──────────────────────────────────────────────────

  private int findChain(int edgeId) {
    for (int i = chainStarts.length - 1; i >= 0; i--) {
      if (edgeId >= chainStarts[i]) return i;
    }
    return 0;
  }

  /**
   * Returns true when the ring's first and last vertex compare equal as raw doubles, i.e. the ring
   * is closed in the standard WKB sense. Sedona's own WKBWriter produces open rings, so this cheap
   * numeric comparison on the in-buffer bytes lets us distinguish the two cases without running
   * through the S2Point conversion.
   */
  private static boolean firstAndLastEqual(ByteBuffer buf, int byteOffset, int numCoords) {
    int lastOffset = byteOffset + (numCoords - 1) * 16;
    return buf.getDouble(byteOffset) == buf.getDouble(lastOffset)
        && buf.getDouble(byteOffset + 8) == buf.getDouble(lastOffset + 8);
  }

  /** Read numCoords (lon, lat) doubles from WKB and convert to S2Points. */
  private static S2Point[] readVertices(ByteBuffer buf, int byteOffset, int numCoords) {
    S2Point[] pts = new S2Point[numCoords];
    for (int i = 0; i < numCoords; i++) {
      double lon = buf.getDouble(byteOffset);
      double lat = buf.getDouble(byteOffset + 8);
      pts[i] = S2LatLng.fromDegrees(lat, lon).toPoint();
      byteOffset += 16;
    }
    return pts;
  }

  /**
   * Compute containsOrigin for polygon outer ring using direct edge-crossing test against
   * S2.origin(). Same algorithm as S2Loop.initOriginAndBound() but without constructing an S2Loop
   * (which builds its own internal S2ShapeIndex).
   */
  private boolean computeContainsOrigin() {
    int start = vertexOffsets[0];
    int numVerts = chainLengths[0]; // edges = verts - 1 for closed ring, but we use edge count

    if (numVerts < 3) return false;

    // Same logic as S2Loop.initOriginAndBound():
    // 1. Guess originInside = false
    // 2. Check if vertex(1) is inside via angle test
    // 3. Check if contains(vertex(1)) matches — if not, flip originInside
    S2Point v0 = vertices[start];
    S2Point v1 = vertices[start + 1];
    S2Point v2 = vertices[start + 2];

    boolean v1Inside =
        !v0.equalsPoint(v1) && !v2.equalsPoint(v1) && S2Predicates.angleContainsVertex(v0, v1, v2);

    // Brute force contains(vertex(1)) with originInside = false
    boolean originInside = false;
    S2Point origin = S2.origin();
    S2EdgeUtil.EdgeCrosser crosser = new S2EdgeUtil.EdgeCrosser(origin, v1, v0);
    boolean inside = originInside;
    for (int i = 1; i <= numVerts; i++) {
      S2Point next = vertices[start + (i % numVerts)];
      inside ^= crosser.edgeOrVertexCrossing(next);
    }

    if (v1Inside != inside) {
      originInside = true;
    }
    return originInside;
  }
}
