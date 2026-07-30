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
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Predicates;
import com.google.common.geometry.S2Shape;
import com.google.common.geometry.S2ShapeMeasures;
import com.google.common.geometry.S2ShapeUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

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
  // Polygon rings are normalized virtually: source WKB and vertices[] remain unchanged.
  private final boolean[] chainReversed;

  // For polygon containsOrigin — computed eagerly at construction for polygons
  private final boolean containsOriginValue;

  public WkbS2Shape(byte[] wkb) {
    this(wkb, true);
  }

  /**
   * Builds a shape whose polygon rings retain their WKB traversal direction.
   *
   * <p>This is an internal escape hatch for callers that intentionally encode the spherical
   * interior through ring direction, such as raster footprints that cover more than a hemisphere.
   * General Geography construction must use {@link #WkbS2Shape(byte[])}, which applies
   * simple-features shell/hole semantics independent of input winding.
   */
  public static WkbS2Shape withPreservedLoopOrientation(byte[] wkb) {
    return new WkbS2Shape(wkb, false);
  }

  private WkbS2Shape(byte[] wkb, boolean normalizePolygonRings) {
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
          this.chainReversed = new boolean[] {false};
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
          this.chainReversed = new boolean[] {false};
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
          this.chainReversed = new boolean[numRings];

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

          if (normalizePolygonRings) {
            // Match SedonaDB's simple-features interpretation: the first ring is a shell and every
            // subsequent ring is a hole regardless of input winding. Reverse only the S2-facing
            // traversal, leaving the stored WKB and vertex order untouched.
            for (int r = 0; r < numRings; r++) {
              boolean isHole = r > 0;
              boolean isClockwise = isClockwise(r);
              chainReversed[r] = isHole != isClockwise;
            }
          }

          // Compute reference containment after any virtual ring reversal has been applied.
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
    getChainEdge(chainId, offset, result);
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
    int vi = vertexOffsets[chainId];
    if (chainReversed[chainId]) {
      result.a = vertices[vi + chainLengths[chainId] - offset];
      result.b = vertices[vi + chainLengths[chainId] - offset - 1];
    } else {
      result.a = vertices[vi + offset];
      result.b = vertices[vi + offset + 1];
    }
  }

  @Override
  public void getChainPosition(int edgeId, ChainPosition result) {
    int chainId = findChain(edgeId);
    result.set(chainId, edgeId - chainStarts[chainId]);
  }

  @Override
  public S2Point getChainVertex(int chainId, int edgeOffset) {
    if (dim == 0) {
      return vertices[0];
    }
    int offset = chainReversed[chainId] ? chainLengths[chainId] - edgeOffset : edgeOffset;
    return vertices[vertexOffsets[chainId] + offset];
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
   * Returns whether a polygon chain has negative S2 curvature (clockwise traversal). The fast path
   * uses S2's robust turning-angle implementation through {@link S2ShapeMeasures}. Only a result
   * within S2's documented turning-angle error bound of a half-sphere allocates an S2Loop to apply
   * S2's hemisphere normalization convention.
   */
  private boolean isClockwise(int chainId) {
    if (chainLengths[chainId] < 3) {
      return false;
    }

    // approxLoopArea() calls back into getChainVertex(). This method must run before assigning this
    // chain's chainReversed entry so it measures the original WKB traversal.
    assert !chainReversed[chainId] : "ring orientation must be measured before virtual reversal";

    double halfSphere = 2.0 * Math.PI;
    double loopArea = S2ShapeMeasures.approxLoopArea(this, chainId);
    double maxError = S2.getTurningAngleMaxError(chainLengths[chainId]);
    if (Math.abs(loopArea - halfSphere) > maxError) {
      return loopArea > halfSphere;
    }

    int length = chainLengths[chainId];
    List<S2Point> ring = new ArrayList<>(length);
    int start = vertexOffsets[chainId];
    for (int i = 0; i < length; i++) {
      ring.add(vertices[start + i]);
    }
    return !new S2Loop(ring).isNormalized();
  }

  /**
   * Computes S2.origin() containment. A single-ring polygon uses the same one-pass initialization
   * as S2Loop; polygons with holes use a whole-shape reference point so every ring participates in
   * the result.
   */
  private boolean computeContainsOrigin() {
    if (numChains() == 1) {
      return computeSingleLoopContainsOrigin(0);
    }

    ReferencePoint reference = S2ShapeUtil.getReferencePoint(this);
    S2Point origin = S2.origin();
    if (reference.equalsPoint(origin)) {
      return reference.contained();
    }

    S2EdgeUtil.EdgeCrosser crosser = new S2EdgeUtil.EdgeCrosser(reference.point(), origin);
    boolean inside = reference.contained();
    MutableEdge edge = new MutableEdge();
    for (int i = 0; i < numEdges(); i++) {
      getEdge(i, edge);
      inside ^= crosser.edgeOrVertexCrossing(edge.a, edge.b);
    }
    return inside;
  }

  /**
   * Computes origin containment for one polygon chain using the initialization algorithm from
   * S2Loop, without constructing an S2Loop or first finding another reference point.
   */
  private boolean computeSingleLoopContainsOrigin(int chainId) {
    int numVertices = chainLengths[chainId];
    if (numVertices < 3) {
      return false;
    }

    S2Point v0 = getChainVertex(chainId, 0);
    S2Point v1 = getChainVertex(chainId, 1);
    S2Point v2 = getChainVertex(chainId, 2);
    boolean v1Inside =
        !v0.equalsPoint(v1) && !v2.equalsPoint(v1) && S2Predicates.angleContainsVertex(v0, v1, v2);

    S2EdgeUtil.EdgeCrosser crosser = new S2EdgeUtil.EdgeCrosser(S2.origin(), v1, v0);
    boolean inside = false;
    for (int i = 1; i <= numVertices; i++) {
      inside ^= crosser.edgeOrVertexCrossing(getChainVertex(chainId, i));
    }
    return v1Inside != inside;
  }
}
