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
package org.apache.sedona.common.utils;

import com.google.common.geometry.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.swing.*;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;

public class S2Utils {

  /**
   * @param coord Coordinate: convert a jts coordinate to a S2Point
   * @return
   */
  public static S2Point toS2Point(Coordinate coord) {
    return S2LatLng.fromDegrees(coord.y, coord.x).toPoint();
  }

  public static List<S2Point> toS2Points(Coordinate[] coords) {
    return Arrays.stream(coords).map(S2Utils::toS2Point).collect(Collectors.toList());
  }

  /**
   * @param line
   * @return
   */
  public static S2Polyline toS2PolyLine(LineString line) {
    return new S2Polyline(toS2Points(line.getCoordinates()));
  }

  public static S2Loop toS2Loop(LinearRing ring) {
    return new S2Loop(
        Orientation.isCCW(ring.getCoordinates())
            ? toS2Points(ring.getCoordinates())
            : toS2Points(ring.reverse().getCoordinates()));
  }

  public static S2Polygon toS2Polygon(Polygon polygon) {
    List<LinearRing> rings = new ArrayList<>();
    rings.add(polygon.getExteriorRing());
    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      rings.add(polygon.getInteriorRingN(i));
    }
    List<S2Loop> s2Loops = rings.stream().map(S2Utils::toS2Loop).collect(Collectors.toList());
    return new S2Polygon(s2Loops);
  }

  public static List<S2CellId> s2RegionToCellIDs(
      S2Region region, int minLevel, int maxLevel, int maxNum) {
    S2RegionCoverer.Builder coverBuilder = S2RegionCoverer.builder();
    coverBuilder.setMinLevel(minLevel);
    coverBuilder.setMaxLevel(maxLevel);
    coverBuilder.setMaxCells(maxNum);
    return coverBuilder.build().getCovering(region).cellIds();
  }

  public static S2CellId coordinateToCellID(Coordinate coordinate, int level) {
    return S2CellId.fromPoint(S2Utils.toS2Point(coordinate)).parent(level);
  }

  public static List<S2CellId> roundCellsToSameLevel(List<S2CellId> cellIDs, int level) {
    Set<Long> results = new HashSet<>();
    for (S2CellId cellID : cellIDs) {
      if (cellID.level() > level) {
        results.add(cellID.parent(level).id());
      } else if (cellID.level() < level) {
        for (S2CellId c = cellID.childBegin(level);
            !c.equals(cellID.childEnd(level));
            c = c.next()) {
          results.add(c.id());
        }
      } else {
        results.add(cellID.id());
      }
    }
    return results.stream().map(S2CellId::new).collect(Collectors.toList());
  }

  public static Polygon toJTSPolygon(S2CellId cellId) {
    S2LatLngRect bound = new S2Cell(cellId).getRectBound();
    Coordinate[] coords = new Coordinate[5];
    int[] iters = new int[] {0, 1, 2, 3, 0};
    for (int i = 0; i < 5; i++) {
      coords[i] =
          new Coordinate(
              bound.getVertex(iters[i]).lngDegrees(), bound.getVertex(iters[i]).latDegrees());
    }
    return new GeometryFactory().createPolygon(coords);
  }

  /**
   * Convert a JTS planar geometry into an S2Region whose lat/lng projection is guaranteed to
   * contain the input geometry.
   *
   * <p>Why a buffer is needed: Sedona geometries are planar — an edge between two vertices is a
   * straight line in (lng, lat) space — but S2 connects the same vertices with a great-circle arc
   * on the sphere. The two interpretations agree at the vertices but diverge along the edges (e.g.
   * the great-circle arc between two points at the same northern latitude bulges northward, leaving
   * the parallel that would form the planar chord). If we hand the JTS vertices to S2 directly, the
   * spherical polygon's interior is *smaller* than the planar polygon's interior along
   * non-meridional edges, so the S2 covering misses thin slivers of the original planar polygon
   * (see GH-2857).
   *
   * <p>To compensate, we JTS-buffer the input by an upper bound on the worst-case great-circle
   * deviation before converting to S2. A side effect for {@link LineString} inputs is that the
   * buffer turns the line into a polygon corridor; downstream callers therefore see cells in a thin
   * strip around the line rather than only cells the line geometrically traverses.
   */
  public static S2Region toS2Region(Geometry geom) throws IllegalArgumentException {
    if (!(geom instanceof Polygon) && !(geom instanceof LineString)) {
      throw new IllegalArgumentException(
          "only object of Polygon, LinearRing, LineString type can be converted to S2Region");
    }
    double eps = arcChordBufferDegrees(geom);
    Geometry buffered = (eps > 0) ? geom.buffer(eps) : geom;
    if (buffered instanceof Polygon) {
      return S2Utils.toS2Polygon((Polygon) buffered);
    } else if (buffered instanceof LineString) {
      // Only reachable when eps == 0 (e.g. a single-point degenerate line). Normal lines
      // become Polygon corridors after buffer and are handled above.
      return S2Utils.toS2PolyLine((LineString) buffered);
    } else if (buffered instanceof MultiPolygon && buffered.getNumGeometries() > 0) {
      // JTS buffer of self-touching geometries can collapse to MultiPolygon. We can only
      // hand a single S2Region back to callers, so cover the largest piece — the smaller
      // pieces are typically tiny artifacts of the buffer operation rather than real input.
      Polygon largest = (Polygon) buffered.getGeometryN(0);
      for (int i = 1; i < buffered.getNumGeometries(); i++) {
        Polygon p = (Polygon) buffered.getGeometryN(i);
        if (p.getArea() > largest.getArea()) {
          largest = p;
        }
      }
      return S2Utils.toS2Polygon(largest);
    }
    throw new IllegalArgumentException(
        "only object of Polygon, LinearRing, LineString type can be converted to S2Region");
  }

  /**
   * Compute the JTS buffer amount (in degrees) needed so that the spherical interpretation of the
   * buffered geometry fully contains the original planar geometry.
   *
   * <p>The buffer must be at least as large as the largest great-circle/chord deviation among the
   * edges that S2 will eventually see. Polygons and lines need different bounds because JTS buffer
   * affects their edges differently:
   *
   * <ul>
   *   <li><b>Polygon</b>: each existing edge is offset perpendicularly in place; corners get
   *       rounded into many short arcs, but no edge is dramatically lengthened. The buffered
   *       polygon's edges therefore have ~the same length as the originals, so the original
   *       polygon's per-edge deviation is a tight upper bound on what the buffered polygon's edges
   *       will exhibit. We use {@link #ringMaxDeviationDegrees}.
   *   <li><b>LineString</b>: buffering produces a corridor whose long top/bottom edges span the
   *       line's full envelope — far longer than any individual segment when consecutive segments
   *       are collinear (JTS often simplifies them away). Per-segment deviation severely
   *       under-bounds the corridor's actual edge deviation. We bound by virtual edges across the
   *       envelope via {@link #envelopeDeviationDegrees}.
   * </ul>
   *
   * <p>The 1.5× safety multiplier absorbs numerical error and the small additional deviation the
   * buffered polygon's own (slightly different) edges introduce on top of the bound.
   */
  private static double arcChordBufferDegrees(Geometry geom) {
    double maxDev = 0.0;
    if (geom instanceof Polygon) {
      Polygon poly = (Polygon) geom;
      maxDev = Math.max(maxDev, ringMaxDeviationDegrees(poly.getExteriorRing().getCoordinates()));
      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        maxDev =
            Math.max(maxDev, ringMaxDeviationDegrees(poly.getInteriorRingN(i).getCoordinates()));
      }
    } else if (geom instanceof LineString) {
      maxDev = envelopeDeviationDegrees(geom);
    }
    return maxDev * 1.5;
  }

  /**
   * Conservative deviation upper bound for a geometry, derived from its bounding envelope rather
   * than its actual edges.
   *
   * <p>Used for {@link LineString} inputs because, after JTS buffer, the corridor's long edges are
   * not the line's segments — they connect the line's extreme endpoints (or close to it). To bound
   * them we probe three virtual edges across the envelope:
   *
   * <ul>
   *   <li>The two diagonals (SW–NE and NW–SE) — diagonal great-circle arcs deviate more than
   *       east-west arcs of the same Δλ at high latitudes, and a buffered corridor's long edges can
   *       run in either direction depending on the line's orientation.
   *   <li>The worst-case east-west edge at whichever latitude (top or bottom of the envelope) has
   *       the larger absolute value — east-west arcs bulge poleward, so the deviation grows with
   *       |latitude|, and an envelope-spanning east-west arc is what a horizontal collinear line
   *       would buffer into.
   * </ul>
   *
   * <p>The max across these three bounds the deviation any corridor edge could plausibly exhibit.
   * This deliberately over-bounds zigzag lines whose actual corridor edges are short; the
   * alternative (per-segment analysis) silently fails on collinear inputs.
   */
  private static double envelopeDeviationDegrees(Geometry geom) {
    Envelope env = geom.getEnvelopeInternal();
    if (env.isNull()) {
      return 0.0;
    }
    Coordinate sw = new Coordinate(env.getMinX(), env.getMinY());
    Coordinate se = new Coordinate(env.getMaxX(), env.getMinY());
    Coordinate ne = new Coordinate(env.getMaxX(), env.getMaxY());
    Coordinate nw = new Coordinate(env.getMinX(), env.getMaxY());
    // For the east-west probe, pick whichever latitude band of the envelope is further from
    // the equator — that's where same-Δλ great-circle arcs deviate most from the chord.
    double signedLat =
        Math.abs(env.getMinY()) > Math.abs(env.getMaxY()) ? env.getMinY() : env.getMaxY();
    Coordinate ewWest = new Coordinate(env.getMinX(), signedLat);
    Coordinate ewEast = new Coordinate(env.getMaxX(), signedLat);
    double max = edgeDeviationDegrees(sw, ne);
    max = Math.max(max, edgeDeviationDegrees(nw, se));
    max = Math.max(max, edgeDeviationDegrees(ewWest, ewEast));
    return max;
  }

  /**
   * Per-edge deviation bound for a ring/path: walk consecutive vertex pairs and return the largest
   * single-edge great-circle/chord deviation. Used for polygon rings (exterior and interior), where
   * buffering preserves edge lengths and per-edge analysis is tight.
   */
  private static double ringMaxDeviationDegrees(Coordinate[] coords) {
    double maxDev = 0.0;
    for (int i = 0; i < coords.length - 1; i++) {
      double dev = edgeDeviationDegrees(coords[i], coords[i + 1]);
      if (dev > maxDev) {
        maxDev = dev;
      }
    }
    return maxDev;
  }

  /**
   * Primitive deviation for a single edge: the (lng, lat) distance between the planar chord
   * midpoint and the great-circle arc midpoint.
   *
   * <p>Why the midpoint: a great-circle arc between two points is symmetric about its midpoint, and
   * the (lng, lat) deviation from the chord is maximized there. So the midpoint deviation equals
   * the maximum deviation along the edge — there's no need to sample multiple points.
   *
   * <p>The great-circle midpoint is computed by averaging the two endpoint S2Points (unit vectors
   * on the sphere) and renormalizing — a standard spherical-midpoint trick. The chord midpoint is
   * the plain Euclidean mean of the (lng, lat) coordinates.
   */
  private static double edgeDeviationDegrees(Coordinate a, Coordinate b) {
    S2Point aPt = toS2Point(a);
    S2Point bPt = toS2Point(b);
    double mx = aPt.getX() + bPt.getX();
    double my = aPt.getY() + bPt.getY();
    double mz = aPt.getZ() + bPt.getZ();
    double norm = Math.sqrt(mx * mx + my * my + mz * mz);
    if (norm < 1e-15) {
      // Antipodal endpoints — the great circle through them is not unique, so there is no
      // well-defined midpoint to compare against. Returning 0 effectively skips this edge;
      // antipodal inputs aren't realistic for S2 covering anyway.
      return 0.0;
    }
    S2LatLng midSpherical = new S2LatLng(new S2Point(mx / norm, my / norm, mz / norm));
    double midSphericalLat = midSpherical.latDegrees();
    double midSphericalLng = midSpherical.lngDegrees();
    double midChordLat = (a.y + b.y) / 2.0;
    double midChordLng = (a.x + b.x) / 2.0;
    double dLat = midSphericalLat - midChordLat;
    double dLng = midSphericalLng - midChordLng;
    // Wrap longitude difference into [-180, 180]. Without this, an edge straddling the
    // antimeridian (e.g. -179° to +179°) would compute dLng ≈ 358° and produce a bogus
    // ~360° deviation rather than the small actual deviation.
    if (dLng > 180.0) dLng -= 360.0;
    else if (dLng < -180.0) dLng += 360.0;
    return Math.sqrt(dLat * dLat + dLng * dLng);
  }
}
