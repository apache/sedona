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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.algorithm.Distance;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.LineStringExtracter;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.linearref.LinearLocation;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;

/**
 * Computes the portions of two lineal geometries which follow the same or opposite direction.
 *
 * <p>For valid, non-null inputs, the result always has this shape:
 *
 * <pre>
 * GeometryCollection
 * +-- element 0: MultiLineString of same-direction paths
 * `-- element 1: MultiLineString of opposite-direction paths
 * </pre>
 *
 * Every returned path is oriented like the first input. For example:
 *
 * <pre>
 * left :  A --------------------&gt; B
 * right:  A --------------------&gt; B   =&gt; element 0 (same)
 * right:  A &lt;-------------------- B   =&gt; element 1 (opposite)
 * </pre>
 *
 * <p>The implementation follows these stages:
 *
 * <pre>
 * validate inputs
 *      |
 * Stage 1: robust 2D intersection and shared-line extraction
 *      |
 * Stage 2: build an index over each source geometry
 *      |
 * Stage 3: locate and compare each fragment in both sources
 *      |
 * Stage 4: orient like the first input and restore PostGIS-compatible Z values
 *      |
 * Stage 5: assemble the same-direction and opposite-direction buckets
 * </pre>
 *
 * The spatial indices are important: an overlay can emit one fragment per source segment, so
 * rescanning both inputs for every fragment would make identical long lines quadratic.
 */
public final class SharedPaths {
  // Interior samples avoid the endpoint ambiguity of closed lines and shared source vertices.
  // The same pair of fractions is used consistently when classifying traversal direction and
  // when disambiguating the source segment used for Z interpolation.
  private static final double START_SAMPLE_FRACTION = 0.1;
  private static final double END_SAMPLE_FRACTION = 0.9;

  private SharedPaths() {}

  /**
   * Returns a collection containing same-direction paths followed by opposite-direction paths.
   * Paths are oriented in the direction of {@code left}. Matching SRIDs are required. The result
   * retains Z when either input has Z and, like PostGIS, does not retain M.
   */
  public static Geometry compute(Geometry left, Geometry right) {
    // Match null-propagating SQL behavior before attempting type or SRID validation.
    if (left == null || right == null) {
      return null;
    }
    validateSRIDs(left, right);
    validateLineal(left);
    validateLineal(right);

    // The first input owns the result's precision model, coordinate sequence implementation, and
    // (already validated) SRID. This also keeps every nested result geometry on the same factory.
    GeometryFactory resultFactory = resultFactory(left);
    if (left.isEmpty() || right.isEmpty()) {
      return emptyResult(resultFactory);
    }

    // Stage 1: robust overlay finds the physical shared coverage. Point-only intersections are
    // intentionally ignored later because LineStringExtracter returns only linear components.
    Geometry intersection = OverlayNGRobust.overlay(left, right, OverlayNG.INTERSECTION);
    List<LineString> sameDirection = new ArrayList<>();
    List<LineString> oppositeDirection = new ArrayList<>();
    boolean outputHasZ = hasZ(left) || hasZ(right);

    @SuppressWarnings("unchecked")
    List<LineString> paths = LineStringExtracter.getLines(intersection);

    // Stage 2: build each source index once. Besides avoiding repeated scans, the index preserves
    // source traversal order so repeated or self-overlapping paths have deterministic semantics.
    SourceSegmentIndex leftSegments = !paths.isEmpty() ? new SourceSegmentIndex(left) : null;
    SourceSegmentIndex rightSegments = !paths.isEmpty() ? new SourceSegmentIndex(right) : null;
    for (LineString path : paths) {
      if (path.isEmpty()) {
        continue;
      }
      // Stage 3: determine how this overlay fragment is traversed in each original input.
      Direction direction = direction(path, leftSegments, rightSegments);

      // Stage 4: overlay output direction is not a contract. Normalize it to the direction of the
      // first input before reconstructing ordinates or adding the path to a result bucket.
      LineString orientedPath = direction.forwardOnLeft ? path : (LineString) path.reverse();
      LineString resultPath =
          copyWithPostGISOrdinates(
              orientedPath, resultFactory, outputHasZ, leftSegments, rightSegments);
      if (direction.same) {
        sameDirection.add(resultPath);
      } else {
        oppositeDirection.add(resultPath);
      }
    }

    // Stage 5: even when one bucket is empty, retain both typed MultiLineString children.
    MultiLineString same =
        resultFactory.createMultiLineString(sameDirection.toArray(new LineString[0]));
    MultiLineString opposite =
        resultFactory.createMultiLineString(oppositeDirection.toArray(new LineString[0]));
    return resultFactory.createGeometryCollection(new Geometry[] {same, opposite});
  }

  private static void validateLineal(Geometry geometry) {
    if (!(geometry instanceof LineString) && !(geometry instanceof MultiLineString)) {
      throw new IllegalArgumentException("Geometry is not lineal");
    }
  }

  private static void validateSRIDs(Geometry left, Geometry right) {
    if (left.getSRID() != right.getSRID()) {
      throw new IllegalArgumentException(
          String.format(
              "Operation on mixed SRID geometries (%d != %d)", left.getSRID(), right.getSRID()));
    }
  }

  private static GeometryFactory resultFactory(Geometry geometry) {
    return new GeometryFactory(
        new PrecisionModel(geometry.getPrecisionModel()),
        geometry.getSRID(),
        geometry.getFactory().getCoordinateSequenceFactory());
  }

  private static GeometryCollection emptyResult(GeometryFactory factory) {
    // PostGIS returns two empty MultiLineStrings rather than an empty GeometryCollection or null.
    MultiLineString same = factory.createMultiLineString();
    MultiLineString opposite = factory.createMultiLineString();
    return factory.createGeometryCollection(new Geometry[] {same, opposite});
  }

  private static Direction direction(
      LineString path, SourceSegmentIndex leftSegments, SourceSegmentIndex rightSegments) {
    CoordinateSequence sequence = path.getCoordinateSequence();

    // Each boolean says whether the overlay fragment's current coordinate order is forward in the
    // corresponding source. Equal booleans mean both sources traverse the fragment the same way.
    boolean forwardOnLeft = leftSegments.isForward(sequence);
    boolean forwardOnRight = rightSegments.isForward(sequence);
    return new Direction(forwardOnLeft, forwardOnLeft == forwardOnRight);
  }

  private static int firstNonZeroSegment(CoordinateSequence sequence) {
    // Overlay output should be non-degenerate, but repeated coordinates are legal in a line. Skip
    // them so the direction samples always span a segment with a meaningful orientation.
    for (int i = 0; i < sequence.size() - 1; i++) {
      if (sequence.getX(i) != sequence.getX(i + 1) || sequence.getY(i) != sequence.getY(i + 1)) {
        return i;
      }
    }
    throw new IllegalArgumentException("Shared path does not contain a non-zero segment");
  }

  private static LineString copyWithPostGISOrdinates(
      LineString path,
      GeometryFactory factory,
      boolean outputHasZ,
      SourceSegmentIndex leftSegments,
      SourceSegmentIndex rightSegments) {
    CoordinateSequence source = path.getCoordinateSequence();
    CoordinateSequenceFactory sequenceFactory = factory.getCoordinateSequenceFactory();

    // Shared-path topology is two-dimensional. Build a fresh XY or XYZ sequence so M is always
    // removed, while Z is retained when either source geometry declares a Z dimension.
    CoordinateSequence target = sequenceFactory.create(source.size(), outputHasZ ? 3 : 2, 0);

    for (int i = 0; i < source.size(); i++) {
      double x = source.getX(i);
      double y = source.getY(i);
      target.setOrdinate(i, CoordinateSequence.X, x);
      target.setOrdinate(i, CoordinateSequence.Y, y);
      if (outputHasZ) {
        // A self-crossing source can contain the same XY on several traversals with different Z
        // values. Select the segment adjacent to this particular output path coordinate rather
        // than accepting the first segment that happens to contain the XY point.
        SourceSegment leftSegment = leftSegments.findForPathCoordinate(source, i);
        SourceSegment rightSegment = rightSegments.findForPathCoordinate(source, i);
        target.setOrdinate(
            i,
            CoordinateSequence.Z,
            postGISZ(x, y, leftSegments, rightSegments, leftSegment, rightSegment));
      }
    }
    return factory.createLineString(target);
  }

  private static double postGISZ(
      double x,
      double y,
      SourceSegmentIndex leftSegments,
      SourceSegmentIndex rightSegments,
      SourceSegment leftSegment,
      SourceSegment rightSegment) {
    // Z precedence captures the stable PostGIS/GEOS behavior used by shared paths:
    //
    //   exact vertex in left
    //       -> exact vertex in right
    //       -> interpolation on the matching left traversal
    //       -> interpolation on the matching right traversal
    //
    // Exact vertices are indexed globally because a noded overlay coordinate can coincide with a
    // vertex on another traversal of the same non-simple input. Interpolation, by contrast, must
    // use the traversal containing the shared path or it can pick the wrong Z at a crossing.
    double exactZ = leftSegments.exactZ(x, y);
    if (Double.isFinite(exactZ)) {
      return exactZ;
    }
    exactZ = rightSegments.exactZ(x, y);
    if (Double.isFinite(exactZ)) {
      return exactZ;
    }
    double leftZ = leftSegment.zAt(x, y);
    if (Double.isFinite(leftZ)) {
      return leftZ;
    }
    return rightSegment.zAt(x, y);
  }

  private static boolean hasZ(Geometry geometry) {
    // LineString and MultiLineString are the only accepted inputs, so their component coordinate
    // sequences fully describe the dimensionality relevant to the output.
    for (int component = 0; component < geometry.getNumGeometries(); component++) {
      Geometry child = geometry.getGeometryN(component);
      if (child instanceof LineString && ((LineString) child).getCoordinateSequence().hasZ()) {
        return true;
      }
    }
    return false;
  }

  private static final class Direction {
    // Whether the overlay fragment's current order agrees with the first input. This controls
    // whether the fragment must be reversed before it is returned.
    private final boolean forwardOnLeft;

    // Whether both inputs traverse the physical path in the same direction. This selects the
    // first or second MultiLineString in the result GeometryCollection.
    private final boolean same;

    private Direction(boolean forwardOnLeft, boolean same) {
      this.forwardOnLeft = forwardOnLeft;
      this.same = same;
    }
  }

  /**
   * Indexes source locations for path direction and segment-aware Z interpolation.
   *
   * <p>Two complementary indices are built together during construction:
   *
   * <pre>
   * source vertices -----------------&gt; exactZByVertex (O(1) exact-Z lookup)
   * source segment envelopes --------&gt; STRtree (spatial candidate lookup)
   * source traversal order ----------&gt; segment ordinal (stable tie-breaking)
   * </pre>
   *
   * A point may belong to several segments at a self-intersection or repeated path. Candidate
   * selection therefore uses geometric distance first and source traversal order second.
   */
  private static final class SourceSegmentIndex {
    private final STRtree index = new STRtree();
    private final Map<XYKey, Double> exactZByVertex = new HashMap<>();

    // Overlay interpolation can move a theoretically-on-segment sample by a few ULPs. Track a
    // tolerance at the scale of the largest source coordinate so the STRtree query envelope still
    // reaches the true segment before the more precise distance check below.
    private double queryTolerance = 16 * Math.ulp(1.0);

    private SourceSegmentIndex(Geometry geometry) {
      // Ordinals flatten component/segment positions into source traversal order. Zero-length
      // segments are omitted because no interior sample can be located on them; omitting them does
      // not change the relative order of usable segments.
      int ordinal = 0;
      for (int component = 0; component < geometry.getNumGeometries(); component++) {
        LineString line = (LineString) geometry.getGeometryN(component);
        CoordinateSequence sequence = line.getCoordinateSequence();

        // Use a source-aware query expansion at large coordinate magnitudes while retaining a
        // small absolute floor near zero.
        double maxCoordinateMagnitude = 1.0;
        for (int vertex = 0; vertex < sequence.size(); vertex++) {
          maxCoordinateMagnitude =
              Math.max(
                  maxCoordinateMagnitude,
                  Math.max(Math.abs(sequence.getX(vertex)), Math.abs(sequence.getY(vertex))));
        }
        queryTolerance = Math.max(queryTolerance, 16 * Math.ulp(maxCoordinateMagnitude));
        if (sequence.hasZ()) {
          // putIfAbsent preserves the first finite Z encountered along the source traversal. This
          // makes an exact vertex deterministic when a non-simple line visits the same XY more
          // than once with different Z values.
          for (int vertex = 0; vertex < sequence.size(); vertex++) {
            double z = sequence.getZ(vertex);
            if (Double.isFinite(z)) {
              exactZByVertex.putIfAbsent(
                  new XYKey(sequence.getX(vertex), sequence.getY(vertex)), z);
            }
          }
        }
        for (int segment = 0; segment < sequence.size() - 1; segment++) {
          Coordinate start = sequence.getCoordinateCopy(segment);
          Coordinate end = sequence.getCoordinateCopy(segment + 1);
          if (start.equals2D(end)) {
            continue;
          }

          // The envelope is only a candidate filter. Collinearity and tolerance are verified by
          // SourceSegment.contains after querying the tree.
          SourceSegment sourceSegment = new SourceSegment(start, end, sequence.hasZ(), ordinal++);
          index.insert(new Envelope(start, end), sourceSegment);
        }
      }
      index.build();
    }

    private double exactZ(double x, double y) {
      // Exact means bitwise-equal XY after normalizing signed zero; interpolated coordinates fall
      // through to the traversal-aware segment logic.
      Double z = exactZByVertex.get(new XYKey(x, y));
      return z == null ? Double.NaN : z;
    }

    private boolean isForward(CoordinateSequence path) {
      int segmentIndex = firstNonZeroSegment(path);
      Coordinate start = path.getCoordinateCopy(segmentIndex);
      Coordinate end = path.getCoordinateCopy(segmentIndex + 1);

      // Sample inside the first non-zero output segment. Sampling away from endpoints avoids
      // choosing the preceding segment at a source vertex and avoids the coincident start/end of a
      // closed line. The samples are located independently because an overlay edge may span one or
      // more source vertices after collinear nodes are collapsed.
      Coordinate startSample =
          LinearLocation.pointAlongSegmentByFraction(start, end, START_SAMPLE_FRACTION);
      Coordinate endSample =
          LinearLocation.pointAlongSegmentByFraction(start, end, END_SAMPLE_FRACTION);
      return locate(startSample).compareTo(locate(endSample)) < 0;
    }

    private SourceLocation locate(Coordinate coordinate) {
      SourceLocation earliest = null;
      double closestDistance = Double.POSITIVE_INFINITY;
      for (SourceSegment candidate : candidates(coordinate)) {
        if (!candidate.contains(coordinate)) {
          continue;
        }
        double distance = candidate.distanceTo(coordinate);
        SourceLocation location =
            new SourceLocation(candidate.ordinal, candidate.fractionAlong(coordinate));
        int distanceComparison = Double.compare(distance, closestDistance);

        // Tolerance can admit a nearby parallel segment at large coordinate magnitudes. Prefer the
        // geometrically closest segment; only exact distance ties use the earliest traversal. At a
        // true crossing both distances are zero, so the tie-break matches first-occurrence linear
        // referencing semantics.
        if (earliest == null
            || distanceComparison < 0
            || (distanceComparison == 0 && location.compareTo(earliest) < 0)) {
          earliest = location;
          closestDistance = distance;
        }
      }
      if (earliest == null) {
        throw new IllegalStateException("Shared path point is not present in source geometry");
      }
      return earliest;
    }

    private SourceSegment findForPathCoordinate(CoordinateSequence path, int coordinateIndex) {
      int adjacentIndex = adjacentNonZeroCoordinate(path, coordinateIndex);
      Coordinate coordinate = path.getCoordinateCopy(coordinateIndex);
      Coordinate adjacent = path.getCoordinateCopy(adjacentIndex);
      SourceSegment earliest = null;
      double closestDistance = Double.POSITIVE_INFINITY;
      for (SourceSegment candidate : candidates(coordinate)) {
        // Overlay can collapse many collinear source vertices into one long output edge. Limit the
        // test samples to the portion of that edge covered by this candidate source segment.
        double sharedFraction = candidate.sharedFractionFrom(coordinate, adjacent);
        if (!(sharedFraction > 0)) {
          continue;
        }
        Coordinate startSample =
            LinearLocation.pointAlongSegmentByFraction(
                coordinate, adjacent, sharedFraction * START_SAMPLE_FRACTION);
        Coordinate endSample =
            LinearLocation.pointAlongSegmentByFraction(
                coordinate, adjacent, sharedFraction * END_SAMPLE_FRACTION);
        // Requiring two interior samples, rather than one midpoint, distinguishes the intended
        // traversal when the midpoint itself is a self-intersection with another source segment.
        if (!candidate.contains(startSample) || !candidate.contains(endSample)) {
          continue;
        }
        double distance =
            Math.max(candidate.distanceTo(startSample), candidate.distanceTo(endSample));
        int distanceComparison = Double.compare(distance, closestDistance);

        // As in locate(), distance protects against tolerance-admitted nearby segments and ordinal
        // makes repeated coincident traversals deterministic.
        if (earliest == null
            || distanceComparison < 0
            || (distanceComparison == 0 && candidate.ordinal < earliest.ordinal)) {
          earliest = candidate;
          closestDistance = distance;
        }
      }
      if (earliest == null) {
        throw new IllegalStateException("Shared path segment is not present in source geometry");
      }
      return earliest;
    }

    private List<SourceSegment> candidates(Coordinate coordinate) {
      // Expand before querying; otherwise an interpolated sample infinitesimally outside a segment
      // envelope would never reach SourceSegment.contains for the tolerance-aware final check.
      Envelope searchEnvelope = new Envelope(coordinate);
      searchEnvelope.expandBy(queryTolerance);
      @SuppressWarnings("unchecked")
      List<SourceSegment> candidates = index.query(searchEnvelope);
      return candidates;
    }

    private static int adjacentNonZeroCoordinate(CoordinateSequence sequence, int coordinateIndex) {
      // Prefer the following path edge so an interior vertex uses the traversal leaving it. For the
      // final coordinate, fall back to the preceding edge. Repeated XY coordinates are skipped.
      double x = sequence.getX(coordinateIndex);
      double y = sequence.getY(coordinateIndex);
      for (int i = coordinateIndex + 1; i < sequence.size(); i++) {
        if (sequence.getX(i) != x || sequence.getY(i) != y) {
          return i;
        }
      }
      for (int i = coordinateIndex - 1; i >= 0; i--) {
        if (sequence.getX(i) != x || sequence.getY(i) != y) {
          return i;
        }
      }
      throw new IllegalArgumentException("Shared path does not contain a non-zero segment");
    }
  }

  /** A non-zero source segment plus its position in the flattened source traversal. */
  private static final class SourceSegment {
    private final Coordinate start;
    private final Coordinate end;
    private final boolean hasZ;
    private final int ordinal;

    private SourceSegment(Coordinate start, Coordinate end, boolean hasZ, int ordinal) {
      this.start = start;
      this.end = end;
      this.hasZ = hasZ;
      this.ordinal = ordinal;
    }

    private double zAt(double x, double y) {
      boolean atStart = start.getX() == x && start.getY() == y;
      boolean atEnd = end.getX() == x && end.getY() == y;
      if (!hasZ) {
        return Double.NaN;
      }

      // Preserve exact endpoint ordinates before considering interpolation. The global exact-Z map
      // normally handles these cases; keeping this local rule also makes the segment primitive
      // correct when used independently.
      if (atStart) {
        return start.getZ();
      }
      if (atEnd) {
        return end.getZ();
      }
      double startZ = start.getZ();
      double endZ = end.getZ();
      if (!Double.isFinite(startZ) || !Double.isFinite(endZ)) {
        return Double.NaN;
      }

      // Interpolate on the dominant XY axis. For an exactly on-segment point the two axes are
      // mathematically equivalent; the dominant axis reduces roundoff and avoids division by a
      // tiny delta.
      double dx = end.getX() - start.getX();
      double dy = end.getY() - start.getY();
      double fraction =
          Math.abs(dx) >= Math.abs(dy) ? (x - start.getX()) / dx : (y - start.getY()) / dy;
      return startZ + fraction * (endZ - startZ);
    }

    private double sharedFractionFrom(Coordinate coordinate, Coordinate adjacent) {
      // Project both source endpoints onto the outgoing output edge. The largest positive projected
      // fraction bounds candidate-local samples; the later containment checks reject candidates
      // which are not actually collinear and overlapping. Clamp to one because only the current
      // output edge is relevant.
      double dx = adjacent.getX() - coordinate.getX();
      double dy = adjacent.getY() - coordinate.getY();
      double startFraction;
      double endFraction;
      if (Math.abs(dx) >= Math.abs(dy)) {
        startFraction = (start.getX() - coordinate.getX()) / dx;
        endFraction = (end.getX() - coordinate.getX()) / dx;
      } else {
        startFraction = (start.getY() - coordinate.getY()) / dy;
        endFraction = (end.getY() - coordinate.getY()) / dy;
      }
      return Math.min(1.0, Math.max(startFraction, endFraction));
    }

    private double fractionAlong(Coordinate coordinate) {
      // This is the linear-reference position used after the segment ordinal. Clamping absorbs the
      // same tiny roundoff for which the spatial lookup uses an ULP-scaled tolerance.
      double dx = end.getX() - start.getX();
      double dy = end.getY() - start.getY();
      double fraction =
          Math.abs(dx) >= Math.abs(dy)
              ? (coordinate.getX() - start.getX()) / dx
              : (coordinate.getY() - start.getY()) / dy;
      return Math.max(0.0, Math.min(1.0, fraction));
    }

    private boolean contains(Coordinate coordinate) {
      // Use a conservative floating-point tolerance for the containment check. Candidate ranking
      // still chooses the smallest actual distance, so any admitted nearby parallel line cannot
      // override the exact shared segment.
      double scale =
          Math.max(
              1.0,
              Math.max(
                  Math.max(Math.abs(start.getX()), Math.abs(start.getY())),
                  Math.max(
                      Math.max(Math.abs(end.getX()), Math.abs(end.getY())),
                      Math.max(Math.abs(coordinate.getX()), Math.abs(coordinate.getY())))));
      double tolerance = 16 * Math.ulp(scale);
      return distanceTo(coordinate) <= tolerance;
    }

    private double distanceTo(Coordinate coordinate) {
      return Distance.pointToSegment(coordinate, start, end);
    }
  }

  /** Hash key for an exact XY vertex lookup; Z and M deliberately do not participate. */
  private static final class XYKey {
    private final long x;
    private final long y;

    private XYKey(double x, double y) {
      this.x = normalizedBits(x);
      this.y = normalizedBits(y);
    }

    private static long normalizedBits(double value) {
      // Primitive double == treats -0.0 and +0.0 as equal, but their raw bit patterns differ.
      // Canonicalize both so the hash key has the same semantics as an XY coordinate comparison.
      return Double.doubleToLongBits(value == 0.0 ? 0.0 : value);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof XYKey)) {
        return false;
      }
      XYKey key = (XYKey) other;
      return x == key.x && y == key.y;
    }

    @Override
    public int hashCode() {
      int result = Long.hashCode(x);
      return 31 * result + Long.hashCode(y);
    }
  }

  /**
   * Linear-reference position in a source geometry.
   *
   * <p>The segment ordinal is compared first, then the fraction along that segment:
   *
   * <pre>
   * segment 4 @ 0.9  &lt;  segment 5 @ 0.1
   * segment 5 @ 0.1  &lt;  segment 5 @ 0.8
   * </pre>
   *
   * This ordering lets two interior samples reveal whether a source traverses a shared fragment
   * forward or backward without rescanning the original geometry.
   */
  private static final class SourceLocation implements Comparable<SourceLocation> {
    private final int segmentOrdinal;
    private final double segmentFraction;

    private SourceLocation(int segmentOrdinal, double segmentFraction) {
      this.segmentOrdinal = segmentOrdinal;
      this.segmentFraction = segmentFraction;
    }

    @Override
    public int compareTo(SourceLocation other) {
      // Segment order dominates fraction because the flattened ordinal follows source traversal.
      int segmentComparison = Integer.compare(segmentOrdinal, other.segmentOrdinal);
      return segmentComparison != 0
          ? segmentComparison
          : Double.compare(segmentFraction, other.segmentFraction);
    }
  }
}
