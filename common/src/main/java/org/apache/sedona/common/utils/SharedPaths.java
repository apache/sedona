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
import org.locationtech.jts.index.strtree.ItemDistance;
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
 * Stage 4: orient like the first input and restore source-derived Z values
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
   * Paths are oriented in the direction of {@code left}. Matching SRIDs are required. When either
   * input has Z, the result retains Z only if every returned coordinate resolves to a finite source
   * Z; otherwise the whole result is XY. Like PostGIS, the result does not retain M.
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

    // A shared path requires the envelopes to overlap at overlay precision. Avoid overlay setup,
    // dimensional scanning, and result assembly when the precision-adjusted envelopes prove the
    // result is empty.
    if (envelopesDisjoint(left, right)) {
      return emptyResult(resultFactory);
    }

    // Stage 1: robust overlay finds the physical shared coverage. Point-only intersections are
    // intentionally ignored later because LineStringExtracter returns only linear components.
    Geometry intersection = OverlayNGRobust.overlay(left, right, OverlayNG.INTERSECTION);
    boolean retainZ = hasZ(left) || hasZ(right);

    @SuppressWarnings("unchecked")
    List<LineString> paths = LineStringExtracter.getLines(intersection);

    // Stage 2: build each source index once. Besides avoiding repeated scans, the index preserves
    // source traversal order so repeated or self-overlapping paths have deterministic semantics.
    double overlayTolerance = overlayTolerance(left.getPrecisionModel());
    SourceSegmentIndex leftSegments =
        !paths.isEmpty() ? new SourceSegmentIndex(left, overlayTolerance) : null;
    SourceSegmentIndex rightSegments =
        !paths.isEmpty() ? new SourceSegmentIndex(right, overlayTolerance) : null;
    List<PreparedPath> preparedPaths = new ArrayList<>();
    for (LineString path : paths) {
      if (path.isEmpty()) {
        continue;
      }
      // Stage 3: determine how this overlay fragment is traversed in each original input.
      Direction direction = direction(path, leftSegments, rightSegments);

      // Stage 4: overlay output direction is not a contract. Normalize it to the direction of the
      // first input before reconstructing ordinates or adding the path to a result bucket.
      LineString orientedPath = direction.forwardOnLeft ? path : (LineString) path.reverse();
      double[] zValues =
          retainZ ? resolvePostGISZ(orientedPath, leftSegments, rightSegments) : null;
      if (retainZ && zValues == null) {
        // WKT and WKB assign one dimensionality to the whole result. If any returned coordinate
        // has no finite source Z, use XY throughout rather than fabricating a value or emitting
        // NaN.
        retainZ = false;
      }
      preparedPaths.add(new PreparedPath(orientedPath, direction.same, zValues));
    }

    // Stage 5: even when one bucket is empty, retain both typed MultiLineString children.
    List<LineString> sameDirection = new ArrayList<>();
    List<LineString> oppositeDirection = new ArrayList<>();
    for (PreparedPath preparedPath : preparedPaths) {
      LineString resultPath =
          copyWithOrdinates(
              preparedPath.path, resultFactory, retainZ ? preparedPath.zValues : null);
      (preparedPath.same ? sameDirection : oppositeDirection).add(resultPath);
    }
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

  private static double overlayTolerance(PrecisionModel precisionModel) {
    if (precisionModel.isFloating()) {
      return 0;
    }
    // A fixed-precision overlay may round both ordinates by half a grid cell. Use the diagonal
    // displacement so the snapped result can still be located on the original source linework.
    return Math.sqrt(0.5) * precisionModel.gridSize();
  }

  private static boolean envelopesDisjoint(Geometry left, Geometry right) {
    Envelope leftEnvelope = left.getEnvelopeInternal();
    Envelope rightEnvelope = right.getEnvelopeInternal();
    PrecisionModel precisionModel = left.getPrecisionModel();
    if (precisionModel.isFloating()) {
      return leftEnvelope.disjoint(rightEnvelope);
    }

    // Fixed-precision overlay rounds coordinates to the first input's grid. Compare rounded
    // envelope bounds so nearby raw envelopes are not rejected when rounding makes them overlap.
    return precisionModel.makePrecise(rightEnvelope.getMinX())
            > precisionModel.makePrecise(leftEnvelope.getMaxX())
        || precisionModel.makePrecise(rightEnvelope.getMaxX())
            < precisionModel.makePrecise(leftEnvelope.getMinX())
        || precisionModel.makePrecise(rightEnvelope.getMinY())
            > precisionModel.makePrecise(leftEnvelope.getMaxY())
        || precisionModel.makePrecise(rightEnvelope.getMaxY())
            < precisionModel.makePrecise(leftEnvelope.getMinY());
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

  private static LineString copyWithOrdinates(
      LineString path, GeometryFactory factory, double[] zValues) {
    CoordinateSequence source = path.getCoordinateSequence();
    CoordinateSequenceFactory sequenceFactory = factory.getCoordinateSequenceFactory();

    // Shared-path topology is two-dimensional. Build a fresh XY or XYZ sequence so M is always
    // removed. A non-null zValues array has already been checked to contain only finite values.
    CoordinateSequence target = sequenceFactory.create(source.size(), zValues != null ? 3 : 2, 0);

    for (int i = 0; i < source.size(); i++) {
      double x = source.getX(i);
      double y = source.getY(i);
      target.setOrdinate(i, CoordinateSequence.X, x);
      target.setOrdinate(i, CoordinateSequence.Y, y);
      if (zValues != null) {
        target.setOrdinate(i, CoordinateSequence.Z, zValues[i]);
      }
    }
    return factory.createLineString(target);
  }

  private static double[] resolvePostGISZ(
      LineString path, SourceSegmentIndex leftSegments, SourceSegmentIndex rightSegments) {
    CoordinateSequence sequence = path.getCoordinateSequence();
    double[] zValues = new double[sequence.size()];
    for (int i = 0; i < sequence.size(); i++) {
      zValues[i] = postGISZ(sequence, i, leftSegments, rightSegments);
      if (!Double.isFinite(zValues[i])) {
        return null;
      }
    }
    return zValues;
  }

  private static double postGISZ(
      CoordinateSequence path,
      int coordinateIndex,
      SourceSegmentIndex leftSegments,
      SourceSegmentIndex rightSegments) {
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
    double x = path.getX(coordinateIndex);
    double y = path.getY(coordinateIndex);
    double exactZ = leftSegments.exactZ(x, y);
    if (Double.isFinite(exactZ)) {
      return exactZ;
    }
    exactZ = rightSegments.exactZ(x, y);
    if (Double.isFinite(exactZ)) {
      return exactZ;
    }
    // Segment searches are deliberately lazy. Exact vertices are the common case, and a source
    // with no finite-Z segment can never contribute an interpolated value.
    if (leftSegments.hasInterpolatableZ()) {
      double leftZ = leftSegments.findForPathCoordinate(path, coordinateIndex).zAt(x, y);
      if (Double.isFinite(leftZ)) {
        return leftZ;
      }
    }
    if (rightSegments.hasInterpolatableZ()) {
      return rightSegments.findForPathCoordinate(path, coordinateIndex).zAt(x, y);
    }
    return Double.NaN;
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

  private static final class PreparedPath {
    private final LineString path;
    private final boolean same;
    private final double[] zValues;

    private PreparedPath(LineString path, boolean same, double[] zValues) {
      this.path = path;
      this.same = same;
      this.zValues = zValues;
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
    private static final ItemDistance SEGMENT_TO_POINT_DISTANCE =
        (first, second) -> {
          Object firstItem = first.getItem();
          SourceSegment segment =
              firstItem instanceof SourceSegment
                  ? (SourceSegment) firstItem
                  : (SourceSegment) second.getItem();
          Coordinate coordinate =
              firstItem instanceof Coordinate
                  ? (Coordinate) firstItem
                  : (Coordinate) second.getItem();
          return segment.distanceTo(coordinate);
        };

    private final STRtree index = new STRtree();
    private final Map<XYKey, Double> exactZByVertex = new HashMap<>();
    private boolean hasInterpolatableZ;

    // Overlay interpolation can move a theoretically-on-segment sample by a few ULPs, while a
    // fixed precision model can move it to the nearest grid point. Track both displacements so the
    // STRtree query envelope still reaches the true segment before the distance check below.
    private double queryTolerance;

    private SourceSegmentIndex(Geometry geometry, double overlayTolerance) {
      queryTolerance = Math.max(16 * Math.ulp(1.0), overlayTolerance);
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

          // The envelope is only a candidate filter. Actual distance and traversal order rank the
          // segments after querying the tree.
          SourceSegment sourceSegment = new SourceSegment(start, end, sequence.hasZ(), ordinal++);
          hasInterpolatableZ |= sourceSegment.hasInterpolatableZ();
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

    private boolean hasInterpolatableZ() {
      return hasInterpolatableZ;
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
      SourceSegment closest = closestSegment(coordinate, candidates(coordinate), queryTolerance);
      if (closest == null) {
        // Robust overlay snapping can move a valid result farther than a predictable number of
        // ULPs. An indexed nearest-neighbour fallback avoids failing the whole query while keeping
        // the normal on-source path bounded by the small tolerance above.
        closest =
            closestSegment(coordinate, nearestCandidates(coordinate), Double.POSITIVE_INFINITY);
      }
      if (closest == null) {
        throw new IllegalStateException("Shared path point is not present in source geometry");
      }
      return new SourceLocation(closest, coordinate);
    }

    private SourceSegment findForPathCoordinate(CoordinateSequence path, int coordinateIndex) {
      int adjacentIndex = adjacentNonZeroCoordinate(path, coordinateIndex);
      Coordinate coordinate = path.getCoordinateCopy(coordinateIndex);
      Coordinate adjacent = path.getCoordinateCopy(adjacentIndex);
      SourceSegment closest =
          closestPathSegment(coordinate, adjacent, candidates(coordinate), true);
      if (closest == null) {
        closest = closestPathSegment(coordinate, adjacent, nearestCandidates(coordinate), false);
      }
      if (closest == null) {
        throw new IllegalStateException("Shared path segment is not present in source geometry");
      }
      return closest;
    }

    private SourceSegment closestPathSegment(
        Coordinate coordinate,
        Coordinate adjacent,
        List<SourceSegment> candidates,
        boolean requireTolerance) {
      SourceSegment closest = null;
      double closestDistance = Double.POSITIVE_INFINITY;
      for (SourceSegment candidate : candidates) {
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
        double startDistance = candidate.distanceTo(startSample);
        double endDistance = candidate.distanceTo(endSample);
        if (requireTolerance && (startDistance > queryTolerance || endDistance > queryTolerance)) {
          continue;
        }
        double distance = Math.max(startDistance, endDistance);

        // As in locate(), distance protects against tolerance-admitted nearby segments and ordinal
        // makes repeated coincident traversals deterministic.
        if (isBetter(candidate, distance, closest, closestDistance)) {
          closest = candidate;
          closestDistance = distance;
        }
      }
      return closest;
    }

    private static SourceSegment closestSegment(
        Coordinate coordinate, List<SourceSegment> candidates, double maximumDistance) {
      SourceSegment closest = null;
      double closestDistance = Double.POSITIVE_INFINITY;
      for (SourceSegment candidate : candidates) {
        double distance = candidate.distanceTo(coordinate);
        if (distance <= maximumDistance
            && isBetter(candidate, distance, closest, closestDistance)) {
          closest = candidate;
          closestDistance = distance;
        }
      }
      return closest;
    }

    private static boolean isBetter(
        SourceSegment candidate, double distance, SourceSegment closest, double closestDistance) {
      int distanceComparison = Double.compare(distance, closestDistance);
      return closest == null
          || distanceComparison < 0
          || (distanceComparison == 0 && candidate.ordinal < closest.ordinal);
    }

    private List<SourceSegment> candidates(Coordinate coordinate) {
      // Expand before querying; otherwise an interpolated or grid-snapped sample just outside a
      // segment envelope would never reach the tolerance-aware distance check.
      Envelope searchEnvelope = new Envelope(coordinate);
      searchEnvelope.expandBy(queryTolerance);
      @SuppressWarnings("unchecked")
      List<SourceSegment> candidates = index.query(searchEnvelope);
      return candidates;
    }

    private List<SourceSegment> nearestCandidates(Coordinate coordinate) {
      SourceSegment nearest =
          (SourceSegment)
              index.nearestNeighbour(
                  new Envelope(coordinate), coordinate, SEGMENT_TO_POINT_DISTANCE);
      if (nearest == null) {
        return new ArrayList<>();
      }
      double distance = nearest.distanceTo(coordinate);
      Envelope searchEnvelope = new Envelope(coordinate);
      searchEnvelope.expandBy(Math.nextUp(distance + queryTolerance));
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

    private boolean hasInterpolatableZ() {
      return hasZ && Double.isFinite(start.getZ()) && Double.isFinite(end.getZ());
    }

    private double sharedFractionFrom(Coordinate coordinate, Coordinate adjacent) {
      // Project both source endpoints onto the outgoing output edge. The largest positive projected
      // fraction bounds candidate-local samples; the later distance checks reject candidates
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
   * <p>The segment ordinal is compared first, then the displacement between two samples is
   * projected onto that segment:
   *
   * <pre>
   * segment 4 @ 90  &lt;  segment 5 @ 10
   * segment 5 moving with the source  &lt;  segment 5 farther along the source
   * </pre>
   *
   * This ordering lets two interior samples reveal whether a source traverses a shared fragment
   * forward or backward without rescanning the original geometry.
   */
  private static final class SourceLocation implements Comparable<SourceLocation> {
    private final SourceSegment segment;
    private final Coordinate coordinate;

    private SourceLocation(SourceSegment segment, Coordinate coordinate) {
      this.segment = segment;
      this.coordinate = coordinate;
    }

    @Override
    public int compareTo(SourceLocation other) {
      // Segment order dominates position because the flattened ordinal follows source traversal.
      int segmentComparison = Integer.compare(segment.ordinal, other.segment.ordinal);
      if (segmentComparison != 0) {
        return segmentComparison;
      }

      // Compare the two samples directly rather than normalizing each against a possibly distant
      // segment endpoint. Scaling the source vector avoids overflow without changing the sign of
      // the projection. Using both axes also handles fixed-precision overlays which change which
      // axis is dominant after snapping.
      double sourceDx = segment.end.getX() - segment.start.getX();
      double sourceDy = segment.end.getY() - segment.start.getY();
      double scale = Math.max(Math.abs(sourceDx), Math.abs(sourceDy));
      double sampleDx = coordinate.getX() - other.coordinate.getX();
      double sampleDy = coordinate.getY() - other.coordinate.getY();
      double projection = Math.fma(sampleDx, sourceDx / scale, sampleDy * (sourceDy / scale));
      return projection == 0.0 ? 0 : Double.compare(projection, 0.0);
    }
  }
}
