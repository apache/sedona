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

/** Computes the portions of two lineal geometries which follow the same or opposite direction. */
public final class SharedPaths {
  private static final double START_SAMPLE_FRACTION = 0.1;
  private static final double END_SAMPLE_FRACTION = 0.9;

  private SharedPaths() {}

  /**
   * Returns a collection containing same-direction paths followed by opposite-direction paths.
   * Paths are oriented in the direction of {@code left}. Matching SRIDs are required. The result
   * retains Z when either input has Z and, like PostGIS, does not retain M.
   */
  public static Geometry compute(Geometry left, Geometry right) {
    if (left == null || right == null) {
      return null;
    }
    validateSRIDs(left, right);
    validateLineal(left);
    validateLineal(right);

    GeometryFactory resultFactory = resultFactory(left);
    if (left.isEmpty() || right.isEmpty()) {
      return emptyResult(resultFactory);
    }

    Geometry intersection = OverlayNGRobust.overlay(left, right, OverlayNG.INTERSECTION);
    List<LineString> sameDirection = new ArrayList<>();
    List<LineString> oppositeDirection = new ArrayList<>();
    boolean outputHasZ = hasZ(left) || hasZ(right);

    @SuppressWarnings("unchecked")
    List<LineString> paths = LineStringExtracter.getLines(intersection);
    SourceSegmentIndex leftSegments = !paths.isEmpty() ? new SourceSegmentIndex(left) : null;
    SourceSegmentIndex rightSegments = !paths.isEmpty() ? new SourceSegmentIndex(right) : null;
    for (LineString path : paths) {
      if (path.isEmpty()) {
        continue;
      }
      Direction direction = direction(path, leftSegments, rightSegments);
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
    MultiLineString same = factory.createMultiLineString();
    MultiLineString opposite = factory.createMultiLineString();
    return factory.createGeometryCollection(new Geometry[] {same, opposite});
  }

  private static Direction direction(
      LineString path, SourceSegmentIndex leftSegments, SourceSegmentIndex rightSegments) {
    CoordinateSequence sequence = path.getCoordinateSequence();
    boolean forwardOnLeft = leftSegments.isForward(sequence);
    boolean forwardOnRight = rightSegments.isForward(sequence);
    return new Direction(forwardOnLeft, forwardOnLeft == forwardOnRight);
  }

  private static int firstNonZeroSegment(CoordinateSequence sequence) {
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
    CoordinateSequence target = sequenceFactory.create(source.size(), outputHasZ ? 3 : 2, 0);

    for (int i = 0; i < source.size(); i++) {
      double x = source.getX(i);
      double y = source.getY(i);
      target.setOrdinate(i, CoordinateSequence.X, x);
      target.setOrdinate(i, CoordinateSequence.Y, y);
      if (outputHasZ) {
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
    for (int component = 0; component < geometry.getNumGeometries(); component++) {
      Geometry child = geometry.getGeometryN(component);
      if (child instanceof LineString && ((LineString) child).getCoordinateSequence().hasZ()) {
        return true;
      }
    }
    return false;
  }

  private static final class Direction {
    private final boolean forwardOnLeft;
    private final boolean same;

    private Direction(boolean forwardOnLeft, boolean same) {
      this.forwardOnLeft = forwardOnLeft;
      this.same = same;
    }
  }

  /** Indexes source locations for path direction and segment-aware Z interpolation. */
  private static final class SourceSegmentIndex {
    private final STRtree index = new STRtree();
    private final Map<XYKey, Double> exactZByVertex = new HashMap<>();
    private double queryTolerance = 16 * Math.ulp(1.0);

    private SourceSegmentIndex(Geometry geometry) {
      int ordinal = 0;
      for (int component = 0; component < geometry.getNumGeometries(); component++) {
        LineString line = (LineString) geometry.getGeometryN(component);
        CoordinateSequence sequence = line.getCoordinateSequence();
        double maxCoordinateMagnitude = 1.0;
        for (int vertex = 0; vertex < sequence.size(); vertex++) {
          maxCoordinateMagnitude =
              Math.max(
                  maxCoordinateMagnitude,
                  Math.max(Math.abs(sequence.getX(vertex)), Math.abs(sequence.getY(vertex))));
        }
        queryTolerance = Math.max(queryTolerance, 16 * Math.ulp(maxCoordinateMagnitude));
        if (sequence.hasZ()) {
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
          SourceSegment sourceSegment = new SourceSegment(start, end, sequence.hasZ(), ordinal++);
          index.insert(new Envelope(start, end), sourceSegment);
        }
      }
      index.build();
    }

    private double exactZ(double x, double y) {
      Double z = exactZByVertex.get(new XYKey(x, y));
      return z == null ? Double.NaN : z;
    }

    private boolean isForward(CoordinateSequence path) {
      int segmentIndex = firstNonZeroSegment(path);
      Coordinate start = path.getCoordinateCopy(segmentIndex);
      Coordinate end = path.getCoordinateCopy(segmentIndex + 1);
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
        if (!candidate.contains(startSample) || !candidate.contains(endSample)) {
          continue;
        }
        double distance =
            Math.max(candidate.distanceTo(startSample), candidate.distanceTo(endSample));
        int distanceComparison = Double.compare(distance, closestDistance);
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
      Envelope searchEnvelope = new Envelope(coordinate);
      searchEnvelope.expandBy(queryTolerance);
      @SuppressWarnings("unchecked")
      List<SourceSegment> candidates = index.query(searchEnvelope);
      return candidates;
    }

    private static int adjacentNonZeroCoordinate(CoordinateSequence sequence, int coordinateIndex) {
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
      double dx = end.getX() - start.getX();
      double dy = end.getY() - start.getY();
      double fraction =
          Math.abs(dx) >= Math.abs(dy) ? (x - start.getX()) / dx : (y - start.getY()) / dy;
      return startZ + fraction * (endZ - startZ);
    }

    private double sharedFractionFrom(Coordinate coordinate, Coordinate adjacent) {
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
      double dx = end.getX() - start.getX();
      double dy = end.getY() - start.getY();
      double fraction =
          Math.abs(dx) >= Math.abs(dy)
              ? (coordinate.getX() - start.getX()) / dx
              : (coordinate.getY() - start.getY()) / dy;
      return Math.max(0.0, Math.min(1.0, fraction));
    }

    private boolean contains(Coordinate coordinate) {
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

  private static final class XYKey {
    private final long x;
    private final long y;

    private XYKey(double x, double y) {
      this.x = normalizedBits(x);
      this.y = normalizedBits(y);
    }

    private static long normalizedBits(double value) {
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

  private static final class SourceLocation implements Comparable<SourceLocation> {
    private final int segmentOrdinal;
    private final double segmentFraction;

    private SourceLocation(int segmentOrdinal, double segmentFraction) {
      this.segmentOrdinal = segmentOrdinal;
      this.segmentFraction = segmentFraction;
    }

    @Override
    public int compareTo(SourceLocation other) {
      int segmentComparison = Integer.compare(segmentOrdinal, other.segmentOrdinal);
      return segmentComparison != 0
          ? segmentComparison
          : Double.compare(segmentFraction, other.segmentFraction);
    }
  }
}
