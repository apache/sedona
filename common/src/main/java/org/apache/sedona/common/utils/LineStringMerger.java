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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.util.LineStringExtracter;

/**
 * Post-processes line split results to merge adjacent segments that were only split by JTS overlay
 * noding, not by a real split point. This avoids extra line segments after {@code
 * Geometry#difference} on linework.
 *
 * <p>JTS has an internal line merge capability in LineBuilder (not yet exposed/used by the overlay
 * API). Once JTS supports it in future releases, we should rely on that directly. See
 * https://github.com/locationtech/jts/blob/1.20.0/modules/core/src/main/java/org/locationtech/jts/operation/overlayng/LineBuilder.java#L247-L261
 */
public final class LineStringMerger {
  private LineStringMerger() {}

  @SuppressWarnings("unchecked")
  public static Geometry mergeDifferenceSplit(
      Geometry linework, Geometry originalLines, Geometry blade, GeometryFactory factory) {
    if (linework == null || linework.isEmpty() || !GeomUtils.geometryIsLineal(linework)) {
      return linework;
    }

    List<Geometry> geoms = LineStringExtracter.getLines(linework);
    List<LineString> lines = new ArrayList<>();
    List<Geometry> nonLines = new ArrayList<>();
    extractLines(geoms, lines, nonLines);
    if (lines.size() <= 1) {
      return linework;
    }

    Set<CoordKey> stopPointIndex = buildStopPointIndex(originalLines, blade);
    List<LineString> merged = mergeLinesAtNonStopNodes(lines, stopPointIndex, factory);

    if (nonLines.isEmpty()) {
      return factory.buildGeometry(merged);
    } else {
      nonLines.addAll(merged);
      return factory.buildGeometry(nonLines);
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<CoordKey> buildStopPointIndex(Geometry originalLines, Geometry blade) {
    Set<CoordKey> index = new HashSet<>();
    if (originalLines != null && blade != null && !originalLines.isEmpty() && !blade.isEmpty()) {
      Coordinate[] coords = originalLines.intersection(blade).getCoordinates();
      for (Coordinate coord : coords) {
        index.add(new CoordKey(coord));
      }
    }

    if (originalLines == null || originalLines.isEmpty()) {
      return index;
    }

    List<Geometry> geoms = LineStringExtracter.getLines(originalLines);
    for (Geometry geom : geoms) {
      if (!(geom instanceof LineString)) {
        continue;
      }
      LineString line = (LineString) geom;
      if (line.isClosed() || line.getNumPoints() == 0) {
        continue;
      }
      index.add(new CoordKey(line.getCoordinateN(0)));
      index.add(new CoordKey(line.getCoordinateN(line.getNumPoints() - 1)));
    }

    return index;
  }

  private static void extractLines(
      List<Geometry> geoms, List<LineString> lines, List<Geometry> nonLines) {
    for (Geometry geom : geoms) {
      if (geom instanceof LineString) {
        lines.add((LineString) geom);
      } else {
        nonLines.add(geom);
      }
    }
  }

  private static List<LineString> mergeLinesAtNonStopNodes(
      List<LineString> lines, Set<CoordKey> stopPointIndex, GeometryFactory factory) {
    if (lines.size() <= 1) {
      return lines;
    }

    Map<CoordKey, List<EndpointRef>> endpointIndex = buildEndpointIndex(lines);
    boolean[] visited = new boolean[lines.size()];
    List<LineString> merged = new ArrayList<>();

    for (int i = 0; i < lines.size(); i++) {
      if (visited[i]) {
        continue;
      }

      LineString line = lines.get(i);
      Coordinate start = line.getCoordinateN(0);
      Coordinate end = line.getCoordinateN(line.getNumPoints() - 1);

      CoordKey startKey = new CoordKey(start);
      CoordKey endKey = new CoordKey(end);

      boolean startMergeable = isMergeableNode(startKey, endpointIndex, stopPointIndex);
      boolean endMergeable = isMergeableNode(endKey, endpointIndex, stopPointIndex);

      if (!startMergeable) {
        merged.add(
            mergeTowardsDirection(i, true, lines, endpointIndex, stopPointIndex, visited, factory));
        continue;
      }
      if (!endMergeable) {
        merged.add(
            mergeTowardsDirection(
                i, false, lines, endpointIndex, stopPointIndex, visited, factory));
      }
    }

    for (int i = 0; i < lines.size(); i++) {
      if (visited[i]) {
        continue;
      }
      merged.add(
          mergeTowardsDirection(i, true, lines, endpointIndex, stopPointIndex, visited, factory));
    }

    return merged;
  }

  private static Map<CoordKey, List<EndpointRef>> buildEndpointIndex(List<LineString> lines) {
    Map<CoordKey, List<EndpointRef>> index = new HashMap<>();
    for (int i = 0; i < lines.size(); i++) {
      LineString line = lines.get(i);
      addEndpoint(index, new CoordKey(line.getCoordinateN(0)), new EndpointRef(i));
      addEndpoint(
          index, new CoordKey(line.getCoordinateN(line.getNumPoints() - 1)), new EndpointRef(i));
    }
    return index;
  }

  private static void addEndpoint(
      Map<CoordKey, List<EndpointRef>> index, CoordKey key, EndpointRef ref) {
    List<EndpointRef> refs = index.computeIfAbsent(key, k -> new ArrayList<>());
    refs.add(ref);
  }

  private static boolean isMergeableNode(
      CoordKey key, Map<CoordKey, List<EndpointRef>> endpointIndex, Set<CoordKey> stopPointIndex) {
    if (stopPointIndex.contains(key)) {
      return false;
    }
    List<EndpointRef> refs = endpointIndex.get(key);
    return refs != null && refs.size() == 2;
  }

  private static LineString mergeTowardsDirection(
      int startLineIndex,
      boolean forward,
      List<LineString> lines,
      Map<CoordKey, List<EndpointRef>> endpointIndex,
      Set<CoordKey> stopPointIndex,
      boolean[] visited,
      GeometryFactory factory) {
    LineString first = lines.get(startLineIndex);
    Coordinate startNode =
        forward ? first.getCoordinateN(0) : first.getCoordinateN(first.getNumPoints() - 1);
    Coordinate[] firstCoords = orientedCoordinates(first, startNode);
    if (firstCoords == null) {
      visited[startLineIndex] = true;
      return first;
    }

    visited[startLineIndex] = true;
    CoordinateList coordList = new CoordinateList();
    coordList.add(firstCoords, false);
    Coordinate currentEnd = coordList.getCoordinate(coordList.size() - 1);
    int currentLineIndex = startLineIndex;

    while (true) {
      CoordKey endKey = new CoordKey(currentEnd);
      if (!isMergeableNode(endKey, endpointIndex, stopPointIndex)) {
        break;
      }

      EndpointRef nextRef = nextEndpointRef(endpointIndex.get(endKey), currentLineIndex);
      if (nextRef == null || visited[nextRef.lineIndex]) {
        break;
      }

      LineString nextLine = lines.get(nextRef.lineIndex);
      Coordinate[] nextCoords = orientedCoordinates(nextLine, currentEnd);
      if (nextCoords == null) {
        break;
      }

      for (int i = 1; i < nextCoords.length; i++) {
        coordList.add(nextCoords[i], false);
      }
      visited[nextRef.lineIndex] = true;
      currentLineIndex = nextRef.lineIndex;
      currentEnd = coordList.getCoordinate(coordList.size() - 1);

      if (currentEnd.equals2D(coordList.getCoordinate(0))) {
        break;
      }
    }

    return factory.createLineString(coordList.toCoordinateArray());
  }

  private static EndpointRef nextEndpointRef(List<EndpointRef> refs, int currentLineIndex) {
    if (refs == null) {
      return null;
    }
    for (EndpointRef ref : refs) {
      if (ref.lineIndex != currentLineIndex) {
        return ref;
      }
    }
    return null;
  }

  private static Coordinate[] orientedCoordinates(LineString line, Coordinate shared) {
    Coordinate[] coords = line.getCoordinates();
    if (coords.length == 0) {
      return null;
    }
    boolean start = coords[0].equals2D(shared);
    boolean end = coords[coords.length - 1].equals2D(shared);
    if (start && end) {
      return null;
    }
    if (start) {
      return coords;
    }
    if (end) {
      return reverse(coords);
    }
    return null;
  }

  private static Coordinate[] reverse(Coordinate[] coords) {
    Coordinate[] reversed = new Coordinate[coords.length];
    for (int i = 0; i < coords.length; i++) {
      reversed[i] = coords[coords.length - 1 - i];
    }
    return reversed;
  }

  private static final class EndpointRef {
    final int lineIndex;

    EndpointRef(int lineIndex) {
      this.lineIndex = lineIndex;
    }
  }

  private static final class CoordKey {
    final long xBits;
    final long yBits;

    CoordKey(Coordinate c) {
      this.xBits = Double.doubleToLongBits(c.x);
      this.yBits = Double.doubleToLongBits(c.y);
    }

    @Override
    public int hashCode() {
      return Objects.hash(xBits, yBits);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CoordKey)) {
        return false;
      }
      CoordKey other = (CoordKey) obj;
      return xBits == other.xBits && yBits == other.yBits;
    }
  }
}
