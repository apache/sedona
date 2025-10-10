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
package org.apache.sedona.common.approximate;

import java.util.*;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

/**
 * Voronoi-based medial axis approximation for polygons.
 *
 * <p>This implementation computes an approximate medial axis using Voronoi diagrams: 1. Densify
 * polygon boundary with evenly-spaced sample points 2. Compute Voronoi diagram of sample points 3.
 * Extract Voronoi edges that lie inside the polygon 4. Filter and clean up to produce medial axis
 *
 * <p>This approach is simpler than straight skeleton and works well for road network-like shapes.
 *
 * <p>References: - Lee, D.T. (1982). Medial Axis Transformation of a Planar Shape - Held, M.
 * (2001). VRONI: An engineering approach to Voronoi diagrams
 */
public class StraightSkeleton {

  private static final double EPSILON = 1e-10;
  private static final double DEFAULT_DENSIFICATION_DISTANCE = 5.0; // 5 units spacing

  public StraightSkeleton() {}

  /**
   * Compute the approximate medial axis for a polygon using Voronoi diagrams.
   *
   * @param polygon Input polygon (must be simple, non-self-intersecting)
   * @return MultiLineString representing the medial axis
   */
  public Geometry computeSkeleton(Polygon polygon) {
    if (polygon == null || polygon.isEmpty()) {
      return null;
    }

    GeometryFactory factory = polygon.getFactory();

    // Step 1: Densify polygon boundary
    System.out.println("DEBUG: Starting Voronoi-based medial axis computation");
    List<Coordinate> boundaryPoints = densifyPolygonBoundary(polygon);
    System.out.println("DEBUG: Densified boundary to " + boundaryPoints.size() + " points");

    if (boundaryPoints.size() < 3) {
      return factory.createMultiLineString(new LineString[0]);
    }

    // Step 2: Build Voronoi diagram
    VoronoiDiagramBuilder voronoiBuilder = new VoronoiDiagramBuilder();
    voronoiBuilder.setSites(boundaryPoints);
    voronoiBuilder.setTolerance(EPSILON);

    Geometry voronoiDiagram = voronoiBuilder.getDiagram(factory);
    System.out.println("DEBUG: Voronoi diagram: " + voronoiDiagram.getGeometryType());
    System.out.println(
        "DEBUG: Voronoi diagram has " + voronoiDiagram.getNumGeometries() + " cells");

    // Step 3: Extract Voronoi edges (cell boundaries)
    List<LineString> voronoiEdges = extractVoronoiEdges(voronoiDiagram, factory);
    System.out.println("DEBUG: Extracted " + voronoiEdges.size() + " Voronoi edges");

    // Step 4: Filter edges to keep only those inside polygon
    List<LineString> interiorEdges = filterInteriorEdges(voronoiEdges, polygon);
    System.out.println("DEBUG: Filtered to " + interiorEdges.size() + " interior edges");

    // Step 5: Clean up and simplify the medial axis
    List<LineString> medialAxis = cleanupMedialAxis(interiorEdges, polygon, factory);
    System.out.println("DEBUG: Final medial axis has " + medialAxis.size() + " segments");

    if (medialAxis.isEmpty()) {
      return factory.createMultiLineString(new LineString[0]);
    }

    return factory.createMultiLineString(medialAxis.toArray(new LineString[0]));
  }

  /**
   * Densify the polygon boundary with evenly-spaced sample points.
   *
   * <p>More sample points = more accurate medial axis, but slower computation.
   */
  private List<Coordinate> densifyPolygonBoundary(Polygon polygon) {
    List<Coordinate> points = new ArrayList<>();
    LineString boundary = polygon.getExteriorRing();

    // Calculate densification distance based on polygon size
    double perimeter = boundary.getLength();
    double densificationDistance = Math.min(DEFAULT_DENSIFICATION_DISTANCE, perimeter / 50.0);

    // Densify the boundary
    Geometry densified =
        org.locationtech.jts.densify.Densifier.densify(boundary, densificationDistance);
    Coordinate[] coords = densified.getCoordinates();

    // Add all points (skip last duplicate)
    for (int i = 0; i < coords.length - 1; i++) {
      points.add(new Coordinate(coords[i]));
    }

    return points;
  }

  /**
   * Extract Voronoi edges from the Voronoi diagram.
   *
   * <p>The Voronoi diagram is returned as a GeometryCollection of Polygons. We need to extract the
   * edges (boundaries) of these cells.
   */
  private List<LineString> extractVoronoiEdges(Geometry voronoiDiagram, GeometryFactory factory) {
    Set<LineString> edgesSet = new HashSet<>();

    for (int i = 0; i < voronoiDiagram.getNumGeometries(); i++) {
      Geometry cell = voronoiDiagram.getGeometryN(i);

      if (cell instanceof Polygon) {
        Polygon voronoiCell = (Polygon) cell;
        LineString ring = voronoiCell.getExteriorRing();

        // Extract individual edges from the ring
        Coordinate[] coords = ring.getCoordinates();
        for (int j = 0; j < coords.length - 1; j++) {
          Coordinate c1 = coords[j];
          Coordinate c2 = coords[j + 1];

          // Create normalized edge (to avoid duplicates with different orientations)
          Coordinate[] edgeCoords;
          if (c1.compareTo(c2) < 0) {
            edgeCoords = new Coordinate[] {new Coordinate(c1), new Coordinate(c2)};
          } else {
            edgeCoords = new Coordinate[] {new Coordinate(c2), new Coordinate(c1)};
          }

          LineString edge = factory.createLineString(edgeCoords);
          edgesSet.add(edge);
        }
      }
    }

    return new ArrayList<>(edgesSet);
  }

  /**
   * Filter Voronoi edges to keep only those that lie inside the polygon.
   *
   * <p>An edge is considered interior if: 1. Its midpoint is inside the polygon 2. Both endpoints
   * are reasonably close to the polygon (not on exterior boundary)
   */
  private List<LineString> filterInteriorEdges(List<LineString> edges, Polygon polygon) {
    List<LineString> interiorEdges = new ArrayList<>();
    Geometry polygonBoundary = polygon.getBoundary();

    for (LineString edge : edges) {
      Coordinate[] coords = edge.getCoordinates();
      if (coords.length < 2) continue;

      Coordinate c1 = coords[0];
      Coordinate c2 = coords[1];

      // Compute midpoint
      Coordinate midpoint = new Coordinate((c1.x + c2.x) / 2.0, (c1.y + c2.y) / 2.0);
      Point midpointGeom = polygon.getFactory().createPoint(midpoint);

      // Check if midpoint is inside polygon
      if (!polygon.contains(midpointGeom)) {
        continue;
      }

      // Check if both endpoints are inside or very close to polygon
      Point p1 = polygon.getFactory().createPoint(c1);
      Point p2 = polygon.getFactory().createPoint(c2);

      boolean p1Inside = polygon.contains(p1) || polygon.distance(p1) < EPSILON;
      boolean p2Inside = polygon.contains(p2) || polygon.distance(p2) < EPSILON;

      if (!p1Inside || !p2Inside) {
        continue;
      }

      // Filter out edges on the polygon boundary
      double distanceToBoundary = polygonBoundary.distance(edge);
      if (distanceToBoundary < EPSILON * 10) {
        continue; // Edge is on or very close to boundary
      }

      interiorEdges.add(edge);
    }

    return interiorEdges;
  }

  /**
   * Clean up and simplify the medial axis.
   *
   * <p>This removes very short edges, merges collinear segments, and filters out minor branches.
   */
  private List<LineString> cleanupMedialAxis(
      List<LineString> edges, Polygon polygon, GeometryFactory factory) {

    if (edges.isEmpty()) {
      return new ArrayList<>();
    }

    // Step 1: Filter out very short edges
    double minLength = polygon.getEnvelope().getLength() * 0.01; // 1% of envelope perimeter
    List<LineString> significantEdges = new ArrayList<>();

    for (LineString edge : edges) {
      if (edge.getLength() > minLength) {
        significantEdges.add(edge);
      }
    }

    if (significantEdges.isEmpty()) {
      // If all edges were too short, keep the longest ones
      edges.sort((e1, e2) -> Double.compare(e2.getLength(), e1.getLength()));
      int keepCount = Math.min(3, edges.size());
      for (int i = 0; i < keepCount; i++) {
        significantEdges.add(edges.get(i));
      }
    }

    System.out.println("DEBUG: After length filtering: " + significantEdges.size() + " edges");

    // Step 2: Build connectivity graph (using coordinate keys with tolerance)
    Map<CoordKey, List<EdgeInfo>> vertexEdges = new HashMap<>();
    List<EdgeInfo> edgeInfos = new ArrayList<>();

    for (LineString edge : significantEdges) {
      Coordinate[] coords = edge.getCoordinates();
      if (coords.length < 2) continue;

      EdgeInfo info = new EdgeInfo(edge, coords[0], coords[coords.length - 1]);
      edgeInfos.add(info);

      CoordKey startKey = new CoordKey(coords[0]);
      CoordKey endKey = new CoordKey(coords[coords.length - 1]);

      vertexEdges.computeIfAbsent(startKey, k -> new ArrayList<>()).add(info);
      vertexEdges.computeIfAbsent(endKey, k -> new ArrayList<>()).add(info);
    }

    System.out.println("DEBUG: Built connectivity graph with " + vertexEdges.size() + " vertices");

    // Step 3: Merge collinear and connected segments
    Set<EdgeInfo> visited = new HashSet<>();
    List<LineString> mergedSegments = new ArrayList<>();

    for (EdgeInfo startEdge : edgeInfos) {
      if (visited.contains(startEdge)) continue;

      try {
        // Build a path by extending in both directions
        List<Coordinate> path = buildPath(startEdge, vertexEdges, visited);

        if (path.size() >= 2) {
          // Simplify path by removing collinear intermediate points
          List<Coordinate> simplified = simplifyCollinearPoints(path);

          if (simplified.size() >= 2) {
            LineString merged = factory.createLineString(simplified.toArray(new Coordinate[0]));
            mergedSegments.add(merged);
            System.out.println(
                "DEBUG:   Created merged segment with "
                    + path.size()
                    + " points, simplified to "
                    + simplified.size());
          }
        }
      } catch (Exception e) {
        System.out.println("DEBUG:   Error building path: " + e.getMessage());
        e.printStackTrace();
      }
    }

    System.out.println("DEBUG: After merging: " + mergedSegments.size() + " segments");

    return mergedSegments.isEmpty() ? significantEdges : mergedSegments;
  }

  /** Build a path by extending from a starting edge in both directions */
  private List<Coordinate> buildPath(
      EdgeInfo startEdge, Map<CoordKey, List<EdgeInfo>> vertexEdges, Set<EdgeInfo> visited) {

    List<Coordinate> path = new ArrayList<>();
    Coordinate[] startCoords = startEdge.edge.getCoordinates();

    if (startCoords == null || startCoords.length == 0) {
      System.out.println("DEBUG:   buildPath: startEdge has no coordinates!");
      return path;
    }

    // Add all coordinates from start edge
    for (Coordinate c : startCoords) {
      path.add(new Coordinate(c));
    }
    visited.add(startEdge);

    System.out.println(
        "DEBUG:   buildPath: Starting with "
            + startCoords.length
            + " coords from edge, path size="
            + path.size());

    // Try to extend from end
    extendPath(path, startEdge.end, vertexEdges, visited, false);

    // Try to extend from start (reverse direction)
    Collections.reverse(path);
    extendPath(path, startEdge.start, vertexEdges, visited, false);
    Collections.reverse(path);

    System.out.println("DEBUG:   buildPath: Final path has " + path.size() + " coordinates");

    return path;
  }

  /** Extend a path from one end by following connected edges */
  private void extendPath(
      List<Coordinate> path,
      Coordinate endPoint,
      Map<CoordKey, List<EdgeInfo>> vertexEdges,
      Set<EdgeInfo> visited,
      boolean requireCollinear) {

    if (endPoint == null) {
      return;
    }

    CoordKey endKey = new CoordKey(endPoint);
    List<EdgeInfo> connectedEdges = vertexEdges.getOrDefault(endKey, Collections.emptyList());

    // Filter to unvisited edges
    List<EdgeInfo> candidates = new ArrayList<>();
    for (EdgeInfo edge : connectedEdges) {
      if (!visited.contains(edge)) {
        candidates.add(edge);
      }
    }

    // If there's only one candidate at a degree-2 vertex, extend
    if (candidates.size() == 1 && connectedEdges.size() == 2) {
      EdgeInfo nextEdge = candidates.get(0);

      // Check collinearity if required
      if (requireCollinear && path.size() >= 2) {
        Coordinate prev = path.get(path.size() - 2);
        Coordinate current = path.get(path.size() - 1);

        if (!isCollinear(prev, current, nextEdge.getOtherEnd(endPoint))) {
          return; // Not collinear, stop extending
        }
      }

      visited.add(nextEdge);

      // Add coordinates from next edge
      Coordinate[] nextCoords = nextEdge.edge.getCoordinates();
      Coordinate otherEnd = nextEdge.getOtherEnd(endPoint);

      // Determine direction to add coordinates
      boolean startMatches = nextCoords[0].equals2D(endPoint);

      if (startMatches) {
        // Add coordinates from index 1 onwards
        for (int i = 1; i < nextCoords.length; i++) {
          path.add(new Coordinate(nextCoords[i]));
        }
      } else {
        // Add coordinates in reverse (from end-1 down to 0)
        for (int i = nextCoords.length - 2; i >= 0; i--) {
          path.add(new Coordinate(nextCoords[i]));
        }
      }

      // Recursively extend
      extendPath(path, otherEnd, vertexEdges, visited, requireCollinear);
    }
  }

  /** Remove collinear intermediate points from a path */
  private List<Coordinate> simplifyCollinearPoints(List<Coordinate> path) {
    if (path.size() <= 2) {
      return new ArrayList<>(path);
    }

    List<Coordinate> simplified = new ArrayList<>();
    simplified.add(path.get(0));

    for (int i = 1; i < path.size() - 1; i++) {
      Coordinate prev = path.get(i - 1);
      Coordinate current = path.get(i);
      Coordinate next = path.get(i + 1);

      // Keep point if it's not collinear with neighbors
      if (!isCollinear(prev, current, next)) {
        simplified.add(current);
      }
    }

    simplified.add(path.get(path.size() - 1));
    return simplified;
  }

  /** Check if three points are collinear (within tolerance) */
  private boolean isCollinear(Coordinate p1, Coordinate p2, Coordinate p3) {
    // Use cross product to check collinearity
    double dx1 = p2.x - p1.x;
    double dy1 = p2.y - p1.y;
    double dx2 = p3.x - p2.x;
    double dy2 = p3.y - p2.y;

    double cross = dx1 * dy2 - dy1 * dx2;
    double tolerance = 0.5; // Generous tolerance for near-collinear points

    return Math.abs(cross) < tolerance;
  }

  /** Helper class to store edge info with endpoints */
  private static class EdgeInfo {
    LineString edge;
    Coordinate start;
    Coordinate end;

    EdgeInfo(LineString edge, Coordinate start, Coordinate end) {
      this.edge = edge;
      this.start = start;
      this.end = end;
    }

    Coordinate getOtherEnd(Coordinate point) {
      // Use distance-based comparison with tolerance
      double distToStart = start.distance(point);
      double distToEnd = end.distance(point);

      if (distToStart < CoordKey.SNAP_TOLERANCE) {
        return end;
      } else if (distToEnd < CoordKey.SNAP_TOLERANCE) {
        return start;
      }
      return null;
    }
  }

  /** Coordinate key with tolerance for hash map lookups */
  private static class CoordKey {
    private static final double SNAP_TOLERANCE = 0.1;
    private final long xBucket;
    private final long yBucket;

    CoordKey(Coordinate coord) {
      this.xBucket = (long) Math.round(coord.x / SNAP_TOLERANCE);
      this.yBucket = (long) Math.round(coord.y / SNAP_TOLERANCE);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CoordKey)) return false;
      CoordKey other = (CoordKey) obj;
      return this.xBucket == other.xBucket && this.yBucket == other.yBucket;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(xBucket) * 31 + Long.hashCode(yBucket);
    }
  }
}
