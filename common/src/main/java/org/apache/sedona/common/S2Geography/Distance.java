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

import com.google.common.geometry.*;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

public class Distance {

  public double S2_distance(ShapeIndexGeography geo1, ShapeIndexGeography geo2) {
    S2ShapeIndex index1 = geo1.shapeIndex;
    S2ShapeIndex index2 = geo2.shapeIndex;

    S2ClosestEdgeQuery query = S2ClosestEdgeQuery.builder().build(index1);
    S2ClosestEdgeQuery.ShapeIndexTarget queryTarget =
        S2ClosestEdgeQuery.createShapeIndexTarget(index2);

    Optional<S2BestEdgesQueryBase.Result> resultVisitor = query.findClosestEdge(queryTarget);
    // If there are no edges at all, return infinity
    if (!resultVisitor.isPresent()) {
      return Double.POSITIVE_INFINITY;
    }

    // Extract the Result: it contains two points (one on each shape)
    //    and the chord‐angle distance between them.
    S2ClosestEdgeQuery.Result result = resultVisitor.get();
    S1ChordAngle chordAngle =
        (S1ChordAngle) result.distance(); // a specialized spherical‐distance type
    return chordAngle.toAngle().radians();
  }

  public double S2_maxDistance(ShapeIndexGeography geo1, ShapeIndexGeography geo2) {
    S2ShapeIndex index1 = geo1.shapeIndex;
    S2ShapeIndex index2 = geo2.shapeIndex;

    S2FurthestEdgeQuery query = S2FurthestEdgeQuery.builder().build(index1);
    S2FurthestEdgeQuery.ShapeIndexTarget queryTarget =
        S2FurthestEdgeQuery.createShapeIndexTarget(index2);

    Optional<S2FurthestEdgeQuery.Result> resultVisitor = query.findFurthestEdge(queryTarget);
    // If there are no edges at all, return infinity
    if (!resultVisitor.isPresent()) {
      return Double.POSITIVE_INFINITY;
    }

    // Extract the Result: it contains two points (one on each shape)
    //    and the chord‐angle distance between them.
    S2FurthestEdgeQuery.Result result = resultVisitor.get();
    S1ChordAngle chordAngle = (S1ChordAngle) result.distance();
    return chordAngle.toAngle().radians();
  }

  public S2Point S2_closestPoint(ShapeIndexGeography geo1, ShapeIndexGeography geo2)
      throws Exception {
    return S2_minimumClearanceLineBetween(geo1, geo2).getLeft();
  }

  // returns the shortest possible line between x and y
  // This method finds the two points—one on each geometry—that lie at the minimal great-circle
  // (angular) distance, and returns them as a pair.
  public Pair<S2Point, S2Point> S2_minimumClearanceLineBetween(
      ShapeIndexGeography geo1, ShapeIndexGeography geo2) throws Exception {

    S2ShapeIndex index1 = geo1.shapeIndex;
    S2ShapeIndex index2 = geo2.shapeIndex;

    S2ClosestEdgeQuery query =
        S2ClosestEdgeQuery.builder().setIncludeInteriors(false).build(index1);
    S2ClosestEdgeQuery.ShapeIndexTarget queryTarget =
        S2ClosestEdgeQuery.createShapeIndexTarget(index2);

    Optional<S2BestEdgesQueryBase.Result> resultVisitor = query.findClosestEdge(queryTarget);

    if (!resultVisitor.isPresent() || resultVisitor.get().edgeId() == -1) {
      return Pair.of(new S2Point(0, 0, 0), new S2Point(0, 0, 0));
    }

    // Get the edge from index1 (edge1) that is closest to index2.
    S2BestEdgesQueryBase.Result edge = resultVisitor.get();
    int shapeId = edge.shapeId();
    int edgeNum = edge.edgeId();
    S2Shape.MutableEdge mutableEdge = new S2Shape.MutableEdge();
    index1.getShapes().get(shapeId).getEdge(edgeNum, mutableEdge);
    S2Edge s2edge = (S2Edge) new S2Edge(mutableEdge.getStart(), mutableEdge.getEnd());

    // Now find the edge from index2 (edge2) that is closest to edge1.
    S2ClosestEdgeQuery queryReverse =
        S2ClosestEdgeQuery.builder().setIncludeInteriors(false).build(index2);
    S2ClosestEdgeQuery.EdgeTarget queryTargetReverse =
        new S2ClosestEdgeQuery.EdgeTarget<>(s2edge.getStart(), s2edge.getEnd());
    Optional<S2BestEdgesQueryBase.Result> resultVisitorReverse =
        queryReverse.findClosestEdge(queryTargetReverse);

    if (resultVisitorReverse.get().isInterior()) {
      throw new Exception("S2ClosestEdgeQuery result is interior!");
    }
    S2ClosestEdgeQuery.Result edge2 = resultVisitorReverse.get();
    int shapeId2 = edge2.shapeId();
    int edgeNum2 = edge2.edgeId();
    S2Shape.MutableEdge mutableEdge2 = new S2Shape.MutableEdge();
    index2.getShapes().get(shapeId2).getEdge(edgeNum2, mutableEdge2);
    S2Edge s2edge2 = new S2Edge(mutableEdge2.getStart(), mutableEdge2.getEnd());
    S2Edge s2edgeReverse = new S2Edge(s2edge2.getStart(), s2edge2.getEnd());

    return getEdgePairClosestPoints(
        s2edge.getStart(), s2edge.getEnd(), s2edgeReverse.getStart(), s2edgeReverse.getEnd());
  }

  /**
   * The Java equivalent of:
   *
   * <p>std::pair<S2Point,S2Point> GetEdgePairClosestPoints(a0,a1,b0,b1) # returns the shortest
   * possible line between x and y
   */
  public static Pair<S2Point, S2Point> getEdgePairClosestPoints(
      S2Point a0, S2Point a1, S2Point b0, S2Point b1) {

    S2Shape.MutableEdge resultEdge = new S2Shape.MutableEdge();
    S2EdgeUtil.getEdgePairClosestPoints(a0, a1, b0, b1, resultEdge);
    return Pair.of(resultEdge.getStart(), resultEdge.getEnd());
  }
}
