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
package org.apache.sedona.core.spatialPartitioning;

import static org.apache.sedona.core.formatMapper.shapefileParser.ShapefileRDD.geometryFactory;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.sedona.core.knnJudgement.EuclideanItemDistance;
import org.apache.sedona.core.spatialPartitioning.quadtree.QuadRectangle;
import org.apache.sedona.core.utils.SedonaConf;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * The class is used to build an R-tree over a random sample of another dataset and uses distance
 * bounds to ensure efficient local kNN joins.
 *
 * <p>By calculating distance bounds and using circle range queries, it ensures that the subsets Si,
 * containing all necessary points for accurate kNN results. The final union of local join results
 * provides the complete kNN join result for the datasets R and S.
 *
 * <p>It generates List<List<Integer>> expandedPartitionedBoundaries based on the quad tree.
 */
public class QuadTreeRTPartitioning extends QuadtreePartitioning {
  static final Logger log = Logger.getLogger(QuadTreeRTPartitioning.class);

  private SedonaConf sedonaConf;

  // A query-only R-tree created using the Sort-Tile-Recursive (STR) algorithm.
  private STRtree strTree;
  // The expanded partitioned boundaries based on the quad tree
  private HashMap<Integer, List<Envelope>> mbrs;
  // The spatial index for partitioned MBRs
  private STRtree mbrSpatialIndex;

  public QuadTreeRTPartitioning(List<Envelope> samples, Envelope boundary, int partitions)
      throws Exception {
    super(samples, boundary, partitions);
  }

  public QuadTreeRTPartitioning(
      List<Envelope> samples, Envelope boundary, int partitions, int minTreeLevel)
      throws Exception {
    super(samples, boundary, partitions, minTreeLevel);
  }

  public HashMap<Integer, List<Envelope>> getMbrs() {
    return mbrs;
  }

  public STRtree getMbrSpatialIndex() {
    return mbrSpatialIndex;
  }

  /**
   * This function is used to build the STR tree from the quad-tree built from the samples. It is
   * used to expand the partitioned boundaries.
   *
   * @param samples the samples
   * @param k the number of neighbor samples
   * @return
   */
  public STRtree buildSTRTree(List<Envelope> samples, int k) {
    // The partitioned MBRs
    mbrs = new HashMap<>();

    // A query-only R-tree created using the Sort-Tile-Recursive (STR) algorithm.
    strTree = new STRtree();

    // Get all MBRs (partitions) from the quad-tree
    // The zones might include the one with null partition ids
    List<QuadRectangle> partitionMBRs =
        partitionTree.getAllZones().stream()
            .filter(quadRect -> quadRect.partitionId != null)
            .collect(Collectors.toList());

    for (QuadRectangle quadRect : partitionMBRs) {
      Envelope mbr = quadRect.getEnvelope();
      strTree.insert(mbr, mbr);
    }

    // Insert samples into an STR tree for k-nearest neighbor search
    STRtree sampleTree = new STRtree();
    for (Envelope sample : samples) {
      // convert sample to a point
      Point point =
          geometryFactory.createPoint(
              new Coordinate(sample.centre().getX(), sample.centre().getY()));
      sampleTree.insert(sample, point);
    }

    processPartitions(partitionMBRs, mbrs, k, sampleTree, geometryFactory);

    // Construct a spatial index for the MBRs
    this.mbrSpatialIndex = new STRtree();
    for (Integer id : mbrs.keySet()) {
      for (Envelope envelope : mbrs.get(id)) {
        mbrSpatialIndex.insert(envelope, id);
      }
    }

    // Return the STR tree
    return strTree;
  }

  public void processPartitions(
      List<QuadRectangle> partitionMBRs,
      Map<Integer, List<Envelope>> mbrs,
      int k,
      STRtree sampleTree,
      GeometryFactory geometryFactory) {

    for (QuadRectangle quadRect : partitionMBRs) {
      processPartition(partitionMBRs, quadRect, mbrs, k, sampleTree, geometryFactory);
    }
  }

  private void processPartition(
      List<QuadRectangle> partitionMBRs,
      QuadRectangle quadRect,
      Map<Integer, List<Envelope>> mbrs,
      int k,
      STRtree sampleTree,
      GeometryFactory geometryFactory) {

    Envelope partitionMBR = quadRect.getEnvelope();

    // Calculate the centroid of each MBR in the STR tree
    double centroidX = (partitionMBR.getMinX() + partitionMBR.getMaxX()) / 2.0;
    double centroidY = (partitionMBR.getMinY() + partitionMBR.getMaxY()) / 2.0;
    Coordinate centroidCoord = new Coordinate(centroidX, centroidY);
    Point centroid = geometryFactory.createPoint(centroidCoord);

    // Compute the maximum distance ui from the centroid to any point inside the partition
    double ui = getUi(centroid, partitionMBR);

    // Calculate the maximum distance from the centroid to the k-nearest neighbors in the samples
    double maxDistance = getMaxDistanceFromSamples(k, sampleTree, centroid);
    List<Envelope> intersectingMBRs =
        getMBRIntersectEnvelopes(ui, maxDistance, centroidX, centroidY);

    mbrs.put(quadRect.partitionId, intersectingMBRs);
  }

  /**
   * This function is used to calculate the minimal envelope width of the partitioned MBRs.
   *
   * @param partitionMBRs
   * @return
   */
  public double getMinimalEnvelopeWidth(List<QuadRectangle> partitionMBRs) {
    double minEnvelopeWidth = Double.MAX_VALUE;

    for (QuadRectangle quadRect : partitionMBRs) {
      Envelope partitionMBR = quadRect.getEnvelope();

      // Calculate the width and height of the envelope
      double width = partitionMBR.getMaxX() - partitionMBR.getMinX();

      // Update the minimal envelope length if the current one is smaller
      if (width < minEnvelopeWidth) {
        minEnvelopeWidth = width;
      }
    }

    return minEnvelopeWidth;
  }

  /**
   * This function is used to calculate the maximum distance from the centroid to the k-nearest
   * neighbors in the samples. It is used to expand the partitioned boundaries.
   *
   * @param centroid
   * @param partitionMBR
   * @return
   */
  private static double getUi(Point centroid, Envelope partitionMBR) {
    double ui =
        Math.max(
            centroid.distance(
                geometryFactory.createPoint(
                    new Coordinate(partitionMBR.getMinX(), partitionMBR.getMinY()))),
            Math.max(
                centroid.distance(
                    geometryFactory.createPoint(
                        new Coordinate(partitionMBR.getMinX(), partitionMBR.getMaxY()))),
                Math.max(
                    centroid.distance(
                        geometryFactory.createPoint(
                            new Coordinate(partitionMBR.getMaxX(), partitionMBR.getMinY()))),
                    centroid.distance(
                        geometryFactory.createPoint(
                            new Coordinate(partitionMBR.getMaxX(), partitionMBR.getMaxY()))))));
    return ui;
  }

  /**
   * This function is used to calculate the maximum distance from the centroid to the k-nearest
   * neighbors in the samples. It is used to expand the partitioned boundaries.
   *
   * @param k
   * @param sampleTree
   * @param centroid
   * @return
   */
  private static double getMaxDistanceFromSamples(int k, STRtree sampleTree, Point centroid) {
    // 3 - Find the k-nearest neighbors in the samples of the centroid in the STR tree
    Object[] kNearestNeighbors =
        sampleTree.nearestNeighbour(
            centroid.getEnvelopeInternal(), centroid, new EuclideanItemDistance(), k);

    // 4 - Calculate the distance to the farthest neighbor
    double maxDistance = 0;
    for (Object neighbor : kNearestNeighbors) {
      if (neighbor instanceof Geometry) {
        Envelope neighborEnvelope = ((Geometry) neighbor).getEnvelopeInternal();
        Coordinate neighborCoord =
            new Coordinate(neighborEnvelope.centre().getX(), neighborEnvelope.centre().getY());
        Point neighborPoint = geometryFactory.createPoint(neighborCoord);
        double distance = centroid.distance(neighborPoint);
        if (distance > maxDistance) {
          maxDistance = distance;
        }
      }
    }
    return maxDistance;
  }

  /**
   * This function is used to get the MBRs that intersect with the circle constructed around the
   * centroid. It is used to expand the partitioned boundaries. If the number of intersecting MBRs
   * is too large, we optimize by considering all vertices of the MBRs to construct the circle. This
   * approach eliminates the need to add an additional margin (ui) to the maxDistance.
   *
   * @param ui
   * @param maxDistance
   * @param centroidX
   * @param centroidY
   * @return
   */
  private List<Envelope> getMBRIntersectEnvelopes(
      double ui, double maxDistance, double centroidX, double centroidY) {
    // 5 - Construct the circle with radius ui and center centroid
    // Calculate the radius of the circle
    double gamma_i = 2 * ui + maxDistance;
    // Since we're working with rectangles, this would be an envelope that fully contains the
    // circle
    Envelope circleEnvelope =
        new Envelope(
            centroidX - gamma_i, centroidX + gamma_i,
            centroidY - gamma_i, centroidY + gamma_i);

    Coordinate center = new Coordinate(centroidX, centroidY);
    Geometry circle = geometryFactory.createPoint(center).buffer(gamma_i);

    // 6 - Compute all the MBRs that intersect with the circle and add them to a hash map
    List<Envelope> candidateEnvelopes = strTree.query(circleEnvelope);

    // Filter the candidate envelopes to find those that intersect with the circle
    List<Envelope> intersectingMBRs = new ArrayList<>();
    for (Envelope candidateEnvelope : candidateEnvelopes) {
      Geometry envelopeGeometry = geometryFactory.toGeometry(candidateEnvelope);
      if (circle.intersects(envelopeGeometry)) {
        intersectingMBRs.add(candidateEnvelope);
      }
    }
    return intersectingMBRs;
  }
}
