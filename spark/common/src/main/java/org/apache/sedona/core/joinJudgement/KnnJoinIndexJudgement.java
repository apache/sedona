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
package org.apache.sedona.core.joinJudgement;

import java.io.Serializable;
import java.util.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.knnJudgement.EuclideanItemDistance;
import org.apache.sedona.core.knnJudgement.HaversineItemDistance;
import org.apache.sedona.core.knnJudgement.SpheroidDistance;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.GeometryItemDistance;
import org.locationtech.jts.index.strtree.ItemDistance;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * This class is responsible for performing a K-nearest neighbors (KNN) join operation using a
 * spatial index. It extends the JudgementBase class and implements the FlatMapFunction2 interface.
 *
 * @param <T> extends Geometry - the type of geometries in the left set
 * @param <U> extends Geometry - the type of geometries in the right set
 */
public class KnnJoinIndexJudgement<T extends Geometry, U extends Geometry>
    extends JudgementBase<T, U>
    implements FlatMapFunction2<Iterator<T>, Iterator<SpatialIndex>, Pair<U, T>>, Serializable {
  private final int k;
  private final DistanceMetric distanceMetric;
  private final boolean includeTies;
  private final Broadcast<STRtree> broadcastedTreeIndex;

  /**
   * Constructor for the KnnJoinIndexJudgement class.
   *
   * @param k the number of nearest neighbors to find
   * @param distanceMetric the distance metric to use
   * @param buildCount accumulator for the number of geometries processed from the build side
   * @param streamCount accumulator for the number of geometries processed from the stream side
   * @param resultCount accumulator for the number of join results
   * @param candidateCount accumulator for the number of candidate matches
   * @param broadcastedTreeIndex the broadcasted spatial index
   */
  public KnnJoinIndexJudgement(
      int k,
      DistanceMetric distanceMetric,
      boolean includeTies,
      Broadcast<STRtree> broadcastedTreeIndex,
      LongAccumulator buildCount,
      LongAccumulator streamCount,
      LongAccumulator resultCount,
      LongAccumulator candidateCount) {
    super(null, buildCount, streamCount, resultCount, candidateCount);
    this.k = k;
    this.distanceMetric = distanceMetric;
    this.includeTies = includeTies;
    this.broadcastedTreeIndex = broadcastedTreeIndex;
  }

  /**
   * This method performs the KNN join operation. It iterates over the geometries in the stream side
   * and uses the spatial index to find the k nearest neighbors for each geometry. The method
   * returns an iterator over the join results.
   *
   * @param streamShapes iterator over the geometries in the stream side
   * @param treeIndexes iterator over the spatial indexes
   * @return an iterator over the join results
   * @throws Exception if the spatial index is not of type STRtree
   */
  @Override
  public Iterator<Pair<U, T>> call(Iterator<T> streamShapes, Iterator<SpatialIndex> treeIndexes)
      throws Exception {
    if (!treeIndexes.hasNext() || !streamShapes.hasNext()) {
      buildCount.add(0);
      streamCount.add(0);
      resultCount.add(0);
      candidateCount.add(0);
      return Collections.emptyIterator();
    }

    STRtree strTree;
    if (broadcastedTreeIndex != null) {
      // get the broadcasted spatial index if available
      // this is to support the broadcast join
      strTree = broadcastedTreeIndex.getValue();
    } else {
      // get the spatial index from the iterator
      SpatialIndex treeIndex = treeIndexes.next();
      if (!(treeIndex instanceof STRtree)) {
        throw new Exception(
            "[KnnJoinIndexJudgement][Call] Only STRtree index supports KNN search.");
      }
      strTree = (STRtree) treeIndex;
    }

    List<Pair<U, T>> result = new ArrayList<>();
    ItemDistance itemDistance;

    while (streamShapes.hasNext()) {
      T streamShape = streamShapes.next();
      streamCount.add(1);

      Object[] localK;
      switch (distanceMetric) {
        case EUCLIDEAN:
          itemDistance = new EuclideanItemDistance();
          break;
        case HAVERSINE:
          itemDistance = new HaversineItemDistance();
          break;
        case SPHEROID:
          itemDistance = new SpheroidDistance();
          break;
        default:
          itemDistance = new GeometryItemDistance();
          break;
      }

      localK =
          strTree.nearestNeighbour(streamShape.getEnvelopeInternal(), streamShape, itemDistance, k);
      if (includeTies) {
        localK = getUpdatedLocalKWithTies(streamShape, localK, strTree);
      }

      for (Object obj : localK) {
        T candidate = (T) obj;
        Pair<U, T> pair = Pair.of((U) streamShape, candidate);
        result.add(pair);
        resultCount.add(1);
      }
    }

    return result.iterator();
  }

  private Object[] getUpdatedLocalKWithTies(T streamShape, Object[] localK, STRtree strTree) {
    Envelope searchEnvelope = streamShape.getEnvelopeInternal();
    // get the maximum distance from the k nearest neighbors
    double maxDistance = 0.0;
    LinkedHashSet<T> uniqueCandidates = new LinkedHashSet<>();
    for (Object obj : localK) {
      T candidate = (T) obj;
      uniqueCandidates.add(candidate);
      double distance = streamShape.distance(candidate);
      if (distance > maxDistance) {
        maxDistance = distance;
      }
    }
    searchEnvelope.expandBy(maxDistance);
    List<T> candidates = strTree.query(searchEnvelope);
    if (!candidates.isEmpty()) {
      // update localK with all candidates that are within the maxDistance
      List<Object> tiedResults = new ArrayList<>();
      // add all localK
      Collections.addAll(tiedResults, localK);

      for (T candidate : candidates) {
        double distance = streamShape.distance(candidate);
        if (distance == maxDistance && !uniqueCandidates.contains(candidate)) {
          tiedResults.add(candidate);
        }
      }
      localK = tiedResults.toArray();
    }
    return localK;
  }
}
