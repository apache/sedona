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
import org.apache.sedona.core.wrapper.UniqueGeometry;
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
  private final Double searchRadius;
  private final DistanceMetric distanceMetric;
  private final boolean includeTies;
  private final Broadcast<List> broadcastQueryObjects;
  private final Broadcast<STRtree> broadcastObjectsTreeIndex;

  /**
   * Constructor for the KnnJoinIndexJudgement class.
   *
   * @param k the number of nearest neighbors to find
   * @param searchRadius
   * @param distanceMetric the distance metric to use
   * @param broadcastQueryObjects the broadcast geometries on queries
   * @param broadcastObjectsTreeIndex the broadcast spatial index on objects
   * @param buildCount accumulator for the number of geometries processed from the build side
   * @param streamCount accumulator for the number of geometries processed from the stream side
   * @param resultCount accumulator for the number of join results
   * @param candidateCount accumulator for the number of candidate matches
   */
  public KnnJoinIndexJudgement(
      int k,
      Double searchRadius,
      DistanceMetric distanceMetric,
      boolean includeTies,
      Broadcast<List> broadcastQueryObjects,
      Broadcast<STRtree> broadcastObjectsTreeIndex,
      LongAccumulator buildCount,
      LongAccumulator streamCount,
      LongAccumulator resultCount,
      LongAccumulator candidateCount) {
    super(null, buildCount, streamCount, resultCount, candidateCount);
    this.k = k;
    this.searchRadius = searchRadius;
    this.distanceMetric = distanceMetric;
    this.includeTies = includeTies;
    this.broadcastQueryObjects = broadcastQueryObjects;
    this.broadcastObjectsTreeIndex = broadcastObjectsTreeIndex;
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
    if (!treeIndexes.hasNext() || (streamShapes != null && !streamShapes.hasNext())) {
      buildCount.add(0);
      streamCount.add(0);
      resultCount.add(0);
      candidateCount.add(0);
      return Collections.emptyIterator();
    }

    STRtree strTree;
    if (broadcastObjectsTreeIndex != null) {
      // get the broadcast spatial index on objects side if available
      strTree = broadcastObjectsTreeIndex.getValue();
    } else {
      // get the spatial index from the iterator
      SpatialIndex treeIndex = treeIndexes.next();
      if (!(treeIndex instanceof STRtree)) {
        throw new Exception(
            "[KnnJoinIndexJudgement][Call] Only STRtree index supports KNN search.");
      }
      strTree = (STRtree) treeIndex;
    }

    // TODO: For future improvement, instead of using a list to store the results,
    // we can use lazy evaluation to avoid storing all the results in memory.
    List<Pair<U, T>> result = new ArrayList<>();

    List queryItems;
    if (broadcastQueryObjects != null) {
      // get the broadcast spatial index on queries side if available
      queryItems = broadcastQueryObjects.getValue();
      for (Object item : queryItems) {
        T queryGeom;
        if (item instanceof UniqueGeometry) {
          queryGeom = (T) ((UniqueGeometry) item).getOriginalGeometry();
        } else {
          queryGeom = (T) item;
        }
        streamCount.add(1);

        Object[] localK =
            strTree.nearestNeighbour(
                queryGeom.getEnvelopeInternal(), queryGeom, getItemDistance(), k);
        if (includeTies) {
          localK = getUpdatedLocalKWithTies(queryGeom, localK, strTree);
        }
        if (searchRadius != null) {
          localK = getInSearchRadius(localK, queryGeom);
        }

        for (Object obj : localK) {
          T candidate = (T) obj;
          Pair<U, T> pair = Pair.of((U) item, candidate);
          result.add(pair);
          resultCount.add(1);
        }
      }
      return result.iterator();
    } else {
      while (streamShapes.hasNext()) {
        T streamShape = streamShapes.next();
        streamCount.add(1);

        Object[] localK =
            strTree.nearestNeighbour(
                streamShape.getEnvelopeInternal(), streamShape, getItemDistance(), k);
        if (includeTies) {
          localK = getUpdatedLocalKWithTies(streamShape, localK, strTree);
        }
        if (searchRadius != null) {
          localK = getInSearchRadius(localK, streamShape);
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
  }

  private Object[] getInSearchRadius(Object[] localK, T queryGeom) {
    localK =
        Arrays.stream(localK)
            .filter(
                candidate -> {
                  Geometry candidateGeom = (Geometry) candidate;
                  return distanceByMetric(queryGeom, candidateGeom, distanceMetric) <= searchRadius;
                })
            .toArray();
    return localK;
  }

  /**
   * This method calculates the distance between two geometries using the specified distance metric.
   *
   * @param queryGeom the query geometry
   * @param candidateGeom the candidate geometry
   * @param distanceMetric the distance metric to use
   * @return the distance between the two geometries
   */
  public static double distanceByMetric(
      Geometry queryGeom, Geometry candidateGeom, DistanceMetric distanceMetric) {
    switch (distanceMetric) {
      case EUCLIDEAN:
        EuclideanItemDistance euclideanItemDistance = new EuclideanItemDistance();
        return euclideanItemDistance.distance(queryGeom, candidateGeom);
      case HAVERSINE:
        HaversineItemDistance haversineItemDistance = new HaversineItemDistance();
        return haversineItemDistance.distance(queryGeom, candidateGeom);
      case SPHEROID:
        SpheroidDistance spheroidDistance = new SpheroidDistance();
        return spheroidDistance.distance(queryGeom, candidateGeom);
      default:
        return queryGeom.distance(candidateGeom);
    }
  }

  private ItemDistance getItemDistance() {
    ItemDistance itemDistance;
    itemDistance = getItemDistanceByMetric(distanceMetric);
    return itemDistance;
  }

  /**
   * This method returns the ItemDistance object based on the specified distance metric.
   *
   * @param distanceMetric the distance metric to use
   * @return the ItemDistance object
   */
  public static ItemDistance getItemDistanceByMetric(DistanceMetric distanceMetric) {
    ItemDistance itemDistance;
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
    return itemDistance;
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

  public static <U extends Geometry, T extends Geometry> double distance(
      U key, T value, DistanceMetric distanceMetric) {
    switch (distanceMetric) {
      case EUCLIDEAN:
        return new EuclideanItemDistance().distance(key, value);
      case HAVERSINE:
        return new HaversineItemDistance().distance(key, value);
      case SPHEROID:
        return new SpheroidDistance().distance(key, value);
      default:
        return new EuclideanItemDistance().distance(key, value);
    }
  }
}
