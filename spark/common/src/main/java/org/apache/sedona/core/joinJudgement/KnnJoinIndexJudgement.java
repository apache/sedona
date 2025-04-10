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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.knnJudgement.EuclideanItemDistance;
import org.apache.sedona.core.knnJudgement.HaversineItemDistance;
import org.apache.sedona.core.knnJudgement.SpheroidDistance;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
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
    implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<T, U>>, Serializable {
  private final int k;
  private final DistanceMetric distanceMetric;
  private final boolean includeTies;
  private final Broadcast<List<T>> broadcastQueryObjects;
  private final Broadcast<STRtree> broadcastObjectsTreeIndex;

  /**
   * Constructor for the KnnJoinIndexJudgement class.
   *
   * @param k the number of nearest neighbors to find
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
      DistanceMetric distanceMetric,
      boolean includeTies,
      Broadcast<List<T>> broadcastQueryObjects,
      Broadcast<STRtree> broadcastObjectsTreeIndex,
      LongAccumulator buildCount,
      LongAccumulator streamCount,
      LongAccumulator resultCount,
      LongAccumulator candidateCount) {
    super(null, buildCount, streamCount, resultCount, candidateCount);
    this.k = k;
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
   * @param queryShapes iterator over the geometries in the query side
   * @param objectShapes iterator over the geometries in the object side
   * @return an iterator over the join results
   * @throws Exception if the spatial index is not of type STRtree
   */
  @Override
  public Iterator<Pair<T, U>> call(Iterator<T> queryShapes, Iterator<U> objectShapes)
      throws Exception {
    if (!objectShapes.hasNext() || (queryShapes != null && !queryShapes.hasNext())) {
      buildCount.add(0);
      streamCount.add(0);
      resultCount.add(0);
      candidateCount.add(0);
      return Collections.emptyIterator();
    }

    STRtree strTree = buildSTRtree(objectShapes);
    return new InMemoryKNNJoinIterator<>(
        queryShapes, strTree, k, distanceMetric, includeTies, streamCount, resultCount);
  }

  /**
   * This method performs the KNN join operation using the broadcast spatial index built using all
   * geometries in the object side.
   *
   * @param queryShapes iterator over the geometries in the query side
   * @return an iterator over the join results
   */
  public Iterator<Pair<T, U>> callUsingBroadcastObjectIndex(Iterator<T> queryShapes) {
    if (!queryShapes.hasNext()) {
      buildCount.add(0);
      streamCount.add(0);
      resultCount.add(0);
      candidateCount.add(0);
      return Collections.emptyIterator();
    }

    // There's no need to use external spatial index, since the object side is small enough to be
    // broadcasted, the STRtree built from the broadcasted object should be able to fit into memory.
    STRtree strTree = broadcastObjectsTreeIndex.getValue();
    return new InMemoryKNNJoinIterator<>(
        queryShapes, strTree, k, distanceMetric, includeTies, streamCount, resultCount);
  }

  /**
   * This method performs the KNN join operation using the broadcast query geometries.
   *
   * @param objectShapes iterator over the geometries in the object side
   * @return an iterator over the join results
   */
  public Iterator<Pair<T, U>> callUsingBroadcastQueryList(Iterator<U> objectShapes) {
    if (!objectShapes.hasNext()) {
      buildCount.add(0);
      streamCount.add(0);
      resultCount.add(0);
      candidateCount.add(0);
      return Collections.emptyIterator();
    }

    List<T> queryItems = broadcastQueryObjects.getValue();
    STRtree strTree = buildSTRtree(objectShapes);
    return new InMemoryKNNJoinIterator<>(
        queryItems.iterator(), strTree, k, distanceMetric, includeTies, streamCount, resultCount);
  }

  private STRtree buildSTRtree(Iterator<U> objectShapes) {
    STRtree strTree = new STRtree();
    while (objectShapes.hasNext()) {
      U spatialObject = objectShapes.next();
      strTree.insert(spatialObject.getEnvelopeInternal(), spatialObject);
      buildCount.add(1);
    }
    strTree.build();
    return strTree;
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

  public static ItemDistance getItemDistance(DistanceMetric distanceMetric) {
    ItemDistance itemDistance;
    itemDistance = getItemDistanceByMetric(distanceMetric);
    return itemDistance;
  }
}
