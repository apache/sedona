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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.wrapper.UniqueGeometry;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemDistance;
import org.locationtech.jts.index.strtree.STRtree;

public class InMemoryKNNJoinIterator<T extends Geometry, U extends Geometry>
    implements Iterator<Pair<T, U>> {
  private final Iterator<T> querySideIterator;
  private final STRtree strTree;

  private final int k;
  private final DistanceMetric distanceMetric;
  private final boolean includeTies;
  private final boolean exclusive;
  private final boolean queryLocalTieSemantics;
  private final ItemDistance itemDistance;

  private final LongAccumulator streamCount;
  private final LongAccumulator resultCount;

  private final List<Pair<T, U>> currentResults = new ArrayList<>();
  private int currentResultIndex = 0;

  public InMemoryKNNJoinIterator(
      Iterator<T> querySideIterator,
      STRtree strTree,
      int k,
      DistanceMetric distanceMetric,
      boolean includeTies,
      LongAccumulator streamCount,
      LongAccumulator resultCount) {
    this(
        querySideIterator,
        strTree,
        k,
        distanceMetric,
        includeTies,
        false,
        streamCount,
        resultCount);
  }

  public InMemoryKNNJoinIterator(
      Iterator<T> querySideIterator,
      STRtree strTree,
      int k,
      DistanceMetric distanceMetric,
      boolean includeTies,
      boolean exclusive,
      LongAccumulator streamCount,
      LongAccumulator resultCount) {
    this(
        querySideIterator,
        strTree,
        k,
        distanceMetric,
        includeTies,
        exclusive,
        false,
        streamCount,
        resultCount);
  }

  public InMemoryKNNJoinIterator(
      Iterator<T> querySideIterator,
      STRtree strTree,
      int k,
      DistanceMetric distanceMetric,
      boolean includeTies,
      boolean exclusive,
      boolean queryLocalTieSemantics,
      LongAccumulator streamCount,
      LongAccumulator resultCount) {
    this.querySideIterator = querySideIterator;
    this.strTree = strTree;

    this.k = k;
    this.distanceMetric = distanceMetric;
    this.includeTies = includeTies;
    this.exclusive = exclusive;
    this.queryLocalTieSemantics = queryLocalTieSemantics;
    this.itemDistance = KnnJoinIndexJudgement.getItemDistance(distanceMetric, exclusive);

    this.streamCount = streamCount;
    this.resultCount = resultCount;
  }

  @Override
  public boolean hasNext() {
    if (currentResultIndex < currentResults.size()) {
      return true;
    }

    currentResultIndex = 0;
    currentResults.clear();
    while (querySideIterator.hasNext()) {
      populateNextBatch();
      if (!currentResults.isEmpty()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Pair<T, U> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return currentResults.get(currentResultIndex++);
  }

  private void populateNextBatch() {
    T queryItem = querySideIterator.next();
    Geometry queryGeom;
    if (queryItem instanceof UniqueGeometry) {
      queryGeom = (Geometry) ((UniqueGeometry<?>) queryItem).getOriginalGeometry();
    } else {
      queryGeom = queryItem;
    }
    streamCount.add(1);

    Object[] localK =
        strTree.nearestNeighbour(queryGeom.getEnvelopeInternal(), queryGeom, itemDistance, k);
    List<U> nearest = new ArrayList<>();
    for (Object obj : localK) {
      U candidate = (U) obj;
      if (!exclusive || !KnnJoinIndexJudgement.areTopologicallyEqual(queryGeom, candidate)) {
        nearest.add(candidate);
      }
    }
    if (includeTies) {
      nearest = getUpdatedLocalKWithTies(queryGeom, nearest, strTree);
    }

    for (U candidate : nearest) {
      Pair<T, U> pair = Pair.of(queryItem, candidate);
      currentResults.add(pair);
      resultCount.add(1);
    }
  }

  private List<U> getUpdatedLocalKWithTies(Geometry streamShape, List<U> localK, STRtree strTree) {
    if (localK.isEmpty()) {
      return localK;
    }

    Envelope searchEnvelope = streamShape.getEnvelopeInternal();
    // get the maximum distance from the k nearest neighbors
    double maxDistance = 0.0;
    Set<U> uniqueCandidates =
        queryLocalTieSemantics
            ? Collections.newSetFromMap(new IdentityHashMap<>())
            : new LinkedHashSet<>();
    for (U candidate : localK) {
      uniqueCandidates.add(candidate);
      double distance = tieDistance(streamShape, candidate);
      if (distance > maxDistance) {
        maxDistance = distance;
      }
    }
    searchEnvelope.expandBy(maxDistance);
    List<U> candidates = strTree.query(searchEnvelope);
    if (!candidates.isEmpty()) {
      // update localK with all candidates that are within the maxDistance
      List<U> tiedResults = new ArrayList<>();
      // add all localK
      tiedResults.addAll(localK);

      for (U candidate : candidates) {
        if (exclusive && KnnJoinIndexJudgement.areTopologicallyEqual(streamShape, candidate)) {
          continue;
        }
        double distance = tieDistance(streamShape, candidate);
        boolean isNewCandidate =
            queryLocalTieSemantics
                ? uniqueCandidates.add(candidate)
                : !uniqueCandidates.contains(candidate);
        if (distance == maxDistance && isNewCandidate) {
          tiedResults.add(candidate);
        }
      }
      localK = tiedResults;
    }
    return localK;
  }

  private double tieDistance(Geometry query, Geometry candidate) {
    if (queryLocalTieSemantics) {
      return KnnJoinIndexJudgement.distanceByMetric(query, candidate, distanceMetric);
    }
    return query.distance(candidate);
  }
}
