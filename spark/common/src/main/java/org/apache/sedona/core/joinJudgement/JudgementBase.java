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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.apache.spark.TaskContext;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;

/**
 * Base class for partition level join implementations.
 *
 * <p>Provides `match` method to test whether a given pair of geometries satisfies join condition.
 *
 * <p>
 */
abstract class JudgementBase<T extends Geometry, U extends Geometry> implements Serializable {
  private static final Logger log = LogManager.getLogger(JudgementBase.class);

  private final SpatialPredicate spatialPredicate;
  private transient SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
  protected final LongAccumulator buildCount;
  protected final LongAccumulator streamCount;
  protected final LongAccumulator resultCount;
  protected final LongAccumulator candidateCount;

  private int shapeCnt;

  // A batch of pre-computed matches
  private List<Pair<U, T>> batch = null;
  // An index of the element from 'batch' to return next
  private int nextIndex = 0;

  /**
   * @param spatialPredicate spatial predicate as join condition
   * @param buildCount num of geometries in build side
   * @param streamCount num of geometries in stream side
   * @param resultCount num of join results
   * @param candidateCount num of candidate pairs to be refined by their real geometries
   */
  protected JudgementBase(
      SpatialPredicate spatialPredicate,
      LongAccumulator buildCount,
      LongAccumulator streamCount,
      LongAccumulator resultCount,
      LongAccumulator candidateCount) {
    this.spatialPredicate = spatialPredicate;
    this.buildCount = buildCount;
    this.streamCount = streamCount;
    this.resultCount = resultCount;
    this.candidateCount = candidateCount;
    this.shapeCnt = 0;
  }

  /**
   * Looks up the extent of the current partition. If found, `match` method will activate the logic
   * to avoid emitting duplicate join results from multiple partitions.
   *
   * <p>Must be called before processing a partition. Must be called from the same instance that
   * will be used to process the partition.
   */
  protected void initPartition() {
    evaluator = SpatialPredicateEvaluators.create(spatialPredicate);
  }

  private boolean match(Geometry left, Geometry right) {
    return evaluator.eval(left, right);
  }

  /**
   * Iterator model for the index-based join. It checks if there is a next match and populate it to
   * the result.
   *
   * @param spatialIndex
   * @param streamShapes
   * @param buildLeft
   * @return
   */
  protected boolean hasNextBase(
      SpatialIndex spatialIndex, Iterator<? extends Geometry> streamShapes, boolean buildLeft) {
    if (batch != null) {
      return true;
    } else {
      return populateNextBatch(spatialIndex, streamShapes, buildLeft);
    }
  }

  /**
   * Iterator model for the nest loop join. It checks if there is a next match and populate it to
   * the result.
   *
   * @param buildShapes
   * @param streamShapes
   * @return
   */
  protected boolean hasNextBase(
      List<? extends Geometry> buildShapes, Iterator<? extends Geometry> streamShapes) {
    if (batch != null) {
      return true;
    } else {
      return populateNextBatch(buildShapes, streamShapes);
    }
  }

  /**
   * Iterator model for the index-based join. It returns 1 pair in the current batch. Each batch
   * contains a list of pairs of geometries that satisfy the join condition. The current batch is
   * the result of the current stream shape against all the build shapes.
   *
   * @param spatialIndex
   * @param streamShapes
   * @param buildLeft
   * @return
   */
  protected Pair<U, T> nextBase(
      SpatialIndex spatialIndex, Iterator<? extends Geometry> streamShapes, boolean buildLeft) {
    if (batch == null) {
      populateNextBatch(spatialIndex, streamShapes, buildLeft);
    }

    if (batch != null) {
      final Pair<U, T> result = batch.get(nextIndex);
      nextIndex++;
      if (nextIndex >= batch.size()) {
        populateNextBatch(spatialIndex, streamShapes, buildLeft);
        nextIndex = 0;
      }
      return result;
    }

    throw new NoSuchElementException();
  }

  /**
   * Iterator model for the nest loop join. It returns 1 pair in the current batch. Each batch
   * contains a list of pairs of geometries that satisfy the join condition. The current batch is
   * the result of the current stream shape against all the build shapes.
   *
   * @param buildShapes
   * @param streamShapes
   * @return
   */
  protected Pair<U, T> nextBase(
      List<? extends Geometry> buildShapes, Iterator<? extends Geometry> streamShapes) {
    if (batch == null) {
      populateNextBatch(buildShapes, streamShapes);
    }

    if (batch != null) {
      final Pair<U, T> result = batch.get(nextIndex);
      nextIndex++;
      if (nextIndex >= batch.size()) {
        populateNextBatch(buildShapes, streamShapes);
        nextIndex = 0;
      }
      return result;
    }

    throw new NoSuchElementException();
  }

  /**
   * Populates the next batch of matches given the current shape in the stream side. It works as
   * follows: 1. If there is no shape left in the stream side, it returns false. 2. If there are
   * shapes left in the stream side, it uses the current shape in the stream side to query the
   * spatial index. The query result is a list of geometries in the build side that overlap with the
   * current shape in the stream side. The query result is flattened to a list of pairs of
   * geometries 3. If there are no results, it returns false.
   *
   * @param spatialIndex spatial index of the build side
   * @param streamShapes stream side geometries
   * @param buildLeft whether the build side is left
   * @return whether there is a next batch
   */
  private boolean populateNextBatch(
      SpatialIndex spatialIndex, Iterator<? extends Geometry> streamShapes, boolean buildLeft) {
    if (!streamShapes.hasNext()) {
      if (batch != null) {
        batch = null;
      }
      return false;
    }

    batch = new ArrayList<>();

    while (streamShapes.hasNext()) {
      shapeCnt++;
      streamCount.add(1);
      final Geometry streamShape = streamShapes.next();
      final List candidates = spatialIndex.query(streamShape.getEnvelopeInternal());
      for (Object candidate : candidates) {
        candidateCount.add(1);
        final Geometry buildShape = (Geometry) candidate;
        if (buildLeft) {
          if (match(buildShape, streamShape)) {
            batch.add(Pair.of((U) buildShape, (T) streamShape));
            resultCount.add(1);
          }
        } else {
          if (match(streamShape, buildShape)) {
            batch.add(Pair.of((U) streamShape, (T) buildShape));
            resultCount.add(1);
          }
        }
      }
      logMilestone(shapeCnt, 100 * 1000, "Streaming shapes");
      if (!batch.isEmpty()) {
        return true;
      }
    }

    batch = null;
    return false;
  }

  /**
   * Populates the next batch of matches given the current shape in the stream side. This is solely
   * used for nested loop join. It works as follows: 1. If there is no shape left in the stream
   * side, it returns false. 2. If there are shapes left in the stream side, it uses the current
   * shape in the stream side to query buildShapes The query result is a list of geometries in the
   * build side that overlap with the current shape in the stream side. The query result is
   * flattened to a list of pairs of geometries 3. If there are no results, it returns false.
   *
   * @param buildShapes
   * @param streamShapes
   * @return
   */
  private boolean populateNextBatch(
      List<? extends Geometry> buildShapes, Iterator<? extends Geometry> streamShapes) {
    if (!streamShapes.hasNext()) {
      if (batch != null) {
        batch = null;
      }
      return false;
    }

    batch = new ArrayList<>();

    while (streamShapes.hasNext()) {
      shapeCnt++;
      streamCount.add(1);
      final Geometry streamShape = streamShapes.next();
      for (Object candidate : buildShapes) {
        candidateCount.add(1);
        final Geometry buildShape = (Geometry) candidate;
        if (match(streamShape, buildShape)) {
          batch.add(Pair.of((U) streamShape, (T) buildShape));
          resultCount.add(1);
        }
      }
      logMilestone(shapeCnt, 100 * 1000, "Streaming shapes");
      if (!batch.isEmpty()) {
        return true;
      }
    }

    batch = null;
    return false;
  }

  protected void log(String message, Object... params) {
    if (Level.INFO.isGreaterOrEqual(log.getEffectiveLevel())) {
      final int partitionId = TaskContext.getPartitionId();
      final long threadId = Thread.currentThread().getId();
      log.info("[" + threadId + ", PID=" + partitionId + "] " + String.format(message, params));
    }
  }

  private void logMilestone(long cnt, long threshold, String name) {
    if (cnt > 1 && cnt % threshold == 1) {
      log("[%s] Reached a milestone: %d", name, cnt);
    }
  }
}
