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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.utils.TimeUtils;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

public class DynamicIndexLookupJudgement<T extends Geometry, U extends Geometry>
    extends JudgementBase<T, U>
    implements FlatMapFunction2<Iterator<U>, Iterator<T>, Pair<U, T>>, Serializable {
  private final IndexType indexType;
  private final JoinBuildSide joinBuildSide;

  /** @see JudgementBase */
  public DynamicIndexLookupJudgement(
      SpatialPredicate spatialPredicate,
      IndexType indexType,
      JoinBuildSide joinBuildSide,
      LongAccumulator buildCount,
      LongAccumulator streamCount,
      LongAccumulator resultCount,
      LongAccumulator candidateCount) {
    super(spatialPredicate, buildCount, streamCount, resultCount, candidateCount);
    this.indexType = indexType;
    this.joinBuildSide = joinBuildSide;
  }

  @Override
  public Iterator<Pair<U, T>> call(final Iterator<U> leftShapes, final Iterator<T> rightShapes)
      throws Exception {

    if (!leftShapes.hasNext() || !rightShapes.hasNext()) {
      buildCount.add(0);
      streamCount.add(0);
      resultCount.add(0);
      candidateCount.add(0);
      return Collections.emptyIterator();
    }

    initPartition();

    final boolean buildLeft = (joinBuildSide == JoinBuildSide.LEFT);

    final Iterator<? extends Geometry> buildShapes;
    final Iterator<? extends Geometry> streamShapes;
    if (buildLeft) {
      buildShapes = leftShapes;
      streamShapes = rightShapes;
    } else {
      buildShapes = rightShapes;
      streamShapes = leftShapes;
    }

    final SpatialIndex spatialIndex = buildIndex(buildShapes);

    return new Iterator<Pair<U, T>>() {
      @Override
      public boolean hasNext() {
        return hasNextBase(spatialIndex, streamShapes, buildLeft);
      }

      @Override
      public Pair<U, T> next() {
        return nextBase(spatialIndex, streamShapes, buildLeft);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private SpatialIndex buildIndex(Iterator<? extends Geometry> geometries) {
    long startTime = System.currentTimeMillis();
    long count = 0;
    final SpatialIndex index = newIndex();
    while (geometries.hasNext()) {
      Geometry geometry = geometries.next();
      index.insert(geometry.getEnvelopeInternal(), geometry);
      count++;
    }
    index.query(new Envelope(0.0, 0.0, 0.0, 0.0));
    log("Loaded %d shapes into an index in %d ms", count, TimeUtils.elapsedSince(startTime));
    buildCount.add((int) count);
    return index;
  }

  private SpatialIndex newIndex() {
    switch (indexType) {
      case RTREE:
        return new STRtree();
      case QUADTREE:
        return new Quadtree();
      default:
        throw new IllegalArgumentException("Unsupported index type: " + indexType);
    }
  }
}
