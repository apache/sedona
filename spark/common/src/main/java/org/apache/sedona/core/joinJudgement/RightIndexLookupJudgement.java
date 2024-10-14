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
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;

public class RightIndexLookupJudgement<T extends Geometry, U extends Geometry>
    extends JudgementBase<T, U>
    implements FlatMapFunction2<Iterator<T>, Iterator<SpatialIndex>, Pair<U, T>>, Serializable {

  /** @see JudgementBase */
  public RightIndexLookupJudgement(
      SpatialPredicate spatialPredicate,
      LongAccumulator buildCount,
      LongAccumulator streamCount,
      LongAccumulator resultCount,
      LongAccumulator candidateCount) {
    super(spatialPredicate, buildCount, streamCount, resultCount, candidateCount);
  }

  @Override
  public Iterator<Pair<U, T>> call(Iterator<T> streamShapes, Iterator<SpatialIndex> indexIterator)
      throws Exception {
    if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
      buildCount.add(0);
      streamCount.add(0);
      resultCount.add(0);
      candidateCount.add(0);
      return Collections.emptyIterator();
    }

    final boolean buildLeft = false;

    initPartition();

    SpatialIndex spatialIndex = indexIterator.next();

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
}
