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

import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.*;

/**
 * Provides optional de-dup logic. Due to the nature of spatial partitioning, the same pair of
 * geometries may appear in multiple partitions. If that pair satisfies join condition, it will be
 * included in join results multiple times. This duplication can be avoided by (1) choosing spatial
 * partitioning that doesn't allow for overlapping partition extents and (2) reporting a pair of
 * matching geometries only from the partition whose extent contains the reference point of the
 * intersection of the geometries.
 *
 * @param <U>
 * @param <T>
 */
public class DuplicatesFilter<U extends Geometry, T extends Geometry>
    implements Function2<Integer, Iterator<Pair<U, T>>, Iterator<Pair<U, T>>> {

  private static final Logger log = LogManager.getLogger(DuplicatesFilter.class);
  private final Broadcast<DedupParams> dedupParamsBroadcast;

  public DuplicatesFilter(Broadcast<DedupParams> dedupParamsBroadcast) {
    this.dedupParamsBroadcast = dedupParamsBroadcast;
  }

  @Override
  public Iterator<Pair<U, T>> call(Integer partitionId, Iterator<Pair<U, T>> geometryPair)
      throws Exception {
    final List<Envelope> partitionExtents = dedupParamsBroadcast.getValue().getPartitionExtents();
    if (partitionId < partitionExtents.size()) {
      HalfOpenRectangle extent = new HalfOpenRectangle(partitionExtents.get(partitionId));
      return new FilterIterator(
          geometryPair,
          p ->
              !GeomUtils.isDuplicate(
                  ((Pair<U, T>) p).getLeft(), ((Pair<U, T>) p).getRight(), extent));
    } else {
      log.warn("Didn't find partition extent for this partition: " + partitionId);
      return geometryPair;
    }
  }
}
