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

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * The SpatialPartitioner may contain a large number of grids, which may make the serialized tasks
 * to be larger than 1MB and trigger a warning: "WARN DAGScheduler: Broadcasting large task binary
 * with size XXXX KB". This class is a wrapper around a SpatialPartitioner that is broadcasted to
 * reduce the size of serialized tasks.
 */
public class BroadcastedSpatialPartitioner extends SpatialPartitioner {
  private final Broadcast<SpatialPartitioner> bPartitioner;
  private transient SpatialPartitioner partitioner;

  public BroadcastedSpatialPartitioner(Broadcast<SpatialPartitioner> partitioner) {
    super(partitioner.value().gridType, null);
    this.bPartitioner = partitioner;
    this.partitioner = null;
  }

  private SpatialPartitioner getPartitioner() {
    if (partitioner == null) {
      partitioner = bPartitioner.value();
    }
    return partitioner;
  }

  @Override
  public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
      throws Exception {
    return getPartitioner().placeObject(spatialObject);
  }

  @Nullable
  @Override
  public DedupParams getDedupParams() {
    return getPartitioner().getDedupParams();
  }

  @Override
  public List<Envelope> getGrids() {
    return getPartitioner().getGrids();
  }

  @Override
  public int numPartitions() {
    return getPartitioner().numPartitions();
  }
}
