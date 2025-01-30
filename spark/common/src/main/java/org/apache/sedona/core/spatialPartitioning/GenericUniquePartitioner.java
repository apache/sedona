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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public class GenericUniquePartitioner extends SpatialPartitioner {
  private SpatialPartitioner parent;

  public GenericUniquePartitioner(SpatialPartitioner parent) {
    this.parent = parent;
  }

  public GridType getGridType() {
    return parent.gridType;
  }

  public List<Envelope> getGrids() {
    return parent.grids;
  }

  @Override
  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry spatialObject) throws Exception {
    Iterator<Tuple2<Integer, Geometry>> it = parent.placeObject(spatialObject);
    int minParitionId = Integer.MAX_VALUE;
    Geometry minGeometry = null;
    while (it.hasNext()) {
      Tuple2<Integer, Geometry> value = it.next();
      if (value._1() < minParitionId) {
        minParitionId = value._1();
        minGeometry = value._2();
      }
    }

    HashSet<Tuple2<Integer, Geometry>> out = new HashSet<Tuple2<Integer, Geometry>>();
    if (minGeometry != null) {
      out.add(new Tuple2<Integer, Geometry>(minParitionId, minGeometry));
    }

    return out.iterator();
  }

  @Override
  @Nullable
  public DedupParams getDedupParams() {
    throw new UnsupportedOperationException("Unique partitioner cannot deduplicate join results");
  }

  @Override
  public int numPartitions() {
    return parent.numPartitions();
  }
}
