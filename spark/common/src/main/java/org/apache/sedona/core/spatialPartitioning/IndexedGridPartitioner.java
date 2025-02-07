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

import java.util.*;
import org.apache.sedona.core.enums.GridType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

/**
 * The IndexedGridPartitioner is used when there is already a set of grids which the data should be
 * partitioned into. It leverages an STRTree to quickly find the grids to place a geometry into. If
 * you have very few objects to place, it may make more sense to use the FlatGridPartitioner. If you
 * do not have a strict requirement to use a specific set of grids, it may make more sense to use
 * another partitioner that generates its own grids from space-partitioning tree, e.g. the
 * KDBTreePartitioner or the QuadTreePartitioner.
 */
public class IndexedGridPartitioner extends FlatGridPartitioner {
  private final STRtree index;

  public IndexedGridPartitioner(
      GridType gridType, List<Envelope> grids, Boolean preserveUncontainedGeometries) {
    super(gridType, grids, preserveUncontainedGeometries);
    this.index = new STRtree();
    for (int i = 0; i < grids.size(); i++) {
      final Envelope grid = grids.get(i);
      index.insert(grid, i);
    }
    index.build();
  }

  public IndexedGridPartitioner(GridType gridType, List<Envelope> grids) {
    this(gridType, grids, true);
  }

  public IndexedGridPartitioner(List<Envelope> grids, Boolean preserveUncontainedGeometries) {
    this(null, grids, preserveUncontainedGeometries);
  }

  public IndexedGridPartitioner(List<Envelope> grids) {
    this(null, grids);
  }

  public STRtree getIndex() {
    return index;
  }

  @Override
  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry spatialObject) throws Exception {
    List results = index.query(spatialObject.getEnvelopeInternal());
    if (preserveUncontainedGeometries) {
      // borrowed from EqualPartitioning.placeObject
      final int overflowContainerID = grids.size();
      final Envelope envelope = spatialObject.getEnvelopeInternal();

      Set<Tuple2<Integer, Geometry>> result = new HashSet();
      boolean containFlag = false;
      for (Object queryResult : results) {
        Integer i = (Integer) queryResult;
        final Envelope grid = grids.get(i);
        if (grid.covers(envelope)) {
          result.add(new Tuple2(i, spatialObject));
          containFlag = true;
        } else if (grid.intersects(envelope)) {
          result.add(new Tuple2<>(i, spatialObject));
        }
      }

      if (!containFlag) {
        result.add(new Tuple2<>(overflowContainerID, spatialObject));
      }

      return result.iterator();
    } else {
      return results.stream().map(i -> new Tuple2(i, spatialObject)).iterator();
    }
  }
}
