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
import java.util.Map;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.quadtree.ExtendedQuadTree;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

/**
 * This class implements spatial partitioner based on the principles outlined in:
 *
 * <p>It uses the Quad-tree partitioning strategy to create balanced partitions This partitioning is
 * essential for efficiently executing kNN joins. The process involves the following steps:
 *
 * <p>1. **Quad-tree Partitioning**: - Data is partitioned into partitions using the Quad-tree data
 * structure. - This ensures balanced partitions and spatial locality preservation.
 *
 * <p>2. **Global R-Tree Index Construction**: - A set of random samples S' from the dataset S is
 * taken. - An R-tree T is built over S' in the master node (driver program).
 *
 * <p>3. **Distance Bound Calculation**: - For each partition Ri, the distance ui from the furthest
 * point in Ri to the centroid cri is calculated. - k-nearest neighbors of each centroid cri are
 * found using the R-tree T. - A distance bound γi is derived for each Ri, defined as γi = 2ui +
 * |cri, sk|, where sk is the k-th nearest neighbor of cri.
 *
 * <p>4. **Partitioning Neighbors**: - For each partition Ri, a subset Si ⊂ S is identified such
 * that for any r ∈ Ri, knn(r, S) = knn(r, Si) using a circle range query centered at cri with
 * radius γi. - This guarantees that the k-nearest neighbors of any point in Ri can be found within
 * the subset Si.
 *
 * <p>5. **Parallel Local kNN Joins**: - Each combined partition (Ri, Si) is processed in parallel.
 * - An R-tree is built over Si, and a local kNN join is performed for each record in Ri. - The
 * results from all partitions are combined to produce the final kNN join results.
 *
 * <p>Reference: Xie, Dong, et al. "Simba: Efficient in-memory spatial analytics." In Proceedings of
 * the 2016 ACM SIGMOD International Conference on Management of Data (SIGMOD '16), 2016, DOI:
 * 10.1145/2882903.2915237.
 */
public class QuadTreeRTPartitioner extends QuadTreePartitioner {

  // ExtendedQuadTree is used to generate the expanded boundaries.
  ExtendedQuadTree<?> extendedQuadTree;

  public QuadTreeRTPartitioner(ExtendedQuadTree<?> extendedQuadTree) {
    super(extendedQuadTree.getQuadTree());
    this.extendedQuadTree = extendedQuadTree;
  }

  public QuadTreeRTPartitioner nonOverlappedPartitioner() {
    ExtendedQuadTree<Integer> nonOverlappedTree = new ExtendedQuadTree<>(extendedQuadTree, true);
    return new QuadTreeRTPartitioner(nonOverlappedTree);
  }

  public Envelope getBoundary() {
    return extendedQuadTree.getBoundary();
  }

  /**
   * Depending on overlappedPartitioner, return the expanded boundaries or the original boundaries.
   *
   * @param spatialObject the spatial object
   * @return Iterator<Tuple2<Integer, Geometry>>
   * @throws Exception
   */
  @Override
  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry spatialObject) throws Exception {
    return extendedQuadTree.placeObject(spatialObject);
  }

  @Override
  public DedupParams getDedupParams() {
    return super.getDedupParams();
  }

  @Override
  public List<Envelope> getGrids() {
    return super.getGrids();
  }

  @Override
  public int numPartitions() {
    return super.numPartitions();
  }

  public Map<Integer, List<Envelope>> getOverlappedGrids() {
    return extendedQuadTree.getExpandedBoundaries();
  }

  public STRtree getSTRForOverlappedGrids() {
    return extendedQuadTree.getSpatialExpandedBoundaryIndex();
  }
}
