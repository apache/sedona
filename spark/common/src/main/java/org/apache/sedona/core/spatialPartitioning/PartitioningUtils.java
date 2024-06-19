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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.sedona.core.enums.GridType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public abstract class PartitioningUtils {
  // Check the geom against the partition tree to find the ids of overlapping grids
  public abstract Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry);
  // Check the geom against the partition tree to find the ids of overlapping grids. Only return IDs
  public abstract Set<Integer> getKeys(Geometry geometry);
  // Traverse the partition tree and fetch the grids
  public abstract List<Envelope> fetchLeafZones();

  public static PartitioningUtils getPartitioner(
      List<Geometry> samples, GridType gridType, Envelope boundaryEnvelope, int resolution) {
    List<Envelope> sampleEnvelopes = new ArrayList<Envelope>();
    for (Geometry geom : samples) {
      sampleEnvelopes.add(geom.getEnvelopeInternal());
    }
    // Add some padding at the top and right of the boundaryEnvelope to make
    // sure all geometries lie within the half-open rectangle.
    final Envelope paddedBoundary =
        new Envelope(
            boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
            boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);
    PartitioningUtils partitioner = null;
    switch (gridType) {
      case EQUALGRID:
        {
          // Force the quad-tree to grow up to a certain level
          // So the actual num of partitions might be slightly different
          int minLevel = (int) Math.max(Math.log(resolution) / Math.log(4), 0);
          QuadtreePartitioning quadtreePartitioning = null;
          try {
            quadtreePartitioning =
                new QuadtreePartitioning(new ArrayList<>(), paddedBoundary, resolution, minLevel);
          } catch (Exception e) {
            e.printStackTrace();
          }
          partitioner = quadtreePartitioning.getPartitionTree();
          break;
        }
      case QUADTREE:
        {
          QuadtreePartitioning quadtreePartitioning = null;
          try {
            quadtreePartitioning =
                new QuadtreePartitioning(sampleEnvelopes, paddedBoundary, resolution);
          } catch (Exception e) {
            e.printStackTrace();
          }
          partitioner = quadtreePartitioning.getPartitionTree();
          break;
        }
      case KDBTREE:
        {
          final KDB tree = new KDB(sampleEnvelopes.size() / resolution, resolution, paddedBoundary);
          for (final Envelope sample : sampleEnvelopes) {
            tree.insert(sample);
          }
          tree.assignLeafIds();
          partitioner = tree;
          break;
        }
      default:
        try {
          throw new Exception(
              "[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method. "
                  + "The following partitioning methods are not longer supported: R-Tree, Hilbert curve, Voronoi");
        } catch (Exception e) {
          e.printStackTrace();
          break;
        }
    }
    return partitioner;
  }

  /**
   * If the user only provides boundary and numPartitions, use equal grids.
   *
   * @param boundaryEnvelope
   * @param resolution
   * @return
   */
  public static PartitioningUtils getPartitioner(Envelope boundaryEnvelope, int resolution) {
    return getPartitioner(new ArrayList<>(), GridType.EQUALGRID, boundaryEnvelope, resolution);
  }
}
