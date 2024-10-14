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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

// TODO: Auto-generated Javadoc

/** The Class EqualPartitioning. */
public class EqualPartitioning implements Serializable {

  /** The grids. */
  List<Envelope> grids = new ArrayList<Envelope>();

  public EqualPartitioning(List<Envelope> grids) {
    this.grids = grids;
  }
  /**
   * Instantiates a new equal partitioning.
   *
   * @param boundary the boundary
   * @param partitions the partitions
   */
  public EqualPartitioning(Envelope boundary, int partitions) {
    // Local variable should be declared here
    Double root = Math.sqrt(partitions);
    int partitionsAxis;
    double intervalX;
    double intervalY;

    // Calculate how many bounds should be on each axis
    partitionsAxis = root.intValue();
    intervalX = (boundary.getMaxX() - boundary.getMinX()) / partitionsAxis;
    intervalY = (boundary.getMaxY() - boundary.getMinY()) / partitionsAxis;
    // System.out.println("Boundary: "+boundary+"root: "+root+" interval:
    // "+intervalX+","+intervalY);
    for (int i = 0; i < partitionsAxis; i++) {
      for (int j = 0; j < partitionsAxis; j++) {
        Envelope grid =
            new Envelope(
                boundary.getMinX() + intervalX * i,
                boundary.getMinX() + intervalX * (i + 1),
                boundary.getMinY() + intervalY * j,
                boundary.getMinY() + intervalY * (j + 1));
        // System.out.println("Grid: "+grid);
        grids.add(grid);
      }
      // System.out.println("Finish one column/one certain x");
    }
  }

  /**
   * Gets the grids.
   *
   * @return the grids
   */
  public List<Envelope> getGrids() {

    return this.grids;
  }

  public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry) {
    Objects.requireNonNull(geometry, "spatialObject");

    // Some grid types (RTree and Voronoi) don't provide full coverage of the RDD extent and
    // require an overflow container.
    final int overflowContainerID = grids.size();

    final Envelope envelope = geometry.getEnvelopeInternal();

    Set<Tuple2<Integer, Geometry>> result = new HashSet();
    boolean containFlag = false;
    for (int i = 0; i < grids.size(); i++) {
      final Envelope grid = grids.get(i);
      if (grid.covers(envelope)) {
        result.add(new Tuple2(i, geometry));
        containFlag = true;
      } else if (grid.intersects(envelope) || envelope.covers(grid)) {
        result.add(new Tuple2<>(i, geometry));
      }
    }

    if (!containFlag) {
      result.add(new Tuple2<>(overflowContainerID, geometry));
    }

    return result.iterator();
  }

  public Set<Integer> getKeys(Geometry geometry) {
    Objects.requireNonNull(geometry, "spatialObject");

    // Some grid types (RTree and Voronoi) don't provide full coverage of the RDD extent and
    // require an overflow container.
    final int overflowContainerID = grids.size();

    final Envelope envelope = geometry.getEnvelopeInternal();

    Set<Integer> result = new HashSet();
    boolean containFlag = false;
    for (int i = 0; i < grids.size(); i++) {
      final Envelope grid = grids.get(i);
      if (grid.covers(envelope)) {
        result.add(i);
        containFlag = true;
      } else if (grid.intersects(envelope) || envelope.covers(grid)) {
        result.add(i);
      }
    }

    if (!containFlag) {
      result.add(overflowContainerID);
    }
    return result;
  }
}
