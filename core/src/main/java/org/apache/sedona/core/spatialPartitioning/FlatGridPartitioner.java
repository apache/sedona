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

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FlatGridPartitioner
        extends SpatialPartitioner
{
    public FlatGridPartitioner(GridType gridType, List<Envelope> grids)
    {
        super(gridType, grids);
    }

    // For backwards compatibility (see SpatialRDD.spatialPartitioning(otherGrids))
    public FlatGridPartitioner(List<Envelope> grids)
    {
        super(null, grids);
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
            throws Exception
    {
        Objects.requireNonNull(spatialObject, "spatialObject");

        // Some grid types (RTree and Voronoi) don't provide full coverage of the RDD extent and
        // require an overflow container.
        final int overflowContainerID = grids.size();

        final Envelope envelope = spatialObject.getEnvelopeInternal();

        Set<Tuple2<Integer, T>> result = new HashSet();
        boolean containFlag = false;
        for (int i = 0; i < grids.size(); i++) {
            final Envelope grid = grids.get(i);
            if (grid.covers(envelope)) {
                result.add(new Tuple2(i, spatialObject));
                containFlag = true;
            }
            else if (grid.intersects(envelope) || envelope.covers(grid)) {
                result.add(new Tuple2<>(i, spatialObject));
            }
        }

        if (!containFlag) {
            result.add(new Tuple2<>(overflowContainerID, spatialObject));
        }

        return result.iterator();
    }

    @Nullable
    public DedupParams getDedupParams()
    {
        /**
         * Equal and Hilbert partitioning methods have necessary properties to support de-dup.
         * These methods provide non-overlapping partition extents and not require overflow
         * partition as they cover full extent of the RDD. However, legacy
         * SpatialRDD.spatialPartitioning(otherGrids) method doesn't preserve the grid type
         * making it impossible to reliably detect whether partitioning allows efficient de-dup or not.
         *
         * TODO Figure out how to remove SpatialRDD.spatialPartitioning(otherGrids) API. Perhaps,
         * make the implementation no-op and fold the logic into JoinQuery, RangeQuery and KNNQuery APIs.
         */

        return null;
    }

    @Override
    public int numPartitions()
    {
        return grids.size() + 1 /* overflow partition */;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof FlatGridPartitioner)) {
            return false;
        }

        final FlatGridPartitioner other = (FlatGridPartitioner) o;

        // For backwards compatibility (see SpatialRDD.spatialPartitioning(otherGrids))
        if (this.gridType == null || other.gridType == null) {
            return other.grids.equals(this.grids);
        }

        return other.gridType.equals(this.gridType) && other.grids.equals(this.grids);
    }
}
