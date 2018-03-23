/*
 * FILE: FlatGridPartitioner
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.joinJudgement.DedupParams;
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
