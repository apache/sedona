/*
 * FILE: CubeGridPartitioner
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
package org.datasyslab.geospark.spatioTemporalPartitioning;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.datasyslab.geospark.SpatioTemporalObjects.SpatioTemporalObject;
import org.datasyslab.geospark.enums.SpatioTemporalGridType;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.joinJudgement.SpatioTemporalDedupParams;

import scala.Tuple2;

/**
 * The Class CubeGridPartitioner.
 */
public class CubeGridPartitioner
        extends SpatioTemporalPartitioner
{
    public CubeGridPartitioner(SpatioTemporalGridType gridType, List<Cube> grids)
    {
        super(gridType, grids);
    }


    // For backwards compatibility (see SpatialRDD.spatialPartitioning(otherGrids))
    public CubeGridPartitioner(List<Cube> grids)
    {
        super(null, grids);
    }

    @Override
    public <T extends SpatioTemporalObject> Iterator<Tuple2<Integer, T>> placeObject(T spatioTemporalObject)
            throws Exception
    {
        Objects.requireNonNull(spatioTemporalObject, "spatialObject");

        // Some grid types (RTree and Equal) don't provide full coverage of the RDD extent and
        // require an overflow container.
        final int overflowContainerID = grids.size();

        final Cube cube = spatioTemporalObject.getCubeInternal();

        final Point3D point = spatioTemporalObject instanceof Point3D ? (Point3D) spatioTemporalObject : null;

        Set<Tuple2<Integer, T>> result = new HashSet();
        boolean containFlag = false;
        for (int i = 0; i < grids.size(); i++) {
            final Cube grid = grids.get(i);
            if (grid.covers(cube)) {
                result.add(new Tuple2(i, spatioTemporalObject));
                containFlag = true;
            }
            else if (grid.intersects(cube) || cube.covers(grid)) {
                result.add(new Tuple2<>(i, spatioTemporalObject));
            }
        }

        if (!containFlag) {
            result.add(new Tuple2<>(overflowContainerID, spatioTemporalObject));
        }

        return result.iterator();
    }

    @Nullable
    @Override
    public SpatioTemporalDedupParams getSpatioTemporalDedupParams() {
        return null;
    }

    @Nullable
    public DedupParams getDedupParams()
    {
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
        if (o == null || !(o instanceof CubeGridPartitioner)) {
            return false;
        }

        final CubeGridPartitioner other = (CubeGridPartitioner) o;

        // For backwards compatibility (see SpatialRDD.spatialPartitioning(otherGrids))
        if (this.gridType == null || other.gridType == null) {
            return other.grids.equals(this.grids);
        }

        return other.gridType.equals(this.gridType) && other.grids.equals(this.grids);
    }
}
