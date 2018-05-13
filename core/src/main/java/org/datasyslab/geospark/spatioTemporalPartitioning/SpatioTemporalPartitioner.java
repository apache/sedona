/*
 * FILE: SpatioTemporalPartitioner
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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.spark.Partitioner;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.SpatioTemporalObject;
import org.datasyslab.geospark.enums.SpatioTemporalGridType;
import org.datasyslab.geospark.joinJudgement.SpatioTemporalDedupParams;

import scala.Tuple2;


/**
 * The Class SpatioTemporalPartitioner.
 */
abstract public class SpatioTemporalPartitioner
        extends Partitioner
        implements Serializable
{

    protected final SpatioTemporalGridType gridType;
    protected final List<Cube> grids;

    protected SpatioTemporalPartitioner(SpatioTemporalGridType gridType, List<Cube> grids)
    {
        this.gridType = gridType;
        this.grids = Objects.requireNonNull(grids, "grids");
    }

    /**
     * Given a SpatioTemporalElement, returns a list of partitions it overlaps.
     * <p>
     * For points, returns exactly one partition as long as grid type is non-overlapping.
     * For other SpatioTemporalElement types or for overlapping grid types, may return multiple partitions.
     */
    abstract public <T extends SpatioTemporalObject> Iterator<Tuple2<Integer, T>>
    placeObject(T SpatioTemporalObject)
            throws Exception;

    @Nullable
    abstract public SpatioTemporalDedupParams getSpatioTemporalDedupParams();

    public SpatioTemporalGridType getGridType()
    {
        return gridType;
    }

    public List<Cube> getGrids()
    {
        return grids;
    }

    @Override
    public int getPartition(Object key)
    {
        return (int) key;
    }
}
