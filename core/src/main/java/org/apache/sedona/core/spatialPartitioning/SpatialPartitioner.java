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
import org.apache.spark.Partitioner;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

abstract public class SpatialPartitioner
        extends Partitioner
        implements Serializable
{

    protected final GridType gridType;
    protected final List<Envelope> grids;

    protected SpatialPartitioner(GridType gridType, List<Envelope> grids)
    {
        this.gridType = gridType;
        this.grids = Objects.requireNonNull(grids, "grids");
    }

    /**
     * Given a geometry, returns a list of partitions it overlaps.
     * <p>
     * For points, returns exactly one partition as long as grid type is non-overlapping.
     * For other geometry types or for overlapping grid types, may return multiple partitions.
     */
    abstract public <T extends Geometry> Iterator<Tuple2<Integer, T>>
    placeObject(T spatialObject)
            throws Exception;

    @Nullable
    abstract public DedupParams getDedupParams();

    public GridType getGridType()
    {
        return gridType;
    }

    public List<Envelope> getGrids()
    {
        return grids;
    }

    @Override
    public int getPartition(Object key)
    {
        return (int) key;
    }
}
