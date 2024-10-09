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
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.ImprovedQT.ImprovedQuadTree;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

public class ImprovedQuadTreePartitioner
        extends SpatialPartitioner
{
    private final ImprovedQuadTree<? extends Geometry> quadTree;

    public ImprovedQuadTreePartitioner(ImprovedQuadTree<? extends Geometry> quadTree)
    {
        super(GridType.ImprovedQT, quadTree.fetchLeafZones());
        this.quadTree = quadTree;

        // Make sure not to broadcast all the samples used to build the Quad
        // tree to all nodes which are doing partitioning
        this.quadTree.dropElements();
    }

    @Override
    public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry spatialObject)
            throws Exception
    {

        if(spatialObject instanceof Circle){
            return quadTree.placeObjectWithFilter(spatialObject);
        }

        return quadTree.placeObject(spatialObject);
    }

    @Nullable
    @Override
    public DedupParams getDedupParams()
    {
        return new DedupParams(grids);
    }

    @Override
    public int numPartitions()
    {
        return grids.size();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof ImprovedQuadTreePartitioner)) {
            return false;
        }

        final ImprovedQuadTreePartitioner other = (ImprovedQuadTreePartitioner) o;
        return other.quadTree.equals(this.quadTree);
    }

    public ImprovedQuadTree<? extends Geometry> getQuadTree() {
        return quadTree;
    }

}
