/*
 * FILE: OctreePartitioner
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
package org.datasyslab.geospark.spatioTemporalPartitioning.octree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.utils.HalfOpenCube;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.datasyslab.geospark.SpatioTemporalObjects.SpatioTemporalObject;
import org.datasyslab.geospark.enums.SpatioTemporalGridType;
import org.datasyslab.geospark.joinJudgement.SpatioTemporalDedupParams;
import org.datasyslab.geospark.spatioTemporalPartitioning.SpatioTemporalPartitioner;

import scala.Tuple2;

public class OctreePartitioner
        extends SpatioTemporalPartitioner
{
    private Octree<? extends SpatioTemporalObject> threeDQuadTree;

    public OctreePartitioner(Octree<? extends SpatioTemporalObject> threeDQuadTree)
    {
        super(SpatioTemporalGridType.OCTREE, getLeafGrids(threeDQuadTree));
        this.threeDQuadTree = threeDQuadTree;

        // Make sure not to broadcast all the samples used to build the Quad
        // tree to all nodes which are doing partitioning
        this.threeDQuadTree.dropElements();
    }

    @Override
    public <T extends SpatioTemporalObject> Iterator<Tuple2<Integer, T>> placeObject(T spatioTemporalObject)
            throws Exception
    {
        Objects.requireNonNull(spatioTemporalObject, "spatioTemporalObject");

        final Cube cube = spatioTemporalObject.getCubeInternal();

        final List<OctreeRectangle> matchedPartitions = threeDQuadTree.findZones(new OctreeRectangle(cube));

        final Point3D point = spatioTemporalObject instanceof Point3D ? (Point3D) spatioTemporalObject : null;

        final Set<Tuple2<Integer, T>> result = new HashSet<>();
        for (OctreeRectangle rectangle : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenCube(rectangle.getSTEnvelope())).contains(point)) {
                continue;
            }

            result.add(new Tuple2(rectangle.partitionId, spatioTemporalObject));
        }

        return result.iterator();
    }

    @Nullable
    @Override
    public SpatioTemporalDedupParams getSpatioTemporalDedupParams()
    {
        return new SpatioTemporalDedupParams(grids);
    }

    @Override
    public int numPartitions()
    {
        return grids.size();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof OctreePartitioner)) {
            return false;
        }

        final OctreePartitioner other = (OctreePartitioner) o;
        return other.threeDQuadTree.equals(this.threeDQuadTree);
    }

    private static List<Cube> getLeafGrids(Octree<? extends SpatioTemporalObject> quadTree)
    {
        Objects.requireNonNull(quadTree, "3DquadTree");

        final List<OctreeRectangle> zones = quadTree.getLeafZones();
        final List<Cube> grids = new ArrayList<>();
        for (OctreeRectangle zone : zones) {
            grids.add(zone.getSTEnvelope());
        }

        return grids;
    }
}
