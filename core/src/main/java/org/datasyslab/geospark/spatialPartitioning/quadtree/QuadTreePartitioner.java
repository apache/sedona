/*
 * FILE: QuadTreePartitioner
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
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.utils.HalfOpenRectangle;
import scala.Tuple2;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class QuadTreePartitioner
        extends SpatialPartitioner
{
    private StandardQuadTree<? extends Geometry> quadTree;

    public QuadTreePartitioner(StandardQuadTree<? extends Geometry> quadTree)
    {
        super(GridType.QUADTREE, getLeafGrids(quadTree));
        this.quadTree = quadTree;

        // Make sure not to broadcast all the samples used to build the Quad
        // tree to all nodes which are doing partitioning
        this.quadTree.dropElements();
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
            throws Exception
    {
        Objects.requireNonNull(spatialObject, "spatialObject");

        final Envelope envelope = spatialObject.getEnvelopeInternal();

        final List<QuadRectangle> matchedPartitions = quadTree.findZones(new QuadRectangle(envelope));

        final Point point = spatialObject instanceof Point ? (Point) spatialObject : null;

        final Set<Tuple2<Integer, T>> result = new HashSet<>();
        for (QuadRectangle rectangle : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(rectangle.getEnvelope())).contains(point)) {
                continue;
            }

            result.add(new Tuple2(rectangle.partitionId, spatialObject));
        }

        return result.iterator();
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
        if (o == null || !(o instanceof QuadTreePartitioner)) {
            return false;
        }

        final QuadTreePartitioner other = (QuadTreePartitioner) o;
        return other.quadTree.equals(this.quadTree);
    }

    private static List<Envelope> getLeafGrids(StandardQuadTree<? extends Geometry> quadTree)
    {
        Objects.requireNonNull(quadTree, "quadTree");

        final List<QuadRectangle> zones = quadTree.getLeafZones();
        final List<Envelope> grids = new ArrayList<>();
        for (QuadRectangle zone : zones) {
            grids.add(zone.getEnvelope());
        }

        return grids;
    }
}
