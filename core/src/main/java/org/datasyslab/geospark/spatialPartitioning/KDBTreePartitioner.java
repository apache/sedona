/*
 * FILE: KDBTreePartitioner
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.datasyslab.geospark.spatialPartitioning;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.utils.HalfOpenRectangle;
import scala.Tuple2;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class KDBTreePartitioner
        extends SpatialPartitioner
{
    private final KDBTree tree;

    public KDBTreePartitioner(KDBTree tree)
    {
        super(GridType.KDBTREE, getLeafZones(tree));
        this.tree = tree;
        this.tree.dropElements();
    }

    @Override
    public int numPartitions()
    {
        return grids.size();
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
            throws Exception
    {

        Objects.requireNonNull(spatialObject, "spatialObject");

        final Envelope envelope = spatialObject.getEnvelopeInternal();

        final List<KDBTree> matchedPartitions = tree.findLeafNodes(envelope);

        final Point point = spatialObject instanceof Point ? (Point) spatialObject : null;

        final Set<Tuple2<Integer, T>> result = new HashSet<>();
        for (KDBTree leaf : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(leaf.getExtent())).contains(point)) {
                continue;
            }

            result.add(new Tuple2(leaf.getLeafId(), spatialObject));
        }

        return result.iterator();
    }

    @Nullable
    @Override
    public DedupParams getDedupParams()
    {
        return new DedupParams(grids);
    }

    private static List<Envelope> getLeafZones(KDBTree tree)
    {
        final List<Envelope> leafs = new ArrayList<>();
        tree.traverse(new KDBTree.Visitor()
        {
            @Override
            public boolean visit(KDBTree tree)
            {
                if (tree.isLeaf()) {
                    leafs.add(tree.getExtent());
                }
                return true;
            }
        });

        return leafs;
    }
}
