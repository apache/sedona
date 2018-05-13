/*
 * FILE: RTree3DPartitioning
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

import java.util.ArrayList;
import java.util.List;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.thirdlibraray.rtree3d.geometry.Rect3d;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.Leaf;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.RTree;


/**
 * The Class RTree3DPartitioning.
 */
public class RTree3DPartitioning {

    private final RTree<Rect3d> partitionTree;

    /**
     * The grids.
     */
    final List<Cube> grids = new ArrayList<>();


    public RTree3DPartitioning(List<Cube> samples, Cube boundary, final int partitions)
    {
        int maxItemPerNode = samples.size() / partitions;
        partitionTree = new RTree<Rect3d>(new Rect3d.Builder(), 2, maxItemPerNode, RTree.Split.QUADRATIC);

        for (final Cube sample : samples) {
            Rect3d rect3d = new Rect3d(sample.getMinX(), sample.getMinY(), sample.getMinZ(),
                    sample.getMaxX(), sample.getMaxY(), sample.getMaxZ());
            partitionTree.add(rect3d);
        }

        //partitionTree.assignPartitionId();
        Rect3d searchRect = new Rect3d(Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
        List<Leaf> leafs = new ArrayList<Leaf>();
        partitionTree.search(searchRect, leafs);
        for (int i = 0; i < leafs.size(); i++) {
            Rect3d rect = (Rect3d) leafs.get(i).getBound();
            Cube cube = new Cube(rect.getMin().getCoord(0), rect.getMax().getCoord(0),
                    rect.getMin().getCoord(1), rect.getMax().getCoord(1),
                    rect.getMin().getCoord(2), rect.getMax().getCoord(2));
            grids.add(cube);
        }

    }

    public List<Cube> getGrids() {
        return grids;
    }

}
