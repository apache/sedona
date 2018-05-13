/*
 * FILE: OctreePartitioning
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
import java.util.List;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.spatioTemporalPartitioning.octree.OctreeRectangle;
import org.datasyslab.geospark.spatioTemporalPartitioning.octree.Octree;


/**
 * The Class OctreePartitioning.
 */
public class OctreePartitioning
        implements Serializable
{

    /**
     * The 3D-Quad-Tree.
     */
    private final Octree<Integer> partitionTree;

    /**
     * Instantiates a new Quad-Tree partitioning.
     *
     * @param samples the sample list
     * @param boundary the boundary
     * @param partitions the partitions
     */
    public OctreePartitioning(List<Cube> samples, Cube boundary, int partitions)
            throws Exception
    {
        this(samples, boundary, partitions, -1);
    }

    /**
     * build tree and insert
     * */
    public OctreePartitioning(List<Cube> samples, Cube boundary, final int partitions, int minTreeLevel)
            throws Exception
    {
        // Make sure the tree doesn't get too deep in case of data skew
        int maxLevel = partitions;
        int maxItemsPerNode = samples.size() / partitions;
        partitionTree = new Octree(new OctreeRectangle(boundary), 0, maxItemsPerNode, maxLevel);
        if (minTreeLevel > 0) {
            partitionTree.forceGrowUp(minTreeLevel);
        }

        for (final Cube sample : samples) {
            partitionTree.insert(new OctreeRectangle(sample), 1);
        }

        partitionTree.assignPartitionIds();
    }

    public Octree getPartitionTree()
    {
        return this.partitionTree;
    }
}
