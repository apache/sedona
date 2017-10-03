/**
 * FILE: QuadtreePartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;

import java.io.Serializable;
import java.util.List;

public class QuadtreePartitioning implements Serializable {

    /**
     * The Quad-Tree.
     */
    private final StandardQuadTree<Integer> partitionTree;

    /**
     * Instantiates a new Quad-Tree partitioning.
     *
     * @param samples the sample list
     * @param boundary   the boundary
     * @param partitions the partitions
     */
    public QuadtreePartitioning(List<? extends Geometry> samples, Envelope boundary, int partitions) throws Exception {
        this(samples, boundary, partitions, -1);
    }

    public QuadtreePartitioning(List<? extends Geometry> samples, Envelope boundary, final int partitions, int minTreeLevel)
            throws Exception {
        partitionTree = new StandardQuadTree(new QuadRectangle(boundary), 0,
            samples.size() / partitions, 100000);
        if (minTreeLevel > 0) {
            partitionTree.forceGrowUp(minTreeLevel);
        }

        for (final Geometry sample : samples) {
            final Envelope envelope = sample.getEnvelopeInternal();
            partitionTree.insert(new QuadRectangle(envelope), 1);
        }

        partitionTree.assignPartitionIds();
    }

    public StandardQuadTree getPartitionTree()
    {
        return this.partitionTree;
    }
}
