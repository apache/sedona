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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

// TODO: Auto-generated Javadoc

public class QuadtreePartitioning implements Serializable {

    /**
     * The grids.
     */
    private StandardQuadTree<Integer> partitionTree = null;
    /**
     * Instantiates a new rtree partitioning.
     *
     * @param SampleList the sample list
     * @param boundary   the boundary
     * @param partitions the partitions
     * @throws Exception the exception
     */
    public QuadtreePartitioning(List SampleList, Envelope boundary, int partitions) throws Exception {
        StandardQuadTree.maxItemByNode = SampleList.size() / partitions;
        StandardQuadTree.maxLevel = 100000;
        partitionTree = new StandardQuadTree(new QuadRectangle(boundary.getMinX(),boundary.getMinY(),boundary.getWidth(),boundary.getHeight()), 0);
        for (int i = 0; i < SampleList.size(); i++) {
            if (SampleList.get(i) instanceof Envelope) {
                Envelope spatialObject = (Envelope) SampleList.get(i);
                partitionTree.insert(new QuadRectangle(spatialObject), 1);
            } else if (SampleList.get(i) instanceof Geometry) {
                Geometry spatialObject = (Geometry) SampleList.get(i);
                partitionTree.insert(new QuadRectangle(spatialObject.getEnvelopeInternal()), 1);
            } else {
                throw new Exception("[QuadtreePartitioning][Constrcutor] Unsupported spatial object type");
            }
        }
        HashSet<Integer> uniqueIdList = new HashSet<Integer>();
        partitionTree.getAllLeafNodeUniqueId(uniqueIdList);
        HashMap<Integer,Integer> serialIdMapping = partitionTree.getSeriaIdMapping(uniqueIdList);
        partitionTree.decidePartitionSerialId(serialIdMapping);
    }

    public QuadtreePartitioning(List SampleList, Envelope boundary, int partitions, int minTreeLevel) throws Exception {
        StandardQuadTree.maxItemByNode = SampleList.size() / partitions;
        StandardQuadTree.maxLevel = 100000;
        partitionTree = new StandardQuadTree(new QuadRectangle(boundary.getMinX(),boundary.getMinY(),boundary.getWidth(),boundary.getHeight()), 0);
        partitionTree.forceGrowUp(minTreeLevel);
        for (int i = 0; i < SampleList.size(); i++) {
            if (SampleList.get(i) instanceof Envelope) {
                Envelope spatialObject = (Envelope) SampleList.get(i);
                partitionTree.insert(new QuadRectangle(spatialObject), 1);
            } else if (SampleList.get(i) instanceof Geometry) {
                Geometry spatialObject = (Geometry) SampleList.get(i);
                partitionTree.insert(new QuadRectangle(spatialObject.getEnvelopeInternal()), 1);
            } else {
                throw new Exception("[QuadtreePartitioning][Constrcutor] Unsupported spatial object type");
            }
        }
        HashSet<Integer> uniqueIdList = new HashSet<Integer>();
        partitionTree.getAllLeafNodeUniqueId(uniqueIdList);
        HashMap<Integer,Integer> serialIdMapping = partitionTree.getSeriaIdMapping(uniqueIdList);
        partitionTree.decidePartitionSerialId(serialIdMapping);
    }

    public StandardQuadTree getPartitionTree()
    {
        return this.partitionTree;
    }
}
