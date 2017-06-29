/**
 * FILE: StandardQuadTree.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class StandardQuadTree<T> implements Serializable{

    // the current nodes
    ArrayList<QuadNode<T>> nodes = new ArrayList<QuadNode<T>>();

    // current rectangle zone
    private QuadRectangle zone;

    private int serialId = -1;

    // GLOBAL CONFIGRATION
    // if this is reached,
    // the zone is subdivised
    public static int maxItemByNode = 5;
    public static int maxLevel = 10;

    int level;
    int nodeNum=0;
    
    // the four sub regions,
    // may be null if not needed
    StandardQuadTree<T>[] regions;

    public static final int REGION_SELF = -1;
    public static final int REGION_NW = 0;
    public static final int REGION_NE = 1;
    public static final int REGION_SW = 2;
    public static final int REGION_SE = 3;

    public StandardQuadTree(QuadRectangle definition, int level) {
        zone = definition;
        nodes = new ArrayList<QuadNode<T>>();
        this.level = level;
    }

    protected QuadRectangle getZone() {
        return this.zone;
    }

    private int findRegion(QuadRectangle r, boolean split) {
        int region = REGION_SELF;
        if (nodeNum >= maxItemByNode && this.level < maxLevel) {
            // we don't want to split if we just need to retrieve
            // the region, not inserting an element
            if (regions == null && split) {
                // then create the subregions
                this.split();
            }

            // can be null if not splitted
            if (regions != null) {
                if (regions[REGION_NW].getZone().contains(r)) {
                    region = REGION_NW;
                } else if (regions[REGION_NE].getZone().contains(r)) {
                    region = REGION_NE;
                } else if (regions[REGION_SW].getZone().contains(r)) {
                    region = REGION_SW;
                } else if (regions[REGION_SE].getZone().contains(r)) {
                    region = REGION_SE;
                }
            }
        }

        return region;
    }
    private int findRegion(int x, int y) {
        int region = REGION_SELF;
            // can be null if not splitted
            if (regions != null) {
                if (regions[REGION_NW].getZone().contains(x,y)) {
                    region = REGION_NW;
                } else if (regions[REGION_NE].getZone().contains(x,y)) {
                    region = REGION_NE;
                } else if (regions[REGION_SW].getZone().contains(x,y)) {
                    region = REGION_SW;
                } else if (regions[REGION_SE].getZone().contains(x,y)) {
                    region = REGION_SE;
                }
        }
        return region;
    }
    private void split() {

        regions = new StandardQuadTree[4];

        double newWidth = zone.width / 2;
        double newHeight = zone.height / 2;
        int newLevel = level + 1;

        regions[REGION_NW] = new StandardQuadTree<T>(new QuadRectangle(
                zone.x,
                zone.y + zone.height / 2,
                newWidth,
                newHeight
        ), newLevel);

        regions[REGION_NE] = new StandardQuadTree<T>(new QuadRectangle(
                zone.x + zone.width / 2,
                zone.y + zone.height / 2,
                newWidth,
                newHeight
        ), newLevel);

        regions[REGION_SW] = new StandardQuadTree<T>(new QuadRectangle(
                zone.x,
                zone.y,
                newWidth,
                newHeight
        ), newLevel);

        regions[REGION_SE] = new StandardQuadTree<T>(new QuadRectangle(
                zone.x + zone.width / 2,
                zone.y,
                newWidth,
                newHeight
        ), newLevel);
    }

    // Force the quad tree to grow up to a certain level. The number of
    public void forceGrowUp(int minLevel)
    {
        assert minLevel>=1;
        split();
       	nodeNum = maxItemByNode;
        if(level+1>=minLevel)
        {
            return;
        }
        else
        {
            regions[REGION_NW].forceGrowUp(minLevel);
            regions[REGION_NE].forceGrowUp(minLevel);
            regions[REGION_SW].forceGrowUp(minLevel);
            regions[REGION_SE].forceGrowUp(minLevel);
            return;
        }

    }

    public void insert(QuadRectangle r, T element) {
        int region = this.findRegion(r, true);
        if (region == REGION_SELF || this.level == maxLevel) {
            nodes.add(new QuadNode<T>(r, element));
            nodeNum++;
            return;
        } else {
            regions[region].insert(r, element);
        }

        if (nodeNum >= maxItemByNode && this.level < maxLevel) {
            // redispatch the elements
            ArrayList<QuadNode<T>> tempNodes = new ArrayList<QuadNode<T>>();
            int length = nodes.size();
            for (int i = 0; i < length; i++) {
                tempNodes.add(nodes.get(i));

            }
            nodes.clear();

            for (QuadNode<T> node : tempNodes) {
                this.insert(node.r, node.element);
            }
        }
    }

    public ArrayList<T> getElements(ArrayList<T> list, QuadRectangle r) {
        int region = this.findRegion(r, false);

        int length = nodes.size();
        for (int i = 0; i < length; i++) {
            list.add(nodes.get(i).element);
        }

        if (region != REGION_SELF) {
            regions[region].getElements(list, r);
        } else {
            getAllElements(list, true);
        }

        return list;
    }

    public ArrayList<T> getAllElements(ArrayList<T> list, boolean firstCall) {
        if (regions != null) {
            regions[REGION_NW].getAllElements(list, false);
            regions[REGION_NE].getAllElements(list, false);
            regions[REGION_SW].getAllElements(list, false);
            regions[REGION_SE].getAllElements(list, false);
        }

        if (!firstCall) {
            int length = nodes.size();
            for (int i = 0; i < length; i++) {
                list.add(nodes.get(i).element);
            }
        }

        return list;
    }

    public void getAllZones(ArrayList<QuadRectangle> list) {
        list.add(this.zone);
        if (regions != null) {
            regions[REGION_NW].getAllZones(list);
            regions[REGION_NE].getAllZones(list);
            regions[REGION_SW].getAllZones(list);
            regions[REGION_SE].getAllZones(list);
        }
    }

    public void getAllLeafNodeUniqueId(HashSet<Integer> uniqueIdList)
    {
        if (regions!= null) {
            regions[REGION_NW].getAllLeafNodeUniqueId(uniqueIdList);
            regions[REGION_NE].getAllLeafNodeUniqueId(uniqueIdList);
            regions[REGION_SW].getAllLeafNodeUniqueId(uniqueIdList);
            regions[REGION_SE].getAllLeafNodeUniqueId(uniqueIdList);
        } else {
            // This is a leaf node
            uniqueIdList.add(zone.hashCode());
        }
    }

    public int getTotalNumLeafNode() {
        if (regions!= null) {
            return regions[REGION_NW].getTotalNumLeafNode() + regions[REGION_NE].getTotalNumLeafNode()
                    + regions[REGION_SW].getTotalNumLeafNode() + regions[REGION_SE].getTotalNumLeafNode();
        } else {
            // This is a leaf node
            return 1;
        }
    }
    /**
     * Find the zone that fully contains this query point
     * @param x
     * @param y
     * @return
     */
    public QuadRectangle getZone(int x, int y) throws ArrayIndexOutOfBoundsException{
        int region = this.findRegion(x,y);
        if (region != REGION_SELF) {
            return regions[region].getZone(x,y);
        } else {
            if(this.zone.contains(x, y))
            {
                return this.zone;
            }
            else
            {
                throw new ArrayIndexOutOfBoundsException("[Babylon][StandardQuadTree] this pixel is out of the quad tree boundary.");
            }
        }
    }
    /*
    public QuadRectangle getZone(ArrayList<QuadRectangle> zoneList, QuadRectangle queryRectangle) throws ArrayIndexOutOfBoundsException{
        if (regions!= null) {
            regions[REGION_NW].getZone(uniqueIdList, resolutionX, resolutionY);
            regions[REGION_NE].getZone(uniqueIdList, resolutionX, resolutionY);
            regions[REGION_SW].getZone(uniqueIdList, resolutionX, resolutionY);
            regions[REGION_SE].getZone(uniqueIdList, resolutionX, resolutionY);
        } else {
            // This is a leaf node
            uniqueIdList.add(zone.getUniqueId(resolutionX, resolutionY));
        }
    }
    */
    public QuadRectangle getParentZone(int x, int y, int minLevel) throws Exception {
        int region = this.findRegion(x, y);
        // Assume this quad tree has done force grow up. Thus, the min tree depth is the min tree level
        if (level < minLevel) {
            // In our case, this node must have child nodes. But, in general, if the region is still -1, that means none of its child contains
            // the given x and y
            if (region == REGION_SELF) {
                assert regions==null;
                if (zone.contains(x, y)) {
                    // This should not happen
                    throw new Exception("[Babylon][StandardQuadTree][getParentZone] this leaf node doesn't have enough depth. " +
                            "Please check ForceGrowUp. Expected: "+minLevel+" Actual: "+level+". Query point: "+x+" "+y+
                            ". Tree statistics, total leaf nodes: "+getTotalNumLeafNode());
                } else {
                    throw new Exception("[Babylon][StandardQuadTree][getParentZone] this pixel is out of the quad tree boundary.");
                }
            } else {
                return regions[region].getParentZone(x, y, minLevel);
            }
        }
        if (zone.contains(x, y))
        {
            return zone;
        }
        else
        {
            throw new Exception("[Babylon][StandardQuadTree][getParentZone] this pixel is out of the quad tree boundary.");
        }
    }


    public void getZone(ArrayList<QuadRectangle> matchedPartitions, QuadRectangle r)
    {
        // This is a leaf node. Assign a serial Id to this leaf node.
        if (regions == null)
        {
            matchedPartitions.add(zone);
            return;
        }
        else
        {
            if (regions != null) {
                if (!disjoint(regions[REGION_NW].getZone().getEnvelope(),r.getEnvelope())) {
                    regions[REGION_NW].getZone(matchedPartitions, r);
                }
                if (!disjoint(regions[REGION_NE].getZone().getEnvelope(),r.getEnvelope())) {
                    regions[REGION_NE].getZone(matchedPartitions, r);
                }
                if (!disjoint(regions[REGION_SW].getZone().getEnvelope(),r.getEnvelope())) {
                    regions[REGION_SW].getZone(matchedPartitions, r);
                }
                if (!disjoint(regions[REGION_SE].getZone().getEnvelope(),r.getEnvelope())) {
                    regions[REGION_SE].getZone(matchedPartitions, r);
                }
            }
        }
    }
    public boolean disjoint(Envelope r1, Envelope r2)
    {
        if(r1.intersects(r2)||r1.covers(r2)||r2.covers(r1))
        {
            return false;
        }
        else return true;
    }

    public HashMap<Integer,Integer> getSeriaIdMapping(HashSet<Integer> uniqueIdList)
    {
        int accumulator =0;
        HashMap<Integer,Integer> idMapping = new HashMap<Integer, Integer>();
        Iterator<Integer> uniqueIdIterator = uniqueIdList.iterator();
        while(uniqueIdIterator.hasNext())
        {
            int curId = uniqueIdIterator.next();
            idMapping.put(curId, accumulator);
            accumulator++;
        }
        return idMapping;
    }
    public void decidePartitionSerialId(HashMap<Integer, Integer> serialIdMapping)
    {
        // This is a leaf node. Assign a serial Id to this leaf node.
        if (regions == null)
        {
            serialId = serialIdMapping.get(zone.hashCode());
            zone.partitionId = serialId;
            return;
        }
        else
        {
            regions[REGION_NW].decidePartitionSerialId(serialIdMapping);
            regions[REGION_NE].decidePartitionSerialId(serialIdMapping);
            regions[REGION_SW].decidePartitionSerialId(serialIdMapping);
            regions[REGION_SE].decidePartitionSerialId(serialIdMapping);
        }
        return;
    }
}
