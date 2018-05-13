/*
 * FILE: Octree
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableInt;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;

public class Octree<T>
        implements Serializable
{
    // Maximum number of items in any given zone. When reached, a zone is sub-divided.
    private final int maxItemsPerZone;
    private final int maxLevel;

    private final int level;
    private int nodeNum = 0;

    // the four sub regions,
    // may be null if not needed
    private Octree<T>[] regions;

    // the current nodes
    private final List<OctreeNode<T>> nodes = new ArrayList<>();

    // current rectangle zone
    private final OctreeRectangle zone;

    public static final int REGION_SELF = -1;
    public static final int DOWN_REGION_NW = 0;
    public static final int DOWN_REGION_NE = 1;
    public static final int DOWN_REGION_SW = 2;
    public static final int DOWN_REGION_SE = 3;
    public static final int UP_REGION_NW = 4;
    public static final int UP_REGION_NE = 5;
    public static final int UP_REGION_SW = 6;
    public static final int UP_REGION_SE = 7;

    public Octree(OctreeRectangle definition, int level)
    {
        this(definition, level, 5, 10);
    }


    public Octree(OctreeRectangle definition, int level, int maxItemsPerZone, int maxLevel)
    {
        this.maxItemsPerZone = maxItemsPerZone;
        this.maxLevel = maxLevel;
        this.zone = definition;
        this.level = level;
    }

    public OctreeRectangle getZone()
    {
        return this.zone;
    }

    private int findRegion(OctreeRectangle r, boolean split)
    {
        int region = REGION_SELF;
        if (nodeNum >= maxItemsPerZone && this.level < maxLevel) {
            // we don't want to split if we just need to retrieve
            // the region, not inserting an element
            if (regions == null && split) {
                // then create the subregions
                this.split();
            }

            // can be null if not splitted
            if (regions != null) {
                for (int i = 0; i < regions.length; i++) {
                    if (regions[i].getZone().contains(r)) {
                        region = i;
                        break;
                    }
                }
            }
        }

        return region;
    }

    private int findRegion(double x, double y, double z)
    {
        int region = REGION_SELF;
        // can be null if not splitted
        if (regions != null) {
            for (int i = 0; i < regions.length; i++) {
                if (regions[i].getZone().contains(x, y, z)) {
                    region = i;
                    break;
                }
            }
        }
        return region;
    }

    private Octree<T> newQuadTree(OctreeRectangle zone, int level)
    {
        return new Octree<T>(zone, level, this.maxItemsPerZone, this.maxLevel);
    }

    private void split()
    {

        regions = new Octree[8];

        double newWidth = zone.width / 2;
        double newLength = zone.length / 2;
        double newHeight = zone.height / 2;
        int newLevel = level + 1;

        regions[DOWN_REGION_NW] = newQuadTree(new OctreeRectangle(
                zone.x,
                zone.y + zone.length / 2,
                zone.z,
                newWidth,
                newLength,
                newHeight
        ), newLevel);

        regions[DOWN_REGION_NE] = newQuadTree(new OctreeRectangle(
                zone.x + zone.width / 2,
                zone.y + zone.length / 2,
                zone.z,
                newWidth,
                newLength,
                newHeight
        ), newLevel);

        regions[DOWN_REGION_SW] = newQuadTree(new OctreeRectangle(
                zone.x,
                zone.y,
                zone.z,
                newWidth,
                newLength,
                newHeight
        ), newLevel);

        regions[DOWN_REGION_SE] = newQuadTree(new OctreeRectangle(
                zone.x + zone.width / 2,
                zone.y,
                zone.z,
                newWidth,
                newLength,
                newHeight
        ), newLevel);

        regions[UP_REGION_NW] = newQuadTree(new OctreeRectangle(
                zone.x,
                zone.y + zone.length / 2,
                zone.z + zone.height / 2,
                newWidth,
                newLength,
                newHeight
        ), newLevel);

        regions[UP_REGION_NE] = newQuadTree(new OctreeRectangle(
                zone.x + zone.width / 2,
                zone.y + zone.length / 2,
                zone.z + zone.height / 2,
                newWidth,
                newLength,
                newHeight
        ), newLevel);

        regions[UP_REGION_SW] = newQuadTree(new OctreeRectangle(
                zone.x,
                zone.y,
                zone.z + zone.height / 2,
                newWidth,
                newLength,
                newHeight
        ), newLevel);

        regions[UP_REGION_SE] = newQuadTree(new OctreeRectangle(
                zone.x + zone.width / 2,
                zone.y,
                zone.z + zone.height / 2,
                newWidth,
                newLength,
                newHeight
        ), newLevel);
    }

    // Force the quad tree to grow up to a certain level.
    public void forceGrowUp(int minLevel)
    {
        if (minLevel < 1) {
            throw new IllegalArgumentException("minLevel must be >= 1. Received " + minLevel);
        }

        split();
        nodeNum = maxItemsPerZone;
        if (level + 1 >= minLevel) {
            return;
        }

        for (Octree<T> region : regions) {
            region.forceGrowUp(minLevel);
        }
    }

    public void insert(OctreeRectangle r, T element)
    {
        int region = this.findRegion(r, true);
        if (region == REGION_SELF || this.level == maxLevel) {
            nodes.add(new OctreeNode<T>(r, element));
            nodeNum++;
            return;
        }
        else {
            regions[region].insert(r, element);
        }

        if (nodeNum >= maxItemsPerZone && this.level < maxLevel) {
            // redispatch the elements
            List<OctreeNode<T>> tempNodes = new ArrayList<>();
            tempNodes.addAll(nodes);

            nodes.clear();
            for (OctreeNode<T> node : tempNodes) {
                this.insert(node.r, node.element);
            }
        }
    }

    public void dropElements()
    {
        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(Octree<T> tree)
            {
                tree.nodes.clear();
                return true;
            }
        });
    }

    public List<T> getElements(OctreeRectangle r)
    {
        int region = this.findRegion(r, false);

        final List<T> list = new ArrayList<>();

        if (region != REGION_SELF) {
            for (OctreeNode<T> node : nodes) {
                list.add(node.element);
            }

            list.addAll(regions[region].getElements(r));
        }
        else {
            addAllElements(list);
        }

        return list;
    }

    private interface Visitor<T>
    {
        /**
         * Visits a single node of the tree
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(Octree<T> tree);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    private void traverse(Visitor<T> visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (regions != null) {
            regions[DOWN_REGION_NW].traverse(visitor);
            regions[DOWN_REGION_NE].traverse(visitor);
            regions[DOWN_REGION_SW].traverse(visitor);
            regions[DOWN_REGION_SE].traverse(visitor);
            regions[UP_REGION_NW].traverse(visitor);
            regions[UP_REGION_NE].traverse(visitor);
            regions[UP_REGION_SW].traverse(visitor);
            regions[UP_REGION_SE].traverse(visitor);
        }
    }

    private void addAllElements(final List<T> list)
    {
        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(Octree<T> tree)
            {
                for (OctreeNode<T> node : tree.nodes) {
                    list.add(node.element);
                }
                return true;
            }
        });
    }

    public boolean isLeaf()
    {
        return regions == null;
    }

    public List<OctreeRectangle> getAllZones()
    {
        final List<OctreeRectangle> zones = new ArrayList<>();
        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(Octree<T> tree)
            {
                zones.add(tree.zone);
                return true;
            }
        });

        return zones;
    }

    public List<OctreeRectangle> getLeafZones()
    {
        final List<OctreeRectangle> leafZones = new ArrayList<>();
        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(Octree<T> tree)
            {
                if (tree.isLeaf()) {
                    leafZones.add(tree.zone);
                }
                return true;
            }
        });

        return leafZones;
    }

    public int getTotalNumLeafNode()
    {
        final MutableInt leafCount = new MutableInt(0);
        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(Octree<T> tree)
            {
                if (tree.isLeaf()) {
                    leafCount.increment();
                }
                return true;
            }
        });

        return leafCount.getValue();
    }

    /**
     * Find the zone that fully contains this query point
     *
     * @param x
     * @param y
     * @return
     */
    public OctreeRectangle getZone(double x, double y, double z)
            throws ArrayIndexOutOfBoundsException
    {
        int region = this.findRegion(x, y, z);
        if (region != REGION_SELF) {
            return regions[region].getZone(x, y, z);
        }
        else {
            if (this.zone.contains(x, y, z)) {
                return this.zone;
            }

            throw new ArrayIndexOutOfBoundsException("[GeoSparkViz][ThreeDStandardQuadTree] this pixel is out of the quad tree boundary.");
        }
    }

    public OctreeRectangle getParentZone(double x, double y, double z, int minLevel)
            throws Exception
    {
        int region = this.findRegion(x, y, z);
        // Assume this 3Dquad tree has done force grow up. Thus, the min tree depth is the min tree level
        if (level < minLevel) {
            // In our case, this node must have child nodes. But, in general, if the region is still -1, that means none of its child contains
            // the given x and y and z
            if (region == REGION_SELF) {
                assert regions == null;
                if (zone.contains(x, y, z)) {
                    // This should not happen
                    throw new Exception("[GeoSparkViz][ThreeDStandardQuadTree][getParentZone] this leaf node doesn't have enough depth. " +
                            "Please check ForceGrowUp. Expected: " + minLevel + " Actual: " + level + ". Query " + "point:" + " " + x + " " + y + " " + z +
                            ". Tree statistics, total leaf nodes: " + getTotalNumLeafNode());
                }
                else {
                    throw new Exception("[GeoSparkViz][ThreeDStandardQuadTree][getParentZone] this pixel is out of the quad tree boundary.");
                }
            }
            else {
                return regions[region].getParentZone(x, y, z, minLevel);
            }
        }
        if (zone.contains(x, y, z)) {
            return zone;
        }

        throw new Exception("[GeoSparkViz][ThreeDStandardQuadTree][getParentZone] this pixel is out of the 3Dquad tree boundary.");
    }

    public List<OctreeRectangle> findZones(OctreeRectangle r)
    {
        final Cube envelope = r.getSTEnvelope();

        final List<OctreeRectangle> matches = new ArrayList<>();
        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(Octree<T> tree)
            {
                if (!disjoint(tree.zone.getSTEnvelope(), envelope)) {
                    if (tree.isLeaf()) {
                        matches.add(tree.zone);
                    }
                    return true;
                }
                else {
                    return false;
                }
            }
        });

        return matches;
    }

    private boolean disjoint(Cube r1, Cube r2)
    {
        return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
    }

    public void assignPartitionIds()
    {
        traverse(new Visitor<T>()
        {
            private int partitionId = 0;

            @Override
            public boolean visit(Octree<T> tree)
            {
                if (tree.isLeaf()) {
                    tree.getZone().partitionId = partitionId;
                    partitionId++;
                }
                return true;
            }
        });
    }
}
