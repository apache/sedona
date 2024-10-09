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

package org.apache.sedona.core.spatialPartitioning.ImprovedQT;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.core.spatialPartitioning.PartitioningUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class ImprovedQuadTree<T> extends PartitioningUtils
        implements Serializable {
    // Maximum number of items in any given zone. When reached, a zone is sub-divided.
    private final int maxItemsPerZone;
    // Minimum number of items in any given zone. A zone is created util reached.
    private final int minItemsPerZone;
    private final int maxLevel;
    private final int level;
    // the current nodes
    private final List<QuadNode<T>> nodes = new ArrayList<>();
    // current rectangle zone
    private final QuadRectangle zone;
    private Envelope trueZone = null;
    private MultiPolygon leafNodeZone = null;
    private int nodeNum = 0;
    // the four sub regions,
    // may be null if not needed
    private ImprovedQuadTree<T>[] regions;
    // the dataset distribution
    private double gridWidth;
    private double gridHeight;
    private int[][] prefixSum;

    public ImprovedQuadTree(QuadRectangle definition, int level) {
        this(definition, level, 5, 3, 10);
    }

    public ImprovedQuadTree(QuadRectangle definition, int level, int maxItemsPerZone, int minItemsPerZone, int maxLevel) {
        this.maxItemsPerZone = maxItemsPerZone;
        this.minItemsPerZone = minItemsPerZone;
        this.maxLevel = maxLevel;
        this.zone = definition;
        this.level = level;
    }

    public ImprovedQuadTree(QuadRectangle definition, int nodeNum, int level, int maxItemsPerZone, int minItemsPerZone, int maxLevel, double gridWidth, double gridHeight,int[][] prefixSum) {
        this.maxItemsPerZone = maxItemsPerZone;
        this.minItemsPerZone = minItemsPerZone;
        this.maxLevel = maxLevel;
        this.zone = definition;
        this.nodeNum = nodeNum;
        this.level = level;
        this.gridWidth = gridWidth;
        this.gridHeight = gridHeight;
        this.prefixSum=prefixSum;
    }

    private ImprovedQuadTree<T> newUnevenQuadTree(QuadRectangle zone, int nodeNum, int level) {
//        System.out.println("zone"+zone);
        return new ImprovedQuadTree<T>(zone, nodeNum, level, this.maxItemsPerZone, this.minItemsPerZone, this.maxLevel, this.gridWidth, this.gridHeight,this.prefixSum);
    }

    public QuadRectangle getZone() {
        return this.zone;
    }

    public MultiPolygon getLeafNodeZone() {
        return leafNodeZone;
    }

    public void setLeafNodeZone(MultiPolygon leafNodeZone) {
        this.leafNodeZone = leafNodeZone;
    }

    public List<QuadNode<T>> getNodes() {
        return nodes;
    }

    public int getLevel() {
        return level;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public int getNodeSize() {
        return nodes.size();
    }

    public Envelope getTrueZone() {
        return trueZone;
    }

    public List<QuadRectangle> getAllZones() {
        final List<QuadRectangle> zones = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                zones.add(tree.zone);
                return true;
            }
        });

        return zones;
    }

    public int getTotalNumLeafNode() {
        final MutableInt leafCount = new MutableInt(0);
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (tree.isLeaf()) {
                    leafCount.increment();
                }
                return true;
            }
        });

        return leafCount.getValue();
    }

    public int[][] getPrefixSum() {
        return prefixSum;
    }

    public double getGridWidth() {
        return gridWidth;
    }

    public double getGridHeight() {
        return gridHeight;
    }

    public void split() {
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (tree.getNodeNum() > maxItemsPerZone && tree.getLevel() < maxLevel) {
                    tree.unevenSplit();
                }
                if(level>0){
                    prefixSum=null;
                }
                return true;
            }
        });

    }

    private void unevenSplit() {

        regions = new ImprovedQuadTree[4];
        int newLevel = level + 1;

        if (zone.xStep * gridWidth  >= zone.yStep * gridHeight) {
//            System.out.println("-----splitX-----");
            splitX(zone, newLevel, nodeNum, true,0);
            if (regions[0] != null && regions[0].getNodeNum() > maxItemsPerZone) {
                if (regions[0].getZone().xStep * gridWidth  >= 2*regions[0].getZone().yStep * gridHeight) splitX(regions[0].getZone(), newLevel, regions[0].getNodeNum(), false,0);
                else splitY(regions[0].getZone(), newLevel, regions[0].getNodeNum(), false,0);
            }
            if (regions[2] != null && regions[2].getNodeNum() > maxItemsPerZone) {
                if (regions[2].getZone().xStep * gridWidth  >= 2*regions[2].getZone().yStep * gridHeight) splitX(regions[2].getZone(), newLevel, regions[2].getNodeNum(), false,2);
                else splitY(regions[2].getZone(), newLevel, regions[2].getNodeNum(), false,2);
            }
        } else {
//            System.out.println("-----splitY-----");
            splitY(zone, newLevel, nodeNum, true,0);
            if (regions[0] != null && regions[0].getNodeNum() > maxItemsPerZone) {
                if (regions[0].getZone().yStep * gridHeight  >= 2*regions[0].getZone().xStep * gridWidth) splitY(regions[0].getZone(), newLevel, regions[0].getNodeNum(), false,0);
                else splitX(regions[0].getZone(), newLevel, regions[0].getNodeNum(), false,0);
            }
            if (regions[2] != null && regions[2].getNodeNum() > maxItemsPerZone) {
                if (regions[2].getZone().yStep * gridHeight  >= 2*regions[2].getZone().xStep * gridWidth) splitY(regions[2].getZone(), newLevel, regions[2].getNodeNum(), false,2);
                else splitX(regions[2].getZone(), newLevel, regions[2].getNodeNum(), false,2);
            }
        }

    }

    private int getzonePrefixSum(int x1,int y1,int x2,int y2){
        return (prefixSum[x2+1][y2+1]-prefixSum[x1][y2+1]-prefixSum[x2+1][y1]+prefixSum[x1][y1]);
    }

    private void splitX(QuadRectangle zone, int level, int nodeSum, boolean isFirst, int index) {
//        System.out.println("zone: "+zone);
        int minxAxis = 0;
        int minxSum = Integer.MAX_VALUE;
        int leftNodeSum;
        int minLeftNodeSum = 0;
        int rightNodeSum;
        int minRightNodeSum = 0;
        for (int i = 0; i <= zone.xStep / 2; i++) {
            int left = zone.gridX +zone.xStep / 2-i;
            leftNodeSum=getzonePrefixSum(zone.gridX,zone.gridY,left,zone.gridY+zone.yStep);
            if(leftNodeSum>=minItemsPerZone && (nodeSum - leftNodeSum) >= minItemsPerZone){
                int leftxSum=getzonePrefixSum(left,zone.gridY,left,zone.gridY+zone.yStep);
                if(leftxSum==0){
                    minxAxis = left;
                    minxSum = 0;
                    minLeftNodeSum = leftNodeSum;
                    minRightNodeSum = nodeSum - leftNodeSum;
                    break;
                }else if(leftxSum < minxSum){
                    minxSum = leftxSum;
                    minxAxis = left;
                    minLeftNodeSum = leftNodeSum;
                    minRightNodeSum = nodeSum - leftNodeSum;
                }
            }
            int right = zone.gridX + zone.xStep/2 + i;
            rightNodeSum=getzonePrefixSum(right,zone.gridY,zone.gridX+zone.xStep,zone.gridY+zone.yStep);
            if(rightNodeSum>=minItemsPerZone && (nodeSum - rightNodeSum) >= minItemsPerZone){
                int rightxSum=getzonePrefixSum(right,zone.gridY,right,zone.gridY+zone.yStep);
                if(rightxSum==0){
                    minxAxis = right-1;
                    minxSum = 0;
                    minRightNodeSum = rightNodeSum;
                    minLeftNodeSum = nodeSum - rightNodeSum;
                    break;
                }else if(rightxSum < minxSum){
                    minxSum = rightxSum;
                    minxAxis = right-1;
                    minRightNodeSum = rightNodeSum;
                    minLeftNodeSum = nodeSum - rightNodeSum;
                }
            }
        }
//        System.out.println("gridX: "+zone.gridX +" xStep: " +zone.xStep+" minxAxis: "+minxAxis+" minxSum: "+minxSum+" minLeftNodeSum: "+minLeftNodeSum+" minRightNodeSum: "+minRightNodeSum);
        if (isFirst && minxSum != Integer.MAX_VALUE) {
            regions[0] = newUnevenQuadTree(new QuadRectangle(
                    zone.x,
                    zone.y,
                    (minxAxis - zone.gridX + 1) * gridWidth,
                    zone.height,
                    zone.gridX,
                    zone.gridY,
                    minxAxis - zone.gridX,
                    zone.yStep
            ), minLeftNodeSum, level);

            regions[2] = newUnevenQuadTree(new QuadRectangle(
                    zone.x + (minxAxis - zone.gridX + 1) * gridWidth,
                    zone.y,
                    zone.width - (minxAxis - zone.gridX + 1) * gridWidth,
                    zone.height,
                    minxAxis + 1,
                    zone.gridY,
                    zone.gridX + zone.xStep - minxAxis - 1,
                    zone.yStep
            ), minRightNodeSum, level);
        } else if (!isFirst && minxSum != Integer.MAX_VALUE) {
            regions[index] = newUnevenQuadTree(new QuadRectangle(
                    zone.x,
                    zone.y,
                    (minxAxis - zone.gridX + 1) * gridWidth,
                    zone.height,
                    zone.gridX,
                    zone.gridY,
                    minxAxis - zone.gridX,
                    zone.yStep
            ), minLeftNodeSum, level);

            regions[index+1] = newUnevenQuadTree(new QuadRectangle(
                    zone.x + (minxAxis - zone.gridX + 1) * gridWidth,
                    zone.y,
                    zone.width - (minxAxis - zone.gridX + 1) * gridWidth,
                    zone.height,
                    minxAxis + 1,
                    zone.gridY,
                    zone.gridX + zone.xStep - minxAxis - 1,
                    zone.yStep
            ), minRightNodeSum, level);
        }

    }

    private void splitY(QuadRectangle zone, int level, int nodeSum, boolean isFirst, int index) {
//        System.out.println("zone: "+zone);
        int minyAxis = 0;
        int minySum = Integer.MAX_VALUE;
        int topNodeSum;
        int minTopNodeSum = 0;
        int bottomNodeSum;
        int minBottomNodeSum = 0;
        for (int i = 0; i <= zone.yStep / 2; i++) {
            int bottom = zone.gridY +zone.yStep / 2-i;
            bottomNodeSum=getzonePrefixSum(zone.gridX,zone.gridY,zone.gridX+zone.xStep,bottom);
            if (bottomNodeSum >= minItemsPerZone && (nodeSum - bottomNodeSum) >= minItemsPerZone) {
                int bottomySum=getzonePrefixSum(zone.gridX,bottom,zone.gridX+zone.xStep,bottom);
                if(bottomySum == 0){
                    minyAxis = bottom;
                    minySum = 0;
                    minBottomNodeSum = bottomNodeSum;
                    minTopNodeSum = nodeSum - bottomNodeSum;
                    break;
                }else if(bottomySum < minySum){
                    minySum = bottomySum;
                    minyAxis = bottom;
                    minBottomNodeSum = bottomNodeSum;
                    minTopNodeSum = nodeSum - bottomNodeSum;
                }

            }


            int top = zone.gridY +zone.yStep / 2 + i;
            topNodeSum=getzonePrefixSum(zone.gridX,top,zone.gridX+zone.xStep,zone.gridY+zone.yStep);
            if (topNodeSum >= minItemsPerZone && (nodeSum - topNodeSum) >= minItemsPerZone) {
                int topySum=getzonePrefixSum(zone.gridX,top,zone.gridX+zone.xStep,top);
                if(topySum == 0){
                    minyAxis = top-1;
                    minySum = 0;
                    minTopNodeSum = topNodeSum;
                    minBottomNodeSum = nodeSum - topNodeSum;
                    break;
                }else if(topySum < minySum){
                    minySum = topySum;
                    minyAxis = top-1;
                    minTopNodeSum = topNodeSum;
                    minBottomNodeSum = nodeSum - topNodeSum;
                }
            }
        }
//        System.out.println("gridY: "+zone.gridY +" yStep: " +zone.yStep+" minyAxis: "+minyAxis+" minySum: "+minySum+" minTopNodeSum: "+minTopNodeSum+" minBottomNodeSum: "+minBottomNodeSum);
        if (isFirst && minySum != Integer.MAX_VALUE) {
            regions[0] = newUnevenQuadTree(new QuadRectangle(
                    zone.x,
                    zone.y + (minyAxis - zone.gridY + 1) * gridHeight,
                    zone.width,
                    zone.height - (minyAxis - zone.gridY + 1) * gridHeight,
                    zone.gridX,
                    minyAxis + 1,
                    zone.xStep,
                    zone.gridY + zone.yStep - minyAxis - 1
            ), minTopNodeSum, level);

            regions[2] = newUnevenQuadTree(new QuadRectangle(
                    zone.x,
                    zone.y,
                    zone.width,
                    (minyAxis - zone.gridY + 1) * gridHeight,
                    zone.gridX,
                    zone.gridY,
                    zone.xStep,
                    minyAxis - zone.gridY
            ), minBottomNodeSum, level);
        } else if (!isFirst && minySum != Integer.MAX_VALUE) {
            regions[index] = newUnevenQuadTree(new QuadRectangle(
                    zone.x,
                    zone.y + (minyAxis - zone.gridY + 1) * gridHeight,
                    zone.width,
                    zone.height - (minyAxis - zone.gridY + 1) * gridHeight,
                    zone.gridX,
                    minyAxis + 1,
                    zone.xStep,
                    zone.gridY + zone.yStep - minyAxis - 1
            ), minTopNodeSum, level);

            regions[index+1] = newUnevenQuadTree(new QuadRectangle(
                    zone.x,
                    zone.y,
                    zone.width,
                    (minyAxis - zone.gridY + 1) * gridHeight,
                    zone.gridX,
                    zone.gridY,
                    zone.xStep,
                    minyAxis - zone.gridY
            ), minBottomNodeSum, level);
        }

    }

    public void dropElements() {
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                tree.nodes.clear();
                return true;
            }
        });
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    private void traverse(Visitor<T> visitor) {
        if (!visitor.visit(this)) {
            return;
        }

        if (regions != null) {
            for (ImprovedQuadTree region : regions) {
                if (region!=null) region.traverse(visitor);
            }
        }
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     * lineage will memorize the traversal path for each nodes
     */
    private void traverseWithTrace(VisitorWithLineage<T> visitor, String lineage) {
        if (!visitor.visit(this, lineage)) {
            return;
        }

        if (regions != null) {
            for (int i = 0; i < regions.length; i++) {
                regions[i].traverseWithTrace(visitor, lineage + i);
            }
        }
    }

    private void addAllElements(final List<T> list) {
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                for (QuadNode<T> node : tree.nodes) {
                    list.add(node.element);
                }
                return true;
            }
        });
    }

    public boolean isLeaf() {
        boolean flag=true;
        if(regions!=null){
        for (ImprovedQuadTree region:regions
             ) {
            if(region!=null) flag=false;
        }
        }
//        return regions == null;
        return flag;
    }

    public void calculateLeafNodeZones() {
        traverse(new Visitor<T>() {

            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (tree.isLeaf()) {
                    List<QuadNode<T>> quadNodes = tree.getNodes();
                    GeometryFactory geometryFactory = new GeometryFactory();
                    Polygon[] polygons = new Polygon[quadNodes.size()];
                    for (int i = 0; i < quadNodes.size(); i++) {
                        QuadNode<T> node = quadNodes.get(i);
                        double x0 = node.r.x;
                        double y0 = node.r.y;
                        double x1 = node.r.x1;
                        double y1 = node.r.y1;
                        Coordinate[] coordinates = new Coordinate[]{
                                new Coordinate(x0, y0),
                                new Coordinate(x0, y1),
                                new Coordinate(x1, y1),
                                new Coordinate(x1, y0),
                                new Coordinate(x0, y0)
                        };
                        Polygon polygon = geometryFactory.createPolygon(coordinates);
                        polygons[i] = polygon;
                    }
                    tree.setLeafNodeZone(geometryFactory.createMultiPolygon(polygons));
                }
                return true;
            }
        });
    }

    public List<QuadRectangle> findZones(QuadRectangle r) {
        final Envelope envelope = r.getEnvelope();

        final List<QuadRectangle> matches = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (!disjoint(tree.zone.getEnvelope(), envelope)) {
                    if (tree.isLeaf()) {
                        matches.add(tree.zone);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        });

        return matches;
    }

    public List<Pair<QuadRectangle, Integer>> findZonesWithNum(QuadRectangle r) {
        final Envelope envelope = r.getEnvelope();

        final List<Pair<QuadRectangle, Integer>> matches = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (!disjoint(tree.zone.getEnvelope(), envelope)) {
                    if (tree.isLeaf()&&tree.getNodeNum()>0) {
//                        System.out.println("leaf: "+tree.zone);
                        matches.add(Pair.of(tree.zone, tree.getNodeNum()));
                    }
                    return true;
                } else {
                    return false;
                }
            }
        });

        return matches;
    }

    public List<QuadRectangle> findLeafNodeZones(Envelope e) {

        final List<QuadRectangle> matches = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {

                if (!disjoint(tree.zone.getEnvelope(), e)) {
//                    System.out.println("leafNodeZone: " + tree.getLeafNodeZone());
//                    if (tree.isLeaf() && tree.getNodeNum() > 0 && joint(tree.getLeafNodeZone(), e)) {
                    if (tree.isLeaf() && tree.getNodeNum() > 0) {
                        matches.add(tree.zone);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        });

        return matches;
    }

    public List<QuadRectangle> findTrueZones(Envelope e) {

        final List<QuadRectangle> matches = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {

                if (tree.getNodeNum() > 0 && (!disjoint(tree.trueZone, e))) {
//                    System.out.println("trueZone: " + tree.getTrueZone());
//                    System.out.println("Zone: " + tree.getZone());
                    if (tree.isLeaf()) {
                        matches.add(tree.zone);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        });

        return matches;
    }

    private boolean disjoint(Envelope r1, Envelope r2) {
        return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
    }

    public void assignPartitionIds() {
        traverse(new Visitor<T>() {
            private int partitionId = 0;

            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {

                if (tree.isLeaf() && tree.getNodeNum() > 0) {
                    tree.getZone().partitionId = partitionId;
                    System.out.println(tree.zone+" "+tree.getNodeNum());
                    partitionId++;
                }
                return true;
            }
        });
    }


    public void assignPartitionLineage() {
        traverseWithTrace(new VisitorWithLineage<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree, String lineage) {
                if (tree.isLeaf()) {
                    tree.getZone().lineage = lineage;
                }
                return true;
            }
        }, "");
    }

    @Override
    public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry) {
        Objects.requireNonNull(geometry, "spatialObject");

        final Envelope envelope = geometry.getEnvelopeInternal();

        final List<Pair<QuadRectangle, Integer>> matchedPartitions = findZonesWithNum(new QuadRectangle(envelope));

//        System.out.println("size: "+matchedPartitions.size());
        final Point point = geometry instanceof Point ? (Point) geometry : null;

        final Set<Tuple2<Integer, Geometry>> result = new HashSet<>();

        Pair<QuadRectangle, Integer> maxPair = null;
        int maxValue = Integer.MIN_VALUE;

        for (Pair<QuadRectangle, Integer> rectangle : matchedPartitions) {
            if (point != null) {
                int value = rectangle.getValue();
                // make sure to return only one partition
                if (value > maxValue) {
                    maxValue = value;
                    maxPair = rectangle;
//                    System.out.println("partitionId: "+maxPair.getLeft().partitionId);
//                    System.out.println("x: " + point.getX() + ", y: " + point.getY() + ", " + point.getUserData() + ", " + matchedPartitions);

                }
            }

            if (geometry instanceof Circle) {
                Circle circle = (Circle) geometry;
                if (new HalfOpenRectangle(rectangle.getLeft().getEnvelope()).contains(circle.getCenterGeometry().getCentroid())) {
                    continue;
                }
            }
        }
        if (point != null) {
            if (maxPair == null || maxPair.getLeft().partitionId == null)
                System.out.println("x: " + point.getX() + ", y: " + point.getY() + ", " + point.getUserData() + ", " + matchedPartitions);
            if (maxPair != null) result.add(new Tuple2(maxPair.getLeft().partitionId, geometry));
        }
        return result.iterator();
    }

    public Iterator<Tuple2<Integer, Geometry>> placeObjectWithFilter(Geometry geometry) {
        Objects.requireNonNull(geometry, "spatialObject");

        final Envelope envelope = geometry.getEnvelopeInternal();

        final List<QuadRectangle> matchedPartitions = findLeafNodeZones(envelope);
//        final List<QuadRectangle> matchedPartitions = findTrueZones(envelope);

//        final Point point = geometry instanceof Point ? (Point) geometry : null;

        final Set<Tuple2<Integer, Geometry>> result = new HashSet<>();
        for (QuadRectangle rectangle : matchedPartitions) {
            // For points, make sure to return only one partition
//            if (point != null && !(new HalfOpenRectangle(rectangle.getEnvelope())).contains(point)) {
//                continue;
//            }

//            if (geometry instanceof Circle) {
//                Circle circle = (Circle) geometry;
//                if (new HalfOpenRectangle(rectangle.getEnvelope()).contains(circle.getCenterGeometry().getCentroid())) {
//                    continue;
//                }
//            }

            result.add(new Tuple2(rectangle.partitionId, geometry));
        }

        return result.iterator();
    }

    @Override
    public Set<Integer> getKeys(Geometry geometry) {
        Objects.requireNonNull(geometry, "spatialObject");

        final Envelope envelope = geometry.getEnvelopeInternal();

        final List<QuadRectangle> matchedPartitions = findZones(new QuadRectangle(envelope));

        final Point point = geometry instanceof Point ? (Point) geometry : null;

        final Set<Integer> result = new HashSet<>();
        for (QuadRectangle rectangle : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(rectangle.getEnvelope())).contains(point)) {
                continue;
            }

            result.add(rectangle.partitionId);
        }

        return result;
    }

    @Override
    public List<Envelope> fetchLeafZones() {
        final List<Envelope> leafZones = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (tree.isLeaf() && tree.getNodeNum() > 0) {
                    leafZones.add(tree.zone.getEnvelope());
//                    leafZones.add(tree.getTrueZone());
                }
                return true;
            }
        });
        return leafZones;
    }

    public List<Pair<Envelope, Integer>> fetchLeafZonesWithNum() {
        final List<Pair<Envelope, Integer>> leafZones = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (tree.isLeaf()) {
                    leafZones.add(Pair.of(tree.zone.getEnvelope(), tree.getNodeNum()));
                }
                return true;
            }
        });
        return leafZones;
    }

    public List<MultiPolygon> fetchLeafNodeZones() {
        final List<MultiPolygon> leafNodeZones = new ArrayList<>();
        traverse(new Visitor<T>() {
            @Override
            public boolean visit(ImprovedQuadTree<T> tree) {
                if (tree.isLeaf() && tree.getNodeNum() > 0) {

                    leafNodeZones.add(tree.getLeafNodeZone());
                }
                return true;
            }
        });
        return leafNodeZones;
    }



    private interface Visitor<T> {
        /**
         * Visits a single node of the tree
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(ImprovedQuadTree<T> tree);
    }

    private interface VisitorWithLineage<T> {
        /**
         * Visits a single node of the tree, with the traversal trace
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(ImprovedQuadTree<T> tree, String lineage);
    }
}
