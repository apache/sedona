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

package org.apache.sedona.core.spatialPartitioning;

import org.apache.sedona.core.spatialPartitioning.ImprovedQT.ImprovedQuadTree;
import org.apache.sedona.core.spatialPartitioning.ImprovedQT.QuadRectangle;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class ImprovedQuadtreePartitioning<T extends Geometry>
        implements Serializable
{

    /**
     * The Improved Quad-Tree.
     */
    private final ImprovedQuadTree<Integer> partitionTree;


    public ImprovedQuadtreePartitioning(SpatialRDD<T> spatialRDD,
                                        int numPartitions)
            throws Exception {
        //----------start partitioning----------
        spatialRDD.analyze();
        Envelope boundary = spatialRDD.boundaryEnvelope;
        long totalCount = spatialRDD.approximateTotalCount;
        System.out.println("totalCount: " + totalCount);

        //find best gridScale
        int gridScale = (int) Math.pow(2, Math.ceil(Math.log(Math.sqrt(totalCount)) / Math.log(2)));
        System.out.println("gridScale:" + gridScale);
        if (gridScale > 256) gridScale = 256;
        System.out.println("gridScale:" + gridScale);
        long bestCount = totalCount / numPartitions;
        System.out.println("bestCount:" + bestCount);
        double rate = 1.0;
        int maxIter = 0;

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> gridXY_countRDD = null;
        while (rate >= 1&&maxIter<3) {
            double tempGridWidth = Math.ceil((boundary.getMaxX() - boundary.getMinX()) / gridScale * 10000) / 10000.0;
            double tempGridHeight = Math.ceil((boundary.getMaxY() - boundary.getMinY()) / gridScale * 10000) / 10000.0;
            gridXY_countRDD = spatialRDD.rawSpatialRDD.mapToPair(geometry -> {
                Point point = geometry instanceof Point ? (Point) geometry : null;
                assert point != null;
                int gridX = (point.getX() == boundary.getMinX()) ? 0 : (int) (Math.ceil(((point.getX() - boundary.getMinX()) / tempGridWidth)) - 1);
                int gridY = (point.getY() == boundary.getMinY()) ? 0 : (int) (Math.ceil(((point.getY() - boundary.getMinY()) / tempGridHeight)) - 1);
                return new Tuple2<>(new Tuple2<>(gridX, gridY), 1);
            }).reduceByKey(Integer::sum);

            int maxCount = gridXY_countRDD.max(new ValueComparator())._2;
            System.out.println("maxCount:" + maxCount);

            if (gridScale < 256) {
                rate = ((double) maxCount) / bestCount;
                System.out.println("rate:" + rate);
                gridScale = (int) (gridScale * Math.ceil(Math.sqrt(rate)));
                System.out.println("newGridScale:" + gridScale);
            }
            maxIter++;
        }
        System.out.println("gridScale:" + gridScale);
        double gridWidth = Math.ceil((boundary.getMaxX() - boundary.getMinX()) / gridScale * 10000) / 10000.0;
        double gridHeight = Math.ceil((boundary.getMaxY() - boundary.getMinY()) / gridScale * 10000) / 10000.0;
        spatialRDD.rawSpatialRDD= (JavaRDD<T>) spatialRDD.rawSpatialRDD.map(geometry->{
            Point point = geometry instanceof Point ? (Point) geometry : null;
            assert point != null;
            int gridX = (point.getX() == boundary.getMinX()) ? 0 : (int) (Math.ceil(((point.getX() - boundary.getMinX()) / gridWidth)) - 1);
            int gridY = (point.getY() == boundary.getMinY()) ? 0 : (int) (Math.ceil(((point.getY() - boundary.getMinY()) / gridHeight)) - 1);
            point.setUserData(point.getUserData().toString()+","+gridX+","+gridY);
            return point;
        });
//        double gridWidth = tempGridWidth;
//        double gridHeight = tempGridHeight;
        System.out.println("gridWidth: " + gridWidth);
        System.out.println("gridHeight: " + gridHeight);
//        gridXY_countRDD.persist(StorageLevel.MEMORY_ONLY());

        //collect grids and build partition
        int[][] gridCounts = new int[gridScale + 1][gridScale + 1];
        List<Tuple2<Tuple2<Integer, Integer>, Integer>> gridList = gridXY_countRDD.collect();
        for (Tuple2<Tuple2<Integer, Integer>, Integer> grid : gridList) {
            Tuple2<Integer, Integer> gridIndex = grid._1;
            int x = gridIndex._1;
            int y = gridIndex._2;
            gridCounts[x + 1][y + 1] = grid._2;
        }
        int[][] prefixSum = new int[gridScale + 1][gridScale + 1];
        for (int i = 1; i <= gridScale; i++) {
            for (int j = 1; j <= gridScale; j++) {
                prefixSum[i][j] = prefixSum[i - 1][j] + prefixSum[i][j - 1] - prefixSum[i - 1][j - 1] + gridCounts[i][j];
            }
        }
        gridCounts = null;
//        int minItemsPerZone,maxItemsPerZone,maxk;
//        if (totalCount > 1000) {
//            maxk=(int) (5*Math.log(totalCount) / Math.log(2.0));
//            maxItemsPerZone=Math.max((int)(totalCount / numPartitions),maxk*2);
//            minItemsPerZone=Math.max((int) (0.5*maxItemsPerZone),maxk);
//            System.out.println("maxItems: " + maxItemsPerZone + " minItems: " + minItemsPerZone);
//            partitionTree = new ImprovedQuadTree(new QuadRectangle(boundary, 0, 0, gridScale - 1, gridScale - 1), (int) totalCount, 0, maxItemsPerZone, minItemsPerZone, numPartitions, gridWidth, gridHeight,prefixSum);
//        } else {
//            maxk=(int) (totalCount / 20.0);
//            maxItemsPerZone=Math.max((int) (totalCount / numPartitions),maxk*2);
//            minItemsPerZone=Math.max((int) (0.5*maxItemsPerZone),maxk);
//            System.out.println("maxItems: " + maxItemsPerZone + " minItems: " + minItemsPerZone);
//            partitionTree = new ImprovedQuadTree(new QuadRectangle(boundary, 0, 0, gridScale - 1, gridScale - 1), (int) totalCount, 0,maxItemsPerZone, minItemsPerZone, numPartitions, gridWidth, gridHeight,prefixSum);
//        }
        int minItemsPerZone, maxItemsPerZone;
        maxItemsPerZone= (int) (1.2*bestCount);
        minItemsPerZone= (int) (0.8*bestCount);
        System.out.println("maxItems: " + maxItemsPerZone + " minItems: " + minItemsPerZone);
        partitionTree = new ImprovedQuadTree(new QuadRectangle(boundary, 0, 0, gridScale - 1, gridScale - 1), (int) totalCount, 0, maxItemsPerZone, minItemsPerZone, numPartitions, gridWidth, gridHeight,prefixSum);
        partitionTree.split();
        partitionTree.assignPartitionIds();
    }

    public ImprovedQuadTree getPartitionTree()
    {
        return this.partitionTree;
    }
    static class ValueComparator implements Serializable, Comparator<Tuple2<Tuple2<Integer, Integer>, Integer>> {
        @Override
        public int compare(Tuple2<Tuple2<Integer, Integer>, Integer> t1, Tuple2<Tuple2<Integer, Integer>, Integer> t2) {
            return t1._2 - t2._2;
        }

    }

}
