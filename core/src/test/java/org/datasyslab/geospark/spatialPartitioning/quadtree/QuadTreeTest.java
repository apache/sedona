/**
 * FILE: QuadTreeTest.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.quadtree.QuadTreeTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuadTreeTest {

    @Test
    public void testInsertElements() {

        long startTime;
        long endTime;
        int maxTest = 1000000;

//        startTime = System.currentTimeMillis();
//        for (int i = 0; i <= maxTest; i++) {

            StandardQuadTree.maxItemByNode = 1;
            StandardQuadTree.maxLevel = 2;

            StandardQuadTree<QuadRectangle> standardQuadTree = new StandardQuadTree<QuadRectangle>(new QuadRectangle(0, 0, 10, 10), 0);

            QuadRectangle r1 = new QuadRectangle(1, 1, 1, 1);
            QuadRectangle r2 = new QuadRectangle(2, 2, 1, 1);
            QuadRectangle r3 = new QuadRectangle(4, 4, 1, 1);
            QuadRectangle r4 = new QuadRectangle(6, 6, 1, 1);
            QuadRectangle r5 = new QuadRectangle(4, 4, 2, 2);
            QuadRectangle r6 = new QuadRectangle(0.5f, 6.5f, 0.5f, 0.5f);

            standardQuadTree.insert(r1, r1);
            standardQuadTree.insert(r2, r2);
            standardQuadTree.insert(r3, r3);
            standardQuadTree.insert(r4, r4);
            standardQuadTree.insert(r5, r5);
            standardQuadTree.insert(r6, r6);

            ArrayList<QuadRectangle> list = new ArrayList<QuadRectangle>();
            standardQuadTree.getElements(list, new QuadRectangle(2, 2, 1, 1));

            ArrayList<QuadRectangle> expected = new ArrayList<QuadRectangle>();
            expected.add(r1);
            expected.add(r5);
            expected.add(r2);
            expected.add(r3);
            for(QuadRectangle r:list)
            {
            	assert expected.contains(r);
            }
            assertEquals(expected.size(), list.size());

            list.clear();
            standardQuadTree.getElements(list, new QuadRectangle(4, 2, 1, 1));

            expected.clear();
            expected.add(r1);
            expected.add(r5);
            expected.add(r2);
            expected.add(r3);

            ArrayList<QuadRectangle> zoneList = new ArrayList<QuadRectangle>();
            standardQuadTree.getAllZones(zoneList);

            assertEquals(zoneList.size(), 9);
//        }
//        endTime = System.currentTimeMillis();
//        System.out.println("Total execution time hoho: " + (endTime - startTime) + "ms");
    }

    @Test
    public void testIntersectElementsAreInserted() {
        StandardQuadTree.maxItemByNode = 1;
        StandardQuadTree.maxLevel = 2;

        StandardQuadTree<QuadRectangle> standardQuadTree = new StandardQuadTree<QuadRectangle>(new QuadRectangle(0, 0, 10, 10), 0);

        QuadRectangle r1 = new QuadRectangle(1, 1, 1, 1);
        QuadRectangle r2 = new QuadRectangle(2, 2, 1, 1);

        standardQuadTree.insert(r1, r1);
        standardQuadTree.insert(r2, r2);

        ArrayList<QuadRectangle> list = new ArrayList<QuadRectangle>();
        standardQuadTree.getElements(list, new QuadRectangle(2, 2, 1, 1));

        assertTrue(list.size() == 2);


        QuadRectangle r3 = new QuadRectangle(11, 11, 1, 1);

        list = new ArrayList<QuadRectangle>();
        standardQuadTree.getElements(list, new QuadRectangle(2, 2, 1, 1));
    }

    @Test
    public void testPixelQuadTree() {
        StandardQuadTree.maxItemByNode = 5;
        StandardQuadTree.maxLevel = 5;

        StandardQuadTree<QuadRectangle> standardQuadTree = new StandardQuadTree<QuadRectangle>(new QuadRectangle(0, 0, 10, 10), 0);

        QuadRectangle r1 = new QuadRectangle(1, 1, 0, 0);
        QuadRectangle r2 = new QuadRectangle(2, 2, 0, 0);
        QuadRectangle r3 = new QuadRectangle(4, 4, 0, 0);
        QuadRectangle r4 = new QuadRectangle(6, 6, 0, 0);
        QuadRectangle r5 = new QuadRectangle(4, 4, 2, 2);
        QuadRectangle r6 = new QuadRectangle(0.5f, 6.5f, 0.5f, 0.5f);

        standardQuadTree.insert(r1, r1);
        standardQuadTree.insert(r2, r2);
        standardQuadTree.insert(r3, r3);
        standardQuadTree.insert(r4, r4);
        standardQuadTree.insert(r5, r5);
        standardQuadTree.insert(r6, r6);

        ArrayList<QuadRectangle> zoneList = new ArrayList<QuadRectangle>();
        ArrayList<QuadRectangle> matchedZoneList = new ArrayList<QuadRectangle>();

        standardQuadTree.getAllZones(zoneList);
        for(QuadRectangle r:zoneList)
        {
            if (r.contains(1,1))
            {
                matchedZoneList.add(r);
            }
        }
        QuadRectangle matchedZone = standardQuadTree.getZone(1,1);
        assert matchedZoneList.contains(matchedZone);
    }

    @Test
    public void testQuadTreeForceGrow() {
        StandardQuadTree.maxItemByNode = 4;
        StandardQuadTree.maxLevel = 10;

        int resolutionX = 100000;
        int resolutionY = 100000;

        StandardQuadTree<QuadRectangle> standardQuadTree = new StandardQuadTree<QuadRectangle>(new QuadRectangle(0, 0, resolutionX, resolutionY), 0);
        standardQuadTree.forceGrowUp(4);
        int leafPartitionNum = standardQuadTree.getTotalNumLeafNode();
        assert leafPartitionNum == 256;
    
        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            QuadRectangle newR = new QuadRectangle(x, y, 1, 1);
            standardQuadTree.insert(newR, newR);
        }
        HashSet<Integer> uniqueIdList = new HashSet<Integer>();
        standardQuadTree.getAllLeafNodeUniqueId(uniqueIdList);
        HashMap<Integer,Integer> serialIdMapping = standardQuadTree.getSeriaIdMapping(uniqueIdList);
        standardQuadTree.decidePartitionSerialId(serialIdMapping);
        ArrayList<QuadRectangle> allNumberedZoneList = new ArrayList<QuadRectangle>();
        standardQuadTree.getAllZones(allNumberedZoneList);
    }
    @Test
    public void testQuadTreeStressful() {
        StandardQuadTree.maxItemByNode = 1;
        StandardQuadTree.maxLevel = 10;

        int resolutionX = 100000;
        int resolutionY = 100000;

        StandardQuadTree<QuadRectangle> standardQuadTree = new StandardQuadTree<QuadRectangle>(new QuadRectangle(0, 0, resolutionX, resolutionY), 0);

        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            QuadRectangle newR = new QuadRectangle(x, y, 1, 1);
            standardQuadTree.insert(newR, newR);
        }

        ArrayList<QuadRectangle> zoneList = new ArrayList<QuadRectangle>();
        ArrayList<QuadRectangle> matchedZoneList = new ArrayList<QuadRectangle>();

        standardQuadTree.getAllZones(zoneList);
        long  startTimeLoop = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++){
            int queryX = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int queryY = ThreadLocalRandom.current().nextInt(0, resolutionY);
            for (QuadRectangle r : zoneList) {
                if (r.contains(queryX,queryY)) {
                    //System.out.println(r);
                }
                }
        }
        long  stopTimeLoop = System.currentTimeMillis();
        System.out.println("Loop all zones consumed time: "+(stopTimeLoop-startTimeLoop));
        long  startTimeTreeSearch = System.currentTimeMillis();
        /*
        for(int i = 0; i < 1000; i++)
        {
            int queryX = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int queryY = ThreadLocalRandom.current().nextInt(0, resolutionY);
            Pixel queryPixel = new Pixel(queryX, queryY, resolutionX, resolutionY);
            QuadRectangle matchedZone = standardQuadTree.getZone(queryX,queryY);
            //System.out.println(matchedZone);
        }
        */
        long  stopTimeTreeSearch = System.currentTimeMillis();
        System.out.println("Tree search consumed time: "+(stopTimeTreeSearch-startTimeTreeSearch));
        System.out.println("Total number of leaf nodes are "+ standardQuadTree.getTotalNumLeafNode());

    }
}