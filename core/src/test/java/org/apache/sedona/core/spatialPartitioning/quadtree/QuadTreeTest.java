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
package org.apache.sedona.core.spatialPartitioning.quadtree;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuadTreeTest
{

    @Test
    public void testInsertElements()
    {

        long startTime;
        long endTime;
        int maxTest = 1000000;

//        startTime = System.currentTimeMillis();
//        for (int i = 0; i <= maxTest; i++)
        {
            StandardQuadTree<QuadRectangle> quadTree = new StandardQuadTree<>(new QuadRectangle(0, 0, 10, 10), 0, 1, 2);

            QuadRectangle r1 = new QuadRectangle(1, 1, 1, 1);
            QuadRectangle r2 = new QuadRectangle(2, 2, 1, 1);
            QuadRectangle r3 = new QuadRectangle(4, 4, 1, 1);
            QuadRectangle r4 = new QuadRectangle(6, 6, 1, 1);
            QuadRectangle r5 = new QuadRectangle(4, 4, 2, 2);
            QuadRectangle r6 = new QuadRectangle(0.5f, 6.5f, 0.5f, 0.5f);

            for (QuadRectangle r : Arrays.asList(r1, r2, r3, r4, r5, r6)) {
                quadTree.insert(r, r);
            }

            List<QuadRectangle> list = quadTree.getElements(new QuadRectangle(2, 2, 1, 1));

            assertEqualElements(Arrays.asList(r1, r5, r2, r3), list);

            list = quadTree.getElements(new QuadRectangle(4, 2, 1, 1));
            assertEqualElements(Arrays.asList(r1, r5, r2, r3), list);

            list = quadTree.getElements(new QuadRectangle(3, 1, 1, 1));
            assertEqualElements(Arrays.asList(r2, r5), list);

            list = quadTree.getElements(new QuadRectangle(0, 6, 1, 1));
            assertEqualElements(Arrays.asList(r6, r5), list);

            list = quadTree.getElements(new QuadRectangle(2, 2, 10, 10));
            assertEqualElements(Arrays.asList(r1, r2, r3, r4, r5, r6), list);

            final List<QuadRectangle> zones = quadTree.getAllZones();
            assertEquals(9, zones.size());

            final int leafNodeCount = quadTree.getTotalNumLeafNode();
            assertEquals(7, leafNodeCount);

            {
                final List<QuadRectangle> matches =
                        quadTree.findZones(new QuadRectangle(1.1, 0.8, 1, 1));
                assertEquals(1, matches.size());
                assertEquals(new QuadRectangle(0, 0, 2.5, 2.5), matches.get(0));
            }

            {
                final List<QuadRectangle> matches =
                        quadTree.findZones(new QuadRectangle(1.1, 0.8, 10, 10));
                assertEquals(7, matches.size());
            }
        }
//        endTime = System.currentTimeMillis();
//        System.out.println("Total execution time hoho: " + (endTime - startTime) + "ms");
    }

    private void assertEqualElements(List<QuadRectangle> expected, List<QuadRectangle> actual)
    {
        assertEquals(expected.size(), actual.size());
        for (QuadRectangle r : actual) {
            assertTrue(expected.contains(r));
        }
    }

    @Test
    public void testIntersectElementsAreInserted()
    {
        StandardQuadTree<QuadRectangle> quadTree = new StandardQuadTree<>(new QuadRectangle(0, 0, 10, 10), 0, 1, 2);

        QuadRectangle r1 = new QuadRectangle(1, 1, 1, 1);
        QuadRectangle r2 = new QuadRectangle(2, 2, 1, 1);

        quadTree.insert(r1, r1);
        quadTree.insert(r2, r2);

        List<QuadRectangle> list = quadTree.getElements(new QuadRectangle(2, 2, 1, 1));
        assertEqualElements(Arrays.asList(r1, r2), list);
    }

    @Test
    public void testPixelQuadTree()
    {
        StandardQuadTree<QuadRectangle> quadTree = new StandardQuadTree<QuadRectangle>(new QuadRectangle(0, 0, 10, 10), 0, 5, 5);

        QuadRectangle r1 = new QuadRectangle(1, 1, 0, 0);
        QuadRectangle r2 = new QuadRectangle(2, 2, 0, 0);
        QuadRectangle r3 = new QuadRectangle(4, 4, 0, 0);
        QuadRectangle r4 = new QuadRectangle(6, 6, 0, 0);
        QuadRectangle r5 = new QuadRectangle(4, 4, 2, 2);
        QuadRectangle r6 = new QuadRectangle(0.5f, 6.5f, 0.5f, 0.5f);

        for (QuadRectangle r : Arrays.asList(r1, r2, r3, r4, r5, r6)) {
            quadTree.insert(r, r);
        }

        assertEquals(new QuadRectangle(0, 0, 5, 5), quadTree.getZone(1, 1));
        assertEquals(new QuadRectangle(5, 0, 5, 5), quadTree.getZone(6, 1));
        assertEquals(new QuadRectangle(0, 5, 5, 5), quadTree.getZone(5, 5));
        assertEquals(new QuadRectangle(5, 5, 5, 5), quadTree.getZone(7, 8));
    }

    @Test
    public void testQuadTreeForceGrow()
    {
        int resolutionX = 100000;
        int resolutionY = 100000;

        StandardQuadTree<QuadRectangle> quadTree = new StandardQuadTree<>(new QuadRectangle(0, 0, resolutionX, resolutionY), 0, 4, 10);
        quadTree.forceGrowUp(4);
        int leafPartitionNum = quadTree.getTotalNumLeafNode();
        assertEquals(256, leafPartitionNum);

        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            QuadRectangle newR = new QuadRectangle(x, y, 1, 1);
            quadTree.insert(newR, newR);
        }

        quadTree.assignPartitionIds();

        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            QuadRectangle newR = new QuadRectangle(x, y, 1, 1);

            final List<QuadRectangle> zones = quadTree.findZones(newR);
            assertFalse(zones.isEmpty());
            for (QuadRectangle zone : zones) {
                assertTrue(zone.partitionId >= 0);
            }
        }
    }

    @Test
    public void testQuadTreePartitionLineage()
    {
        int resolutionX = 100000;
        int resolutionY = 100000;

        StandardQuadTree<QuadRectangle> quadTree = new StandardQuadTree<>(new QuadRectangle(0, 0, resolutionX, resolutionY), 0, 4, 10);
        quadTree.forceGrowUp(4);
        int leafPartitionNum = quadTree.getTotalNumLeafNode();
        assertEquals(256, leafPartitionNum);

        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            QuadRectangle newR = new QuadRectangle(x, y, 1, 1);
            quadTree.insert(newR, newR);
        }

        quadTree.assignPartitionLineage();

        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            QuadRectangle newR = new QuadRectangle(x, y, 1, 1);

            final List<QuadRectangle> zones = quadTree.findZones(newR);
            assertFalse(zones.isEmpty());
            for (QuadRectangle zone : zones) {
                assertTrue(zone.lineage != null);
            }
        }
    }
}