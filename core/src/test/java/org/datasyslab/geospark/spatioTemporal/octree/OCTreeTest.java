/*
 * FILE: QuadTreeTest
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
package org.datasyslab.geospark.spatioTemporal.octree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.datasyslab.geospark.spatioTemporalPartitioning.octree.OctreeRectangle;
import org.datasyslab.geospark.spatioTemporalPartitioning.octree.Octree;
import org.junit.Test;

public class OCTreeTest
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
            Octree<OctreeRectangle> quadTree = new Octree<>(
                    new OctreeRectangle(0, 0, 0, 10, 10, 10), 0, 1, 2);

            OctreeRectangle r1 = new OctreeRectangle(1, 1, 1, 1, 1, 1);
            OctreeRectangle r2 = new OctreeRectangle(2, 2, 1, 1, 1, 1);
            OctreeRectangle r3 = new OctreeRectangle(4, 4, 1, 1, 1, 1);
            OctreeRectangle r4 = new OctreeRectangle(6, 6, 1, 1, 1, 1);
            OctreeRectangle r5 = new OctreeRectangle(4, 4, 1, 2, 2, 1);
            OctreeRectangle r6 = new OctreeRectangle(0.5f, 6.5f, 1f, 0.5f, 0.5f, 1f);

            for (OctreeRectangle r : Arrays.asList(r1, r2, r3, r4, r5, r6)) {
                quadTree.insert(r, r);
            }

            List<OctreeRectangle> list = quadTree.getElements(new OctreeRectangle(2, 2, 1, 1, 1, 1));

            assertEqualElements(Arrays.asList(r1, r5, r2, r3), list);

            list = quadTree.getElements(new OctreeRectangle(4, 2, 1, 1, 1, 1));
            assertEqualElements(Arrays.asList(r1, r5, r2, r3), list);

            list = quadTree.getElements(new OctreeRectangle(3, 1, 1, 1, 1, 1));
            assertEqualElements(Arrays.asList(r2, r5), list);

            list = quadTree.getElements(new OctreeRectangle(0, 6, 1, 1, 1, 1));
            assertEqualElements(Arrays.asList(r6, r5), list);

            list = quadTree.getElements(new OctreeRectangle(2, 2, 1, 10, 10, 1));
            assertEqualElements(Arrays.asList(r1, r2, r3, r4, r5, r6), list);

            final List<OctreeRectangle> zones = quadTree.getAllZones();
            assertEquals(17, zones.size());

            final int leafNodeCount = quadTree.getTotalNumLeafNode();
            assertEquals(15, leafNodeCount);

            {
                final List<OctreeRectangle> matches =
                        quadTree.findZones(new OctreeRectangle(1.1, 0.8, 1, 1, 1, 1));
                assertEquals(1, matches.size());
                assertEquals(new OctreeRectangle(0, 0, 0, 2.5, 2.5, 2.5), matches.get(0));
            }

            {
                final List<OctreeRectangle> matches =
                        quadTree.findZones(new OctreeRectangle(1.1, 0.8, 1, 10, 10, 1));
                assertEquals(7, matches.size());
            }
        }
    }

    private void assertEqualElements(List<OctreeRectangle> expected, List<OctreeRectangle> actual)
    {
        assertEquals(expected.size(), actual.size());
        for (OctreeRectangle r : actual) {
            assertTrue(expected.contains(r));
        }
    }

    @Test
    public void testIntersectElementsAreInserted()
    {
        Octree<OctreeRectangle> quadTree = new Octree<>(new OctreeRectangle(0, 0, 1,
                10,
                10, 1), 0, 1, 2);

        OctreeRectangle r1 = new OctreeRectangle(1, 1 ,1, 1, 1, 1);
        OctreeRectangle r2 = new OctreeRectangle(2, 2, 1, 1, 1, 1);

        quadTree.insert(r1, r1);
        quadTree.insert(r2, r2);

        List<OctreeRectangle> list = quadTree.getElements(new OctreeRectangle(2, 2, 1, 1, 1, 1));
        assertEqualElements(Arrays.asList(r1, r2), list);
    }

    @Test
    public void testPixelQuadTree()
    {
        Octree<OctreeRectangle> quadTree = new Octree<OctreeRectangle>(new
                OctreeRectangle(0, 0, 0, 10, 10, 10), 0, 5, 5);

        OctreeRectangle r1 = new OctreeRectangle(1, 1, 1, 0, 0, 0);
        OctreeRectangle r2 = new OctreeRectangle(2, 2, 2, 0, 0, 0);
        OctreeRectangle r3 = new OctreeRectangle(4, 4, 4, 0, 0, 0);
        OctreeRectangle r4 = new OctreeRectangle(6, 6, 6, 0, 0, 0);
        OctreeRectangle r5 = new OctreeRectangle(4, 4, 4, 2, 2, 2);
        OctreeRectangle r6 = new OctreeRectangle(0.5f, 6.5f, 1, 0.5f, 0.5f, 1);

        for (OctreeRectangle r : Arrays.asList(r1, r2, r3, r4, r5, r6)) {
            quadTree.insert(r, r);
        }

        assertEquals(new OctreeRectangle(0, 0, 0, 5, 5, 5), quadTree.getZone(1, 1, 1));
        assertEquals(new OctreeRectangle(5, 0, 0, 5, 5, 5), quadTree.getZone(6, 1, 1));
        assertEquals(new OctreeRectangle(0, 5, 0, 5, 5, 5), quadTree.getZone(5, 5, 1));
        assertEquals(new OctreeRectangle(5, 5, 0, 5, 5, 5), quadTree.getZone(7, 8, 1));
    }

    @Test
    public void testQuadTreeForceGrow()
    {
        int resolutionX = 100000;
        int resolutionY = 100000;
        int resolutionZ = 100000;

        Octree<OctreeRectangle> quadTree = new Octree<>(new OctreeRectangle
                (0, 0, 0, resolutionX, resolutionY, resolutionZ), 0, 4, 10);
        quadTree.forceGrowUp(4);
        int leafPartitionNum = quadTree.getTotalNumLeafNode();
        assertEquals(4096, leafPartitionNum);

        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            int z = ThreadLocalRandom.current().nextInt(0, resolutionZ);
            OctreeRectangle newR = new OctreeRectangle(x, y, z, 1, 1, 1);
            quadTree.insert(newR, newR);
        }

        quadTree.assignPartitionIds();

        for (int i = 0; i < 100000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
            int z = ThreadLocalRandom.current().nextInt(0, resolutionZ);
            OctreeRectangle newR = new OctreeRectangle(x, y, z, 1, 1, 1);

            final List<OctreeRectangle> zones = quadTree.findZones(newR);
            assertFalse(zones.isEmpty());
            for (OctreeRectangle zone : zones) {
                assertTrue(zone.partitionId >= 0);
            }
        }
    }
}