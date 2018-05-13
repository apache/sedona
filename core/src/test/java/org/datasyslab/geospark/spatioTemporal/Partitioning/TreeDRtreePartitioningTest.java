/*
 * FILE: TreeDRtreePartitioningTest
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

package org.datasyslab.geospark.spatioTemporal.Partitioning;

import java.util.ArrayList;
import java.util.List;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.spatioTemporalPartitioning.OctreePartitioning;
import org.junit.Assert;
import org.junit.Test;

public class TreeDRtreePartitioningTest {

    /**
     * Verifies that data skew doesn't cause java.lang.StackOverflowError
     * in 3DStandardQuadTree.insert
     */
    @Test
    public void testDatSkew()
            throws Exception
    {

        final List<Cube> samples = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            samples.add(new Cube(0, 0, 0, 0, 0, 0));
        }

        final Cube extent = new Cube(0, 1, 0, 1, 0, 1);

        // Make sure 3DQuad-tree is built successfully without throwing
        // java.lang.StackOverflowError
        OctreePartitioning partitioning = new OctreePartitioning(samples, extent, 10);
        Assert.assertNotNull(partitioning.getPartitionTree());
    }


}
