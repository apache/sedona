/*
 * FILE: PointRDDTest
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
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

// TODO: Auto-generated Javadoc

/**
 * The Class PointRDDTest.
 */
public class PointRDDTest
        extends SpatialRDDTestBase
{
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(PointRDDTest.class.getSimpleName(), "point.test.properties");
    }

    /**
     * Test constructor.
     *
     * @throws Exception the exception
     */
    @Test
    public void testConstructor()
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        assertEquals(inputCount, spatialRDD.approximateTotalCount);
        assertEquals(inputBoundary, spatialRDD.boundaryEnvelope);
        assert spatialRDD.rawSpatialRDD.take(9).get(0).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2");
        assert spatialRDD.rawSpatialRDD.take(9).get(2).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2");
        assert spatialRDD.rawSpatialRDD.take(9).get(4).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2");
        assert spatialRDD.rawSpatialRDD.take(9).get(8).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2");
    }


    @Test
    public void testEmptyConstructor()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.buildIndex(IndexType.RTREE, false);
        // Create an empty spatialRDD and manually assemble it
        PointRDD spatialRDDcopy = new PointRDD();
        spatialRDDcopy.rawSpatialRDD = spatialRDD.rawSpatialRDD;
        spatialRDDcopy.indexedRawRDD = spatialRDD.indexedRawRDD;
        spatialRDDcopy.analyze();
    }

    /**
     * Test equal partitioning.
     *
     * @throws Exception the exception
     */
    @Test
    public void testEqualPartitioning()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, false, 10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.EQUALGRID);
        for (Envelope d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d);
        }
        assert spatialRDD.countWithoutDuplicates() == spatialRDD.countWithoutDuplicatesSPRDD();
    }

    /**
     * Test hilbert curve spatial partitioing.
     *
     * @throws Exception the exception
     */
    @Test
    public void testHilbertCurveSpatialPartitioing()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, false, 10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.HILBERT);
        for (Envelope d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        assert spatialRDD.countWithoutDuplicates() == spatialRDD.countWithoutDuplicatesSPRDD();
    }

    /**
     * Test R tree spatial partitioing.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeSpatialPartitioing()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, 10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.RTREE);
        for (Envelope d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d);
        }
        assert spatialRDD.countWithoutDuplicates() == spatialRDD.countWithoutDuplicatesSPRDD();
    }

    /**
     * Test voronoi spatial partitioing.
     *
     * @throws Exception the exception
     */
    @Test
    public void testVoronoiSpatialPartitioing()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, false, 10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.VORONOI);
        for (Envelope d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        assert spatialRDD.countWithoutDuplicates() == spatialRDD.countWithoutDuplicatesSPRDD();
    }

    /**
     * Test build index without set grid.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildIndexWithoutSetGrid()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.buildIndex(IndexType.RTREE, false);
    }

    /**
     * Test build rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildRtreeIndex()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.RTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
        else {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
    }

    /**
     * Test build quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildQuadtreeIndex()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
        else {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }
}