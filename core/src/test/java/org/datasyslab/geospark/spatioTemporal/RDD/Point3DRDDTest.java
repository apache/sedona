/*
 * FILE: Point3DRDDTest
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
package org.datasyslab.geospark.spatioTemporal.RDD;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalRDD.Point3DRDD;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.SpatioTemporalGridType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc

/**
 * The Class Point3DRDDTest.
 */
public class Point3DRDDTest
        extends SpatioTemporalRDDTestBase
{


    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(Point3DRDDTest.class.getSimpleName(), "point3D.test.properties");
    }

    /**
     * Test constructor.
     *
     * @throws Exception the exception
     */
    /*
        This test case will load a sample data file and
     */
    @Test
    public void testConstructor()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());;
    }

    /**
     * Test 3DR tree spatial temporal partitioing.
     *
     * @throws Exception the exception
     */

    @Test
    public void test3DRTreeSpatioTemporalPartitioing()
            throws Exception
    {
        Point3DRDD spatialRDD = new Point3DRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatioTemporalPartitioning(SpatioTemporalGridType.RTREE3D);
        for (Cube d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d);
        }
        assert spatialRDD.countWithoutDuplicatesSPRDD() == spatialRDD.countWithoutDuplicates();
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
        Point3DRDD spatialRDD = new Point3DRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        // build tree and partitioner
        spatialRDD.spatioTemporalPartitioning(SpatioTemporalGridType.EQUALTIMESLICE);
        spatialRDD.buildIndex(IndexType.RTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryCube.envelope);
        }
        else {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryCube.envelope);
        }
    }

    /**
     * Test parse the time in data
     *
     * @throws Exception the exception
     */
    @Test
    public void testBoundary()
            throws Exception
    {
        Point3DRDD spatialRDD = new Point3DRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        // build tree and partitioner
        spatialRDD.spatioTemporalPartitioning(SpatioTemporalGridType.OCTREE);
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        Cube cube = spatialRDD.boundaryCube;
        Date minDate = new Date((long) cube.getMinZ());
        Date maxDate = new Date((long) cube.getMaxZ());
        String minTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(minDate);
        String maxTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(maxDate);
        assert minTime != null;
        assert maxTime != null;
    }


    /**
     * Test build octree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testTreeDQuadTreeSpatialPartitioing()
            throws Exception
    {
        Point3DRDD spatialRDD = new Point3DRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        // build tree and partitioner
        spatialRDD.spatioTemporalPartitioning(SpatioTemporalGridType.OCTREE);
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryCube.envelope);
        }
        else {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryCube.envelope);
            int a = 1;
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

    public static void main(String[] args) {
        String time = "2010-10-12 00:21:28";
        DateFormat format =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = format.parse(time);
            double doubleOfDate = date.getTime();
            System.out.println(doubleOfDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}