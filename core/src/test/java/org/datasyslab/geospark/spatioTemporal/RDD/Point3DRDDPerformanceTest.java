/*
 * FILE: Point3DRDDPerformanceTest
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.datasyslab.geospark.SpatioTemporalRDD.Point3DRDD;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.SpatioTemporalGridType;
import org.datasyslab.geospark.formatMapper.Point3DFormatMapper;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

/**
 * The Class Point3DRDDPerformanceTest.
 */
public class Point3DRDDPerformanceTest
        extends SpatioTemporalRDDTestBase
{

    private double beginTime = 1284000000000d;
    private double endTime = 1285000000000d;
    private static Cube queryCube = new Cube(0, 100, 0, 100, 1284000000000d, 1285000000000d);
    private static Envelope searchEnvelop = new Envelope(0, 100, 0, 100);


    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(Point3DRDDTest.class.getSimpleName(), "point3D.test.properties");
    }


    @Test
    public void runAll() {
        for (int i = 0; i < 5; i++) {
            testQueryInRawData();
        }
    }


    /**
     * Test query in raw data.
     */
    @Test
    public void testQueryInRawData() {
        long startTime=System.currentTimeMillis();
        JavaRDD<Point3D> rawRDD = sc.textFile(InputLocation, numPartitions).mapPartitions(new Point3DFormatMapper
                (offset, offset, splitter, true));
        JavaRDD<Point3D> zksRDD = rawRDD.filter(new QueryFilter(queryCube));
        List<Point3D> res = zksRDD.collect();
        long endTime=System.currentTimeMillis();
        System.out.println("raw query cost： " + ((double)(endTime-startTime))/1000 + "s");
        assert res != null;
    }

    /**
     * Test equal interval partitioning and R-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testEqualRtree()
            throws Exception
    {
        perform(SpatioTemporalGridType.EQUALTIMESLICE, IndexType.RTREE);
    }

    /**
     * Test equal interval partitioning and quad-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testEqualQuadTree()
            throws Exception
    {
        perform(SpatioTemporalGridType.EQUALTIMESLICE, IndexType.QUADTREE);
    }

    /**
     * Test equal data partitioning and R-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testEqualDataRtree()
            throws Exception
    {
        perform(SpatioTemporalGridType.EQUALDATASLICE, IndexType.RTREE);
    }


    /**
     * Test equal data partitioning and R-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testEqualDataQuadtree()
            throws Exception
    {
        perform(SpatioTemporalGridType.EQUALDATASLICE, IndexType.QUADTREE);
    }


    /**
     * Test 3DR-tree partitioning and R-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void test3DRtreeRtree()
            throws Exception
    {
        perform(SpatioTemporalGridType.RTREE3D, IndexType.RTREE);
    }


    /**
     * Test 3DR-tree partitioning and quad-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void test3DRtreeQuadtree()
            throws Exception
    {
        perform(SpatioTemporalGridType.RTREE3D, IndexType.QUADTREE);
    }


    /**
     * Test Octree partitioning and R-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadRtree()
            throws Exception
    {
        perform(SpatioTemporalGridType.OCTREE, IndexType.RTREE);
    }

    /**
     * Test Octree partitioning and quad-tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadQuad()
            throws Exception
    {
        perform(SpatioTemporalGridType.OCTREE, IndexType.QUADTREE);
    }

    @Test
    public void testArrayListSort() {
        ArrayList<Integer> lists = new ArrayList<Integer>();
        lists.add(2);
        lists.add(1);
        lists.add(2);

        Collections.sort(lists, new Comparator<Integer>() {
            @Override
            public int compare(Integer cube1, Integer cube2)
            {
                int res = 0;
                if (cube1 < cube2) {
                    res = -1;
                } else if (cube1 == cube2) {
                    res = 0;
                } else if (cube1 > cube2) {
                    res = 1;
                }
                return  res;
            }
        });

        assert lists.get(0) == 1;
    }


    private void perform(SpatioTemporalGridType spatioTemporalGridType, IndexType indexType)
            throws Exception
    {
        long startTime=System.currentTimeMillis();
        Point3DRDD spatialRDD = new Point3DRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatioTemporalPartitioning(spatioTemporalGridType);
        Point3D first = spatialRDD.spatioTemporalPartitionedRDD.first();
        long endTime=System.currentTimeMillis();
        System.out.println("build partition cost： " + ((double)(endTime-startTime))/1000 + "s");
        assert first != null;

        deleteDir(new File("C:\\spark\\try"));
        startTime=System.currentTimeMillis();
        List<Point3D> res = spatialRDD.spatioTemporalPartitionedRDD.filter(new QueryFilter(queryCube)).collect();
        endTime=System.currentTimeMillis();
        System.out.println("query on partition costs： " + ((double)(endTime-startTime))/1000 + "s  size:" + res.size());
        assert first != null;

        deleteDir(new File("C:\\spark\\try"));
        List<Point> result = new ArrayList<Point>();
        startTime=System.currentTimeMillis();
        spatialRDD.buildIndex(indexType, true);
        spatialRDD.indexedRDD.saveAsTextFile("C:\\spark\\try");
        endTime=System.currentTimeMillis();
        System.out.println("partition index costs： " + ((double)(endTime-startTime))/1000 + "s");

        startTime=System.currentTimeMillis();
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(searchEnvelop);
            System.out.println("the size of query is: " + result.size());
        }
        else {
            result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(searchEnvelop);
            System.out.println("the size of query is: " + result.size());
        }
        endTime=System.currentTimeMillis();
        System.out.println("partition index query costs： " + ((double)(endTime-startTime))/1000 + "s");
    }


    public boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }


}
