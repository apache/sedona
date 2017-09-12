/**
 * FILE: RectangleJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.RectangleJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Arizona State University DataSystems Lab
 */

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleJoinTest.
 */
public class RectangleJoinTest extends JoinTestBase {

    private static long expectedMatchCount;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize("RectangleJoin", "rectangle.test.properties");
        expectedMatchCount = Long.parseLong(prop.getProperty("matchCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    /**
     * Test spatial join query with rectangle RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoop() throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        RectangleRDD spatialRDD = createRectangleRDD();
        
        spatialRDD.spatialPartitioning(gridType);
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();
        
        sanityCheckJoinResults(result);
        assertEquals(expectedMatchCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTree() throws Exception {
    	testIndexInt(IndexType.RTREE);
    }

    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTree() throws Exception {
        testIndexInt(IndexType.QUADTREE);
    }

    private void testIndexInt(IndexType indexType) throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        RectangleRDD spatialRDD = createRectangleRDD();

        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(indexType, true);

        queryRDD.spatialPartitioning(spatialRDD.grids);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedMatchCount, countJoinResults(result));
    }

    @Test
    public void testDynamicRTree() throws Exception {
        testDynamicIndexInt(IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTree() throws Exception {
        testDynamicIndexInt(IndexType.QUADTREE);
    }

    private void testDynamicIndexInt(IndexType indexType) throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        RectangleRDD spatialRDD = createRectangleRDD();

        spatialRDD.spatialPartitioning(gridType);
        queryRDD.spatialPartitioning(spatialRDD.grids);

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, indexType);
        List<Tuple2<Polygon, Polygon>> result = JoinQuery.spatialJoin(spatialRDD, queryRDD, joinParams).collect();

        sanityCheckFlatJoinResults(result);
        assertEquals(expectedMatchCount, result.size());
    }

    private RectangleRDD createRectangleRDD() {
        return new RectangleRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
    }
}