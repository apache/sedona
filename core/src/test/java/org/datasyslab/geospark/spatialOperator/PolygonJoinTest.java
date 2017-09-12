/**
 * FILE: PolygonJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.PolygonJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
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
 * The Class PolygonJoinTest.
 */
public class PolygonJoinTest extends JoinTestBase {

    static long expectedContainsMatchCount;
    static long expectedIntersectsMatchCount;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize("PolygonJoin", "polygon.test.properties");

        expectedContainsMatchCount = Long.parseLong(prop.getProperty("containsMatchCount"));
        expectedIntersectsMatchCount = Long.parseLong(prop.getProperty("intersectsMatchCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    @Test
    public void testDynamicRTreeAndContains() throws Exception {
        testDynamicIndexInt(false, IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTreeAndContains() throws Exception {
        testDynamicIndexInt(false, IndexType.QUADTREE);
    }

    @Test
    public void testDynamicRTreeAndIntersects() throws Exception {
        testDynamicIndexInt(true, IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTreeAndIntersects() throws Exception {
        testDynamicIndexInt(true, IndexType.QUADTREE);
    }

    private void testDynamicIndexInt(boolean intersects, IndexType indexType) throws Exception {
        final PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        final PolygonRDD spatialRDD = createPolygonRDD(InputLocation);
        spatialRDD.spatialPartitioning(gridType);
        queryRDD.spatialPartitioning(spatialRDD.grids);

        final JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(intersects, indexType);
        final List<Tuple2<Polygon, Polygon>> results = JoinQuery.spatialJoin(spatialRDD, queryRDD, joinParams).collect();
        sanityCheckFlatJoinResults(results);
        assertEquals(getExpectedCount(intersects), results.size());
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopAndContains() throws Exception {
        testNestedLoopInt(false);
    }

    @Test
    public void testNestedLoopAndIntersects() throws Exception {
        testNestedLoopInt(true);
    }

    private void testNestedLoopInt(boolean intersects) throws Exception {
        PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        PolygonRDD spatialRDD = createPolygonRDD(InputLocation);

        spatialRDD.spatialPartitioning(gridType);
        queryRDD.spatialPartitioning(spatialRDD.grids);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,intersects).collect();
        sanityCheckJoinResults(result);
        assertEquals(getExpectedCount(intersects), countJoinResults(result));
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeAndContains() throws Exception {
        testIndexInt(false, IndexType.RTREE);
    }

    @Test
    public void testRTreeAndIntersects() throws Exception {
        testIndexInt(true, IndexType.RTREE);
    }

    @Test
    public void testQuadTreeAndContains() throws Exception {
        testIndexInt(false, IndexType.QUADTREE);
    }

    @Test
    public void testQuadTreeAndIntersects() throws Exception {
        testIndexInt(true, IndexType.QUADTREE);
    }

    private void testIndexInt(boolean intersects, IndexType indexType) throws Exception {
        PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        PolygonRDD spatialRDD = createPolygonRDD(InputLocation);

        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(indexType, true);

        queryRDD.spatialPartitioning(spatialRDD.grids);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,true,intersects).collect();
        sanityCheckJoinResults(result);
        assertEquals(getExpectedCount(intersects), countJoinResults(result));
    }

    private PolygonRDD createPolygonRDD(String inputLocation) {
        return new PolygonRDD(sc, inputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
    }

    private long getExpectedCount(boolean intersects) {
        return intersects ? expectedIntersectsMatchCount : expectedContainsMatchCount;
    }
}