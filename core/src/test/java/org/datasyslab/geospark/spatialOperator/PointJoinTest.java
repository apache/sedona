/**
 * FILE: PointJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.PointJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
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
 * The Class PointJoinTest.
 */
public class PointJoinTest extends JoinTestBase {

    private static long expectedRectangleMatchCount;
    private static long expectedPolygonMatchCount;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
    	initialize("PointJoin", "point.test.properties");
        expectedRectangleMatchCount = Long.parseLong(prop.getProperty("rectangleMatchCount"));
        expectedPolygonMatchCount = Long.parseLong(prop.getProperty("polygonMatchCount"));
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
    /*
    @Test(expected = NullPointerException.class)
    public void testSpatialJoinQueryUsingIndexException() throws Exception {
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }

    }
    */
    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopWithRectanges() throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        testNestedLoopInt(queryRDD, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopWithPolygons() throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();
        testNestedLoopInt(queryRDD, expectedPolygonMatchCount);
    }

    private void testNestedLoopInt(SpatialRDD<Polygon> queryRDD, long expectedCount) throws Exception {
        PointRDD spatialRDD = createPointRDD();

        spatialRDD.spatialPartitioning(gridType);
        queryRDD.spatialPartitioning(spatialRDD.grids);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithRectanges() throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        testIndexInt(queryRDD, IndexType.RTREE, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithPolygons() throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();
        testIndexInt(queryRDD, IndexType.RTREE, expectedPolygonMatchCount);
    }

    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTreeWithRectanges() throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        testIndexInt(queryRDD, IndexType.QUADTREE, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD using quad tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTreeWithPolygons() throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();
        testIndexInt(queryRDD, IndexType.QUADTREE, expectedPolygonMatchCount);
    }

    private void testIndexInt(SpatialRDD<Polygon> queryRDD, IndexType indexType, long expectedCount) throws Exception {
        PointRDD spatialRDD = createPointRDD();

        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(indexType, true);

        queryRDD.spatialPartitioning(spatialRDD.grids);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedCount, countJoinResults(result));
    }

    @Test
    public void testDynamicRTreeWithRectanges() throws Exception {
        final RectangleRDD rectangleRDD = createRectangleRDD();
        testDynamicRTreeInt(rectangleRDD, IndexType.RTREE, expectedRectangleMatchCount);
    }

    private void testDynamicRTreeInt(RectangleRDD queryRDD, IndexType indexType, long expectedCount) throws Exception {
        PointRDD spatialRDD = createPointRDD();

        spatialRDD.spatialPartitioning(gridType);
        queryRDD.spatialPartitioning(spatialRDD.grids);

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, indexType);
        List<Tuple2<Polygon, Point>> results = JoinQuery.spatialJoin(spatialRDD, queryRDD, joinParams).collect();

        sanityCheckFlatJoinResults(results);
        assertEquals(expectedCount, results.size());
    }

    private RectangleRDD createRectangleRDD() {
        return new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
    }

    private PolygonRDD createPolygonRDD() {
        return new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
    }

    private PointRDD createPointRDD() {
        return new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
    }
}