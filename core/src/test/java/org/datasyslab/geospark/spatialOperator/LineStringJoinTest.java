/**
 * FILE: LineStringJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.LineStringJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class LineStringJoinTest extends JoinTestBase {

    private static long expectedMatchCount;
    private static long expectedMatchWithOriginalDuplicatesCount;

    public LineStringJoinTest(GridType gridType, boolean useLegacyPartitionAPIs, int numPartitions) {
        super(gridType, useLegacyPartitionAPIs, numPartitions);
    }

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][] {
            { GridType.RTREE, true, 11 },
            { GridType.RTREE, false, 11 },
            { GridType.QUADTREE, true, 11 },
            { GridType.QUADTREE, false, 11},
            { GridType.KDBTREE, false, 11},
        });
    }

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize("LineStringJoin", "linestring.test.properties");
        expectedMatchCount = Long.parseLong(prop.getProperty("matchCount"));
        expectedMatchWithOriginalDuplicatesCount =
            Long.parseLong(prop.getProperty("matchWithOriginalDuplicatesCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    /**
     * Test spatial join query with line string RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoop() throws Exception {

        PolygonRDD queryRDD = createPolygonRDD();
        LineStringRDD spatialRDD = createLineStringRDD();

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, HashSet<LineString>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedMatchCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTree() throws Exception {
    	testIndexInt(IndexType.RTREE);
    }

    /**
     * Test spatial join query with polygon RDD using quad tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTree() throws Exception {
        testIndexInt(IndexType.QUADTREE);
    }

    private void testIndexInt(IndexType indexType) throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();

        LineStringRDD spatialRDD = createLineStringRDD();

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, HashSet<LineString>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();

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
        PolygonRDD queryRDD = createPolygonRDD();
        LineStringRDD spatialRDD = createLineStringRDD();

        partitionRdds(queryRDD, spatialRDD);

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, indexType);
        List<Tuple2<Polygon, LineString>> results = JoinQuery.spatialJoin(spatialRDD, queryRDD, joinParams).collect();

        sanityCheckFlatJoinResults(results);

        long expectedCount = expectToPreserveOriginalDuplicates()
            ? expectedMatchWithOriginalDuplicatesCount : expectedMatchCount;
        assertEquals(expectedCount, results.size());
    }

    private LineStringRDD createLineStringRDD() {
        return createLineStringRDD(InputLocation);
    }

    private PolygonRDD createPolygonRDD() {
        return createPolygonRDD(InputLocationQueryPolygon);
    }
}