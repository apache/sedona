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
package org.apache.sedona.core.spatialOperator;

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.RectangleRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PointJoinTest
        extends JoinTestBase
{

    private static long expectedRectangleMatchCount;
    private static long expectedRectangleMatchWithOriginalDuplicatesCount;
    private static long expectedPolygonMatchCount;
    private static long expectedPolygonMatchWithOriginalDuplicatesCount;

    public PointJoinTest(GridType gridType, int numPartitions)
    {
        super(gridType, numPartitions);
    }

    @Parameterized.Parameters
    public static Collection testParams()
    {
        return Arrays.asList(new Object[][] {
                {GridType.QUADTREE, 11},
                {GridType.KDBTREE, 11},
        });
    }

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize("PointJoin", "point.test.properties");
        expectedRectangleMatchCount = Long.parseLong(prop.getProperty("rectangleMatchCount"));
        expectedRectangleMatchWithOriginalDuplicatesCount =
                Long.parseLong(prop.getProperty("rectangleMatchWithOriginalDuplicatesCount"));
        expectedPolygonMatchCount = Long.parseLong(prop.getProperty("polygonMatchCount"));
        expectedPolygonMatchWithOriginalDuplicatesCount =
                Long.parseLong(prop.getProperty("polygonMatchWithOriginalDuplicatesCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }

    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopWithRectangles()
            throws Exception
    {
        RectangleRDD queryRDD = createRectangleRDD();
        testNestedLoopInt(queryRDD, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopWithPolygons()
            throws Exception
    {
        PolygonRDD queryRDD = createPolygonRDD();
        final long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedPolygonMatchWithOriginalDuplicatesCount : expectedPolygonMatchCount;
        testNestedLoopInt(queryRDD, expectedCount);
    }

    private void testNestedLoopInt(SpatialRDD<Polygon> queryRDD, long expectedCount)
            throws Exception
    {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, List<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithRectangles()
            throws Exception
    {
        RectangleRDD queryRDD = createRectangleRDD();
        testIndexInt(queryRDD, IndexType.RTREE, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithPolygons()
            throws Exception
    {
        PolygonRDD queryRDD = createPolygonRDD();
        final long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedPolygonMatchWithOriginalDuplicatesCount : expectedPolygonMatchCount;
        testIndexInt(queryRDD, IndexType.RTREE, expectedCount);
    }

    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTreeWithRectangles()
            throws Exception
    {
        RectangleRDD queryRDD = createRectangleRDD();
        testIndexInt(queryRDD, IndexType.QUADTREE, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD using quad tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTreeWithPolygons()
            throws Exception
    {
        PolygonRDD queryRDD = createPolygonRDD();
        final long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedPolygonMatchWithOriginalDuplicatesCount : expectedPolygonMatchCount;
        testIndexInt(queryRDD, IndexType.QUADTREE, expectedCount);
    }

    private void testIndexInt(SpatialRDD<Polygon> queryRDD, IndexType indexType, long expectedCount)
            throws Exception
    {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, List<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedCount, countJoinResults(result));
    }

    @Test
    public void testDynamicRTreeWithRectangles()
            throws Exception
    {
        final RectangleRDD rectangleRDD = createRectangleRDD();
        final long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedRectangleMatchWithOriginalDuplicatesCount : expectedRectangleMatchCount;
        testDynamicRTreeInt(rectangleRDD, IndexType.RTREE, expectedCount);
    }

    @Test
    public void testDynamicRTreeWithPolygons()
            throws Exception
    {
        PolygonRDD polygonRDD = createPolygonRDD();
        final long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedPolygonMatchWithOriginalDuplicatesCount : expectedPolygonMatchCount;
        testDynamicRTreeInt(polygonRDD, IndexType.RTREE, expectedCount);
    }

    private void testDynamicRTreeInt(SpatialRDD<Polygon> queryRDD, IndexType indexType, long expectedCount)
            throws Exception
    {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, true, indexType, JoinBuildSide.LEFT);
        List<Tuple2<Polygon, Point>> results = JoinQuery.spatialJoin(queryRDD, spatialRDD, joinParams).collect();

        sanityCheckFlatJoinResults(results);
        assertEquals(expectedCount, results.size());
    }

    private RectangleRDD createRectangleRDD()
    {
        return createRectangleRDD(InputLocationQueryWindow);
    }

    private PolygonRDD createPolygonRDD()
    {
        return createPolygonRDD(InputLocationQueryPolygon);
    }

    private PointRDD createPointRDD()
    {
        return createPointRDD(InputLocation);
    }
}