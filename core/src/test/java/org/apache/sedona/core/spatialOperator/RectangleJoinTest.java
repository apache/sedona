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
import org.apache.sedona.core.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RectangleJoinTest
        extends JoinTestBase
{

    private static long expectedMatchCount;
    private static long expectedMatchWithOriginalDuplicatesCount;

    public RectangleJoinTest(GridType gridType, int numPartitions)
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
        initialize("RectangleJoin", "rectangle.test.properties");
        expectedMatchCount = Long.parseLong(prop.getProperty("matchCount"));
        expectedMatchWithOriginalDuplicatesCount =
                Long.parseLong(prop.getProperty("matchWithOriginalDuplicatesCount"));
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
     * Test spatial join query with rectangle RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoop()
            throws Exception
    {
        RectangleRDD queryRDD = createRectangleRDD();
        RectangleRDD spatialRDD = createRectangleRDD();

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, List<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        sanityCheckJoinResults(result);
        long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedMatchWithOriginalDuplicatesCount : expectedMatchCount;
        assertEquals(expectedCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTree()
            throws Exception
    {
        testIndexInt(IndexType.RTREE);
    }

    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTree()
            throws Exception
    {
        testIndexInt(IndexType.QUADTREE);
    }

    private void testIndexInt(IndexType indexType)
            throws Exception
    {
        RectangleRDD queryRDD = createRectangleRDD();
        RectangleRDD spatialRDD = createRectangleRDD();

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, List<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        sanityCheckJoinResults(result);
        long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedMatchWithOriginalDuplicatesCount : expectedMatchCount;
        assertEquals(expectedCount, countJoinResults(result));
    }

    @Test
    public void testDynamicRTree()
            throws Exception
    {
        testDynamicIndexInt(IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTree()
            throws Exception
    {
        testDynamicIndexInt(IndexType.QUADTREE);
    }

    private void testDynamicIndexInt(IndexType indexType)
            throws Exception
    {
        RectangleRDD queryRDD = createRectangleRDD();
        RectangleRDD spatialRDD = createRectangleRDD();

        partitionRdds(queryRDD, spatialRDD);

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, true, indexType, JoinBuildSide.LEFT);
        List<Tuple2<Polygon, Polygon>> result = JoinQuery.spatialJoin(queryRDD, spatialRDD, joinParams).collect();

        sanityCheckFlatJoinResults(result);

        final long expectedCount = expectToPreserveOriginalDuplicates()
                ? expectedMatchWithOriginalDuplicatesCount : expectedMatchCount;
        assertEquals(expectedCount, result.size());
    }

    private RectangleRDD createRectangleRDD()
    {
        return createRectangleRDD(InputLocation);
    }
}