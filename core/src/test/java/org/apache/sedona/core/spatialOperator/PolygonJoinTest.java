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
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PolygonJoinTest
        extends JoinTestBase
{

    private static long expectedContainsMatchCount;
    private static long expectedIntersectsMatchCount;
    private static long expectedContainsWithOriginalDuplicatesCount;
    private static long expectedIntersectsWithOriginalDuplicatesCount;

    public PolygonJoinTest(GridType gridType, int numPartitions)
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
        initialize("PolygonJoin", "polygon.test.properties");

        expectedContainsMatchCount = Long.parseLong(prop.getProperty("containsMatchCount"));
        expectedContainsWithOriginalDuplicatesCount =
                Long.parseLong(prop.getProperty("containsMatchWithOriginalDuplicatesCount"));
        expectedIntersectsMatchCount = Long.parseLong(prop.getProperty("intersectsMatchCount"));
        expectedIntersectsWithOriginalDuplicatesCount =
                Long.parseLong(prop.getProperty("intersectsMatchWithOriginalDuplicatesCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }

    @Test
    public void testDynamicRTreeAndContains()
            throws Exception
    {
        testDynamicIndexInt(false, IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTreeAndContains()
            throws Exception
    {
        testDynamicIndexInt(false, IndexType.QUADTREE);
    }

    @Test
    public void testDynamicRTreeAndIntersects()
            throws Exception
    {
        testDynamicIndexInt(true, IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTreeAndIntersects()
            throws Exception
    {
        testDynamicIndexInt(true, IndexType.QUADTREE);
    }

    private void testDynamicIndexInt(boolean intersects, IndexType indexType)
            throws Exception
    {
        final PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        final PolygonRDD spatialRDD = createPolygonRDD(InputLocation);
        partitionRdds(queryRDD, spatialRDD);

        final JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, intersects, indexType, JoinBuildSide.LEFT);
        final List<Tuple2<Polygon, Polygon>> results = JoinQuery.spatialJoin(queryRDD, spatialRDD, joinParams).collect();
        sanityCheckFlatJoinResults(results);

        final long expectedCount = expectToPreserveOriginalDuplicates()
                ? getExpectedWithOriginalDuplicatesCount(intersects) : getExpectedCount(intersects);
        assertEquals(expectedCount, results.size());
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopAndContains()
            throws Exception
    {
        testNestedLoopInt(false);
    }

    @Test
    public void testNestedLoopAndIntersects()
            throws Exception
    {
        testNestedLoopInt(true);
    }

    private void testNestedLoopInt(boolean intersects)
            throws Exception
    {
        PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        PolygonRDD spatialRDD = createPolygonRDD(InputLocation);

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, List<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, intersects).collect();
        sanityCheckJoinResults(result);
        assertEquals(getExpectedWithOriginalDuplicatesCount(intersects), countJoinResults(result));
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeAndContains()
            throws Exception
    {
        testIndexInt(false, IndexType.RTREE);
    }

    @Test
    public void testRTreeAndIntersects()
            throws Exception
    {
        testIndexInt(true, IndexType.RTREE);
    }

    @Test
    public void testQuadTreeAndContains()
            throws Exception
    {
        testIndexInt(false, IndexType.QUADTREE);
    }

    @Test
    public void testQuadTreeAndIntersects()
            throws Exception
    {
        testIndexInt(true, IndexType.QUADTREE);
    }

    private void testIndexInt(boolean intersects, IndexType indexType)
            throws Exception
    {
        PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        PolygonRDD spatialRDD = createPolygonRDD(InputLocation);

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, List<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, true, intersects).collect();
        sanityCheckJoinResults(result);
        assertEquals(getExpectedWithOriginalDuplicatesCount(intersects), countJoinResults(result));
    }

    private long getExpectedCount(boolean intersects)
    {
        return intersects ? expectedIntersectsMatchCount : expectedContainsMatchCount;
    }

    private long getExpectedWithOriginalDuplicatesCount(boolean intersects)
    {
        return intersects ? expectedIntersectsWithOriginalDuplicatesCount : expectedContainsWithOriginalDuplicatesCount;
    }

    @Test
    public void testJoinWithSingletonRDD() throws Exception
    {
        PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        PolygonRDD spatialRDD = createPolygonRDD(InputLocation);
        PolygonRDD singletonRDD = new PolygonRDD();
        Polygon queryPolygon = queryRDD.rawSpatialRDD.first();
        singletonRDD.rawSpatialRDD = sc.parallelize(Collections.singletonList(queryPolygon), 1);
        singletonRDD.analyze(StorageLevel.MEMORY_ONLY());

        // Joining with a singleton RDD is essentially the same with a range query
        long expectedResultCount = RangeQuery.SpatialRangeQuery(spatialRDD, queryPolygon, true, false).count();

        partitionRdds(singletonRDD, spatialRDD);
        List<Tuple2<Polygon, Polygon>> result = JoinQuery.SpatialJoinQueryFlat(spatialRDD, singletonRDD, false, true).collect();
        sanityCheckFlatJoinResults(result);
        assertEquals(expectedResultCount, result.size());

        partitionRdds(spatialRDD, singletonRDD);
        result = JoinQuery.SpatialJoinQueryFlat(singletonRDD, spatialRDD, false, true).collect();
        sanityCheckFlatJoinResults(result);
        assertEquals(expectedResultCount, result.size());

        partitionRdds(singletonRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);
        result = JoinQuery.SpatialJoinQueryFlat(spatialRDD, singletonRDD, true, true).collect();
        sanityCheckFlatJoinResults(result);
        assertEquals(expectedResultCount, result.size());

        partitionRdds(spatialRDD, singletonRDD);
        singletonRDD.buildIndex(indexType, true);
        result = JoinQuery.SpatialJoinQueryFlat(singletonRDD, spatialRDD, true, true).collect();
        sanityCheckFlatJoinResults(result);
        assertEquals(expectedResultCount, result.size());
    }
}
