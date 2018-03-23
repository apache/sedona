/*
 * FILE: PointJoinTest
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
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.JoinBuildSide;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
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
public class PointJoinTest
        extends JoinTestBase
{

    private static long expectedRectangleMatchCount;
    private static long expectedRectangleMatchWithOriginalDuplicatesCount;
    private static long expectedPolygonMatchCount;
    private static long expectedPolygonMatchWithOriginalDuplicatesCount;

    public PointJoinTest(GridType gridType, boolean useLegacyPartitionAPIs, int numPartitions)
    {
        super(gridType, useLegacyPartitionAPIs, numPartitions);
    }

    @Parameterized.Parameters
    public static Collection testParams()
    {
        return Arrays.asList(new Object[][] {
                {GridType.RTREE, true, 11},
                {GridType.RTREE, false, 11},
                {GridType.QUADTREE, true, 11},
                {GridType.QUADTREE, false, 11},
                {GridType.KDBTREE, false, 11},
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
    public void testNestedLoopWithRectanges()
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
        testNestedLoopInt(queryRDD, expectedPolygonMatchCount);
    }

    private void testNestedLoopInt(SpatialRDD<Polygon> queryRDD, long expectedCount)
            throws Exception
    {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithRectanges()
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
        testIndexInt(queryRDD, IndexType.RTREE, expectedPolygonMatchCount);
    }

    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTreeWithRectanges()
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
        testIndexInt(queryRDD, IndexType.QUADTREE, expectedPolygonMatchCount);
    }

    private void testIndexInt(SpatialRDD<Polygon> queryRDD, IndexType indexType, long expectedCount)
            throws Exception
    {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

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

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, indexType, JoinBuildSide.LEFT);
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