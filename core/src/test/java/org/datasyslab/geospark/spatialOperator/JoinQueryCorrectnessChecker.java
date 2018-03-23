/*
 * FILE: JoinQueryCorrectnessChecker
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class JoinQueryCorrectnessChecker
        extends GeoSparkTestBase
{

    /**
     * The test polygon window set.
     */
    public static List<Polygon> testPolygonWindowSet;

    /**
     * The test inside polygon set.
     */
    public static List<Polygon> testInsidePolygonSet;

    /**
     * The test overlapped polygon set.
     */
    public static List<Polygon> testOverlappedPolygonSet;

    /**
     * The test outside polygon set.
     */
    public static List<Polygon> testOutsidePolygonSet;

    /**
     * The test inside line string set.
     */
    public static List<LineString> testInsideLineStringSet;

    /**
     * The test overlapped line string set.
     */
    public static List<LineString> testOverlappedLineStringSet;

    /**
     * The test outside line string set.
     */
    public static List<LineString> testOutsideLineStringSet;

    /**
     * The test inside point set.
     */
    public static List<Point> testInsidePointSet;

    /**
     * The test on boundary point set.
     */
    public static List<Point> testOnBoundaryPointSet;

    /**
     * The test outside point set.
     */
    public static List<Point> testOutsidePointSet;

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    private final GridType gridType;

    public JoinQueryCorrectnessChecker(GridType gridType)
    {
        this.gridType = gridType;
    }

    @Parameterized.Parameters
    public static Collection testParams()
    {
        return Arrays.asList(new Object[][] {
                {GridType.RTREE},
                {GridType.QUADTREE},
                {GridType.KDBTREE},
        });
    }

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        GeoSparkTestBase.initialize(JoinQueryCorrectnessChecker.class.getSimpleName());

        // Define the user data saved in window objects and data objects
        testPolygonWindowSet = new ArrayList<>();
        testInsidePolygonSet = new ArrayList<>();
        testOverlappedPolygonSet = new ArrayList<>();
        testOutsidePolygonSet = new ArrayList<>();

        testInsideLineStringSet = new ArrayList<>();
        testOverlappedLineStringSet = new ArrayList<>();
        testOutsideLineStringSet = new ArrayList<>();

        testInsidePointSet = new ArrayList<>();
        testOnBoundaryPointSet = new ArrayList<>();
        testOutsidePointSet = new ArrayList<>();

        // Generate all test data

        // Generate test windows. Each window is a 5 by 5 rectangle-style polygon.
        // All data is uniformly distributed in a 10 by 10 window space.

        for (int baseX = 0; baseX < 100; baseX += 10) {
            for (int baseY = 0; baseY < 100; baseY += 10) {
                String id = baseX + ":" + baseY;
                String a = "a:" + id;
                String b = "b:" + id;

                testPolygonWindowSet.add(wrap(makeSquare(baseX, baseY, 5), a));
                testPolygonWindowSet.add(wrap(makeSquare(baseX, baseY, 5), b));

                // Polygons
                testInsidePolygonSet.add(wrap(makeSquare(baseX + 2, baseY + 2, 2), a));
                testInsidePolygonSet.add(wrap(makeSquare(baseX + 2, baseY + 2, 2), b));

                testOverlappedPolygonSet.add(wrap(makeSquare(baseX + 3, baseY + 3, 3), a));
                testOverlappedPolygonSet.add(wrap(makeSquare(baseX + 3, baseY + 3, 3), b));

                testOutsidePolygonSet.add(wrap(makeSquare(baseX + 6, baseY + 6, 3), a));
                testOutsidePolygonSet.add(wrap(makeSquare(baseX + 6, baseY + 6, 3), b));

                // LineStrings
                testInsideLineStringSet.add(wrap(makeSquareLine(baseX + 2, baseY + 2, 2), a));
                testInsideLineStringSet.add(wrap(makeSquareLine(baseX + 2, baseY + 2, 2), b));

                testOverlappedLineStringSet.add(wrap(makeSquareLine(baseX + 3, baseY + 3, 3), a));
                testOverlappedLineStringSet.add(wrap(makeSquareLine(baseX + 3, baseY + 3, 3), b));

                testOutsideLineStringSet.add(wrap(makeSquareLine(baseX + 6, baseY + 6, 3), a));
                testOutsideLineStringSet.add(wrap(makeSquareLine(baseX + 6, baseY + 6, 3), b));

                // Points
                testInsidePointSet.add(wrap(makePoint(baseX + 2.5, baseY + 2.5), a));
                testInsidePointSet.add(wrap(makePoint(baseX + 2.5, baseY + 2.5), b));

                testOnBoundaryPointSet.add(wrap(makePoint(baseX + 5, baseY + 5), a));
                testOnBoundaryPointSet.add(wrap(makePoint(baseX + 5, baseY + 5), b));

                testOutsidePointSet.add(wrap(makePoint(baseX + 6, baseY + 6), a));
                testOutsidePointSet.add(wrap(makePoint(baseX + 6, baseY + 6), b));
            }
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

    private static Polygon makeSquare(double minX, double minY, double side)
    {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(minX, minY);
        coordinates[1] = new Coordinate(minX + side, minY);
        coordinates[2] = new Coordinate(minX + side, minY + side);
        coordinates[3] = new Coordinate(minX, minY + side);
        coordinates[4] = coordinates[0];

        return geometryFactory.createPolygon(coordinates);
    }

    private static LineString makeSquareLine(double minX, double minY, double side)
    {
        Coordinate[] coordinates = new Coordinate[3];
        coordinates[0] = new Coordinate(minX, minY);
        coordinates[1] = new Coordinate(minX + side, minY);
        coordinates[2] = new Coordinate(minX + side, minY + side);

        return geometryFactory.createLineString(coordinates);
    }

    private static Point makePoint(double x, double y)
    {
        return geometryFactory.createPoint(new Coordinate(x, y));
    }

    private static <T extends Geometry> T wrap(T geometry, Object userData)
    {
        geometry.setUserData(userData);
        return geometry;
    }

    private <T extends Geometry, U extends Geometry> void prepareRDDs(SpatialRDD<T> objectRDD,
            SpatialRDD<U> windowRDD)
            throws Exception
    {
        objectRDD.rawSpatialRDD.repartition(4);
        objectRDD.spatialPartitioning(gridType);
        objectRDD.buildIndex(IndexType.RTREE, true);
        windowRDD.spatialPartitioning(objectRDD.getPartitioner());
    }

    /**
     * Test inside point join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsidePointJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PointRDD objectRDD = new PointRDD(sc.parallelize(this.testInsidePointSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<Polygon, HashSet<Point>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test on boundary point join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOnBoundaryPointJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PointRDD objectRDD = new PointRDD(sc.parallelize(this.testOnBoundaryPointSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<Polygon, HashSet<Point>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test outside point join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsidePointJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PointRDD objectRDD = new PointRDD(sc.parallelize(this.testOutsidePointSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        assertEquals(0, result.size());

        List<Tuple2<Polygon, HashSet<Point>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        assertEquals(0, resultNoIndex.size());
    }

    /**
     * Test inside line string join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsideLineStringJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testInsideLineStringSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<LineString>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<Polygon, HashSet<LineString>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test overlapped line string join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOverlappedLineStringJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testOverlappedLineStringSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<LineString>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, true).collect();
        verifyJoinResults(result);

        List<Tuple2<Polygon, HashSet<LineString>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, true).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test outside line string join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsideLineStringJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testOutsideLineStringSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<LineString>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        assertEquals(0, result.size());

        List<Tuple2<Polygon, HashSet<LineString>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        assertEquals(0, resultNoIndex.size());
    }

    /**
     * Test inside polygon join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsidePolygonJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testInsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<Polygon, HashSet<Polygon>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test overlapped polygon join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOverlappedPolygonJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOverlappedPolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, true).collect();
        verifyJoinResults(result);

        List<Tuple2<Polygon, HashSet<Polygon>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, true).collect();
        verifyJoinResults(resultNoIndex);
    }

    private <U extends Geometry, T extends Geometry> void verifyJoinResults(List<Tuple2<U, HashSet<T>>> result)
    {
        assertEquals(200, result.size());
        for (Tuple2<U, HashSet<T>> tuple : result) {
            U window = tuple._1;
            Set<T> objects = tuple._2;
            String windowUserData = (String) window.getUserData();

            String[] tokens = windowUserData.split(":", 2);
            String prefix = tokens[0];
            String id = tokens[1];

            assertTrue(prefix.equals("a") || prefix.equals("b"));
            assertEquals(2, objects.size());

            final Set<String> objectIds = new HashSet<>();
            for (T object : objects) {
                objectIds.add((String) object.getUserData());
            }

            assertEquals(new HashSet(Arrays.asList("a:" + id, "b:" + id)), objectIds);
        }
    }

    /**
     * Test outside polygon join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsidePolygonJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOutsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        assertEquals(0, result.size());

        List<Tuple2<Polygon, HashSet<Polygon>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        assertEquals(0, result.size());
    }

    /**
     * Test inside polygon distance join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsidePolygonDistanceJoinCorrectness()
            throws Exception
    {
        PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        CircleRDD windowRDD = new CircleRDD(centerGeometryRDD, 0.1);
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testInsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Geometry, HashSet<Polygon>>> result = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<Geometry, HashSet<Polygon>>> resultNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test overlapped polygon distance join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOverlappedPolygonDistanceJoinCorrectness()
            throws Exception
    {
        PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        CircleRDD windowRDD = new CircleRDD(centerGeometryRDD, 0.1);
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOverlappedPolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Geometry, HashSet<Polygon>>> result = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true, true).collect();
        verifyJoinResults(result);

        List<Tuple2<Geometry, HashSet<Polygon>>> resultNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false, true).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test outside polygon distance join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsidePolygonDistanceJoinCorrectness()
            throws Exception
    {
        PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        CircleRDD windowRDD = new CircleRDD(centerGeometryRDD, 0.1);
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOutsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Geometry, HashSet<Polygon>>> result = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true, true).collect();
        assertEquals(0, result.size());

        List<Tuple2<Geometry, HashSet<Polygon>>> resultNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false, true).collect();
        assertEquals(0, resultNoIndex.size());
    }
}
