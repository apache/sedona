/**
 * FILE: PolygonJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.PolygonJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Arizona State University DataSystems Lab
 */

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonJoinTest.
 */
public class PolygonJoinTest {
    
    /** The sc. */
    public static JavaSparkContext sc;
    
    /** The prop. */
    static Properties prop;
    
    /** The input. */
    static InputStream input;
    
    /** The Input location. */
    static String InputLocation;
    
    /** The Input location query window. */
    static String InputLocationQueryWindow;
    
    /** The Input location query polygon. */
    static String InputLocationQueryPolygon;
    
    /** The offset. */
    static Integer offset;
    
    /** The splitter. */
    static FileDataSplitter splitter;
    
    /** The grid type. */
    static GridType gridType;
    
    /** The index type. */
    static IndexType indexType;
    
    /** The num partitions. */
    static Integer numPartitions;

    /** The conf. */
    static SparkConf conf;

    private static long expectedContainsMatchCount;
    private static long expectedIntersectsMatchCount;
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
    	conf = new SparkConf().setAppName("PolygonJoin").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PolygonJoinTest.class.getClassLoader().getResourceAsStream("polygon.test.properties");
        offset = 0;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);

            //InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+PolygonJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryWindow="file://"+PolygonJoinTest.class.getClassLoader().getResource(prop.getProperty("queryWindowSet")).getPath();
            InputLocationQueryPolygon="file://"+PolygonJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            expectedContainsMatchCount = Long.parseLong(prop.getProperty("containsMatchCount"));
            expectedIntersectsMatchCount = Long.parseLong(prop.getProperty("intersectsMatchCount"));

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
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
        sanityCheckFlatJoinResults(results, intersects);
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
        sanityCheckJoinResults(result, intersects);
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
        sanityCheckJoinResults(result, intersects);
    }

    private PolygonRDD createPolygonRDD(String inputLocation) {
        return new PolygonRDD(sc, inputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
    }

    private void sanityCheckFlatJoinResults(List<Tuple2<Polygon, Polygon>> results, boolean intersects) {
        int count = results.size();
        if (intersects) {
            assertEquals(expectedIntersectsMatchCount, count);
        } else {
            assertEquals(expectedContainsMatchCount, count);
        }

        for (final Tuple2<Polygon, Polygon> tuple : results) {
            assertNotNull(tuple._1().getUserData());
            assertNotNull(tuple._2().getUserData());

            if (intersects) {
                assertTrue(tuple._1().intersects(tuple._2()));
            } else {
                assertTrue(tuple._1().covers(tuple._2()));
            }
        }
    }

    private long countJoinResults(List<Tuple2<Polygon, HashSet<Polygon>>> results) {
        int count = 0;
        for (final Tuple2<Polygon, HashSet<Polygon>> tuple : results) {
            count += tuple._2().size();
        }
        return count;
    }

    private void sanityCheckJoinResults(List<Tuple2<Polygon, HashSet<Polygon>>> results, boolean intersects) {
        final long count = countJoinResults(results);
        if (intersects) {
            assertEquals(expectedIntersectsMatchCount, count);
        } else {
            assertEquals(expectedContainsMatchCount, count);
        }

        for (final Tuple2<Polygon, HashSet<Polygon>> tuple : results) {
            assertNotNull(tuple._1().getUserData());
            assertFalse(tuple._2().isEmpty());
            for (final Polygon polygon : tuple._2()) {
                assertNotNull(polygon.getUserData());
                if (intersects) {
                    assertTrue(tuple._1().intersects(polygon));
                } else {
                    assertTrue(tuple._1().covers(polygon));
                }
            }
        }
    }
}