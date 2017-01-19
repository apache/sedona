/**
 * FILE: DistanceJoinQueryTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.DistanceJoinQueryTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class DistanceJoinQueryTest.
 */
public class DistanceJoinQueryTest {
    
    /** The sc. */
    public static JavaSparkContext sc;
    
    /** The prop. */
    static Properties prop;
    
    /** The input. */
    static InputStream input;
    
    /** The Input location. */
    static String InputLocation;
    
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
    
    /** The distance. */
    static double distance;
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("JoinTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        prop = new Properties();
        input = DistanceJoinQueryTest.class.getClassLoader().getResourceAsStream("point.test.properties");
        InputLocation = "file://"+DistanceJoinQueryTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        offset = 2;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);

            //InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+DistanceJoinQueryTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            distance=Double.parseDouble(prop.getProperty("distance"));
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
     * Test distancel join query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testDistancelJoinQuery() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        PointRDD pointRDD2 = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        pointRDD.spatialPartitioning(GridType.RTREE);
        pointRDD.buildIndex(IndexType.RTREE,true);
        pointRDD2.spatialPartitioning(pointRDD.grids);
        List<Tuple2<Point, HashSet<Point>>> result = DistanceJoin.SpatialJoinQueryWithoutIndex(sc, pointRDD, pointRDD2, distance).collect();
        assert result.size() >0;

    }

    /**
     * Test distancel join query with index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testDistancelJoinQueryWithIndex() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        PointRDD pointRDD2 = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        pointRDD.spatialPartitioning(GridType.RTREE);
        pointRDD.buildIndex(IndexType.RTREE,true);
        pointRDD2.spatialPartitioning(pointRDD.grids);
        List<Tuple2<Point, List<Point>>> result = DistanceJoin.SpatialJoinQueryUsingIndex(sc, pointRDD, pointRDD2, distance).collect();
        assert result.size() >0;
    }


    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}
