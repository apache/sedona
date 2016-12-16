/**
 * FILE: PointRDDTest.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDDTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;

// TODO: Auto-generated Javadoc
/**
 * The Class PointRDDTest.
 */
public class PointRDDTest implements Serializable{
    
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
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("PointRDDTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PointRDDTest.class.getClassLoader().getResourceAsStream("point.test.properties");

        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
        InputLocation = "file://"+PointRDDTest.class.getClassLoader().getResource("primaryroads.csv").getPath();

        offset = 0;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+PointRDDTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            
            

            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
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
     * Test constructor.
     *
     * @throws Exception the exception
     */
    /*
        This test case will load a sample data file and
     */
    @Test
    public void testConstructor() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        //todo: Set this to debug level
        for (EnvelopeWithGrid d : pointRDD.grids) {
            //System.out.println("PointRDD grids: "+d);
        }

        //todo: Move this into log4j.
        Map<Integer, Long> map = pointRDD.gridPointRDD.countByKey();
        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / pointRDD.totalNumberOfRecords;
            //System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }

    /*
     *  This test case test whether the X-Y grid can be build correctly.
     */
    /*
    @Test
    public void testXYGrid() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, "X-Y", 10);
        for (EnvelopeWithGrid d : pointRDD.grids) {
        	System.out.println("PointRDD grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Object> map = pointRDD.gridPointRDD.countByKey();

        System.out.println(map.size());

        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / pointRDD.totalNumberOfRecords;
            System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }*/
    /**
     * Test equal size grids spatial partitioing.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the equal size grids can be build correctly.
     */
    @Test
    public void testEqualSizeGridsSpatialPartitioing() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, GridType.EQUALGRID, 10);
        for (EnvelopeWithGrid d : pointRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = pointRDD.gridPointRDD.countByKey();

        System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / pointRDD.totalNumberOfRecords;
            //System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }
    
    /**
     * Test hilbert curve spatial partitioing.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the Hilbert Curve grid can be build correctly.
     */
    @Test
    public void testHilbertCurveSpatialPartitioing() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, GridType.HILBERT, 10);
        for (EnvelopeWithGrid d : pointRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = pointRDD.gridPointRDD.countByKey();

      //  System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / pointRDD.totalNumberOfRecords;
           // System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }
    
    /**
     * Test R tree spatial partitioing.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the STR-Tree grid can be build correctly.
     */
    @Test
    public void testRTreeSpatialPartitioing() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, GridType.RTREE, 10);
        for (EnvelopeWithGrid d : pointRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = pointRDD.gridPointRDD.countByKey();

        //System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / pointRDD.totalNumberOfRecords;
           // System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }
    
    /**
     * Test voronoi spatial partitioing.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the Voronoi grid can be build correctly.
     */
    @Test
    public void testVoronoiSpatialPartitioing() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, GridType.VORONOI, 10);
        for (EnvelopeWithGrid d : pointRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = pointRDD.gridPointRDD.countByKey();

        //System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / pointRDD.totalNumberOfRecords;
          //  System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }

    
    /**
     * Test build index without set grid.
     *
     * @throws Exception the exception
     */
    /*
     * If we try to build a index on a rawPointRDD which is not construct with grid. We shall see an error.
     */
    @Test//(expected=IllegalClassException.class)
    public void testBuildIndexWithoutSetGrid() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, numPartitions);
        pointRDD.buildIndex(null);
    }

    /**
     * Test build index.
     *
     * @throws Exception the exception
     */
    /*
        Test build Index.
     */
    @Test
    public void testBuildIndex() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        pointRDD.buildIndex(null);
        List<Point> result = pointRDD.indexedRDD.take(1).get(0)._2().query(pointRDD.boundaryEnvelope);
    }
    
    /**
     * Test build with no exists grid.
     *
     * @throws Exception the exception
     */
    /*
     *  If we want to use a grid type that is not supported yet, an exception will be throwed out.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testBuildWithNoExistsGrid() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, null, numPartitions);
    }

    /**
     * Test too large partition number.
     *
     * @throws Exception the exception
     */
    /*
     * If the partition number is set too large, we will
     */
    @Test(expected=IllegalArgumentException.class)
    public void testTooLargePartitionNumber() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, 1000000);
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}