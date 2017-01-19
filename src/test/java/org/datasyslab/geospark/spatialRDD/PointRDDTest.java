/**
 * FILE: PointRDDTest.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDDTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

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
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter,true, numPartitions);
        //todo: Set this to debug level
        assert spatialRDD.totalNumberOfRecords>=1;
        assert spatialRDD.boundary!=null;
        assert spatialRDD.boundaryEnvelope!=null;
    }

    /**
     * Test equal partitioning.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the Hilbert Curve grid can be build correctly.
     */
    @Test
    public void testEqualPartitioning() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, 10);
        spatialRDD.spatialPartitioning(GridType.EQUALGRID);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d);
        }
        //System.out.println(spatialRDD.boundaryEnvelope);
        //todo: Move this into log4j.
        Map<Integer, Long> map = spatialRDD.spatialPartitionedRDD.countByKey();

      //  System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / spatialRDD.totalNumberOfRecords;
           // System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
        //System.out.println("Original number of records: "+spatialRDD.countWithoutDuplicates()+" Spatial partitioned records: "+spatialRDD.countWithoutDuplicatesSPRDD());
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
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
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,10);
        spatialRDD.spatialPartitioning(GridType.HILBERT);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = spatialRDD.spatialPartitionedRDD.countByKey();

      //  System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / spatialRDD.totalNumberOfRecords;
           // System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
        //System.out.println("Original number of records: "+spatialRDD.countWithoutDuplicates()+" Spatial partitioned records: "+spatialRDD.countWithoutDuplicatesSPRDD());
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
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
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,10);
        spatialRDD.spatialPartitioning(GridType.RTREE);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d);
        }
        //System.out.println(spatialRDD.boundaryEnvelope);
        //todo: Move this into log4j.
        Map<Integer, Long> map = spatialRDD.spatialPartitionedRDD.countByKey();

        //System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / spatialRDD.totalNumberOfRecords;
            //System.out.println("Rtree "+entry.getKey() + " : " + String.format("%.4f", percentage));
        }
        //System.out.println("Original number of records: "+spatialRDD.countWithoutDuplicates()+" Spatial partitioned records: "+spatialRDD.countWithoutDuplicatesSPRDD());
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
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
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,10);
        spatialRDD.spatialPartitioning(GridType.VORONOI);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = spatialRDD.spatialPartitionedRDD.countByKey();

        //System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / spatialRDD.totalNumberOfRecords;
            //System.out.println("Voronoi "+entry.getKey() + " : " + String.format("%.4f", percentage));
        }
        //System.out.println("Original number of records: "+spatialRDD.countWithoutDuplicates()+" Spatial partitioned records: "+spatialRDD.countWithoutDuplicatesSPRDD());
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
    }

    
    /**
     * Test build index without set grid.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildIndexWithoutSetGrid() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,numPartitions);
        spatialRDD.buildIndex(IndexType.RTREE,false);
    }


    /**
     * Test build rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildRtreeIndex() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,numPartitions);
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.RTREE,true);
        if(spatialRDD.indexedRDD.take(1).get(0)._2() instanceof STRtree)
        {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)._2()).query(spatialRDD.boundaryEnvelope);
        }
        else
        {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)._2()).query(spatialRDD.boundaryEnvelope);

        }
        }
    
    /**
     * Test build quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildQuadtreeIndex() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.QUADTREE,true);
        if(spatialRDD.indexedRDD.take(1).get(0)._2() instanceof STRtree)
        {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)._2()).query(spatialRDD.boundaryEnvelope);
        }
        else
        {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)._2()).query(spatialRDD.boundaryEnvelope);
        }
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}