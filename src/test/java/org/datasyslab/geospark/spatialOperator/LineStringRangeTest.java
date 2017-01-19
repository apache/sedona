/**
 * FILE: LineStringRangeTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.LineStringRangeTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.LineStringRDDTest;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDDTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;


// TODO: Auto-generated Javadoc
/**
 * The Class LineStringRangeTest.
 */
public class LineStringRangeTest {
    
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
    
    /** The index type. */
    static IndexType indexType;
    
    /** The num partitions. */
    static Integer numPartitions;
    
    /** The query envelope. */
    static Envelope queryEnvelope;
    
    /** The loop times. */
    static int loopTimes;
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("LineStringRange").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = LineStringRangeTest.class.getClassLoader().getResourceAsStream("linestring.test.properties");

        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
        InputLocation = "file://"+LineStringRDDTest.class.getClassLoader().getResource("primaryroads-linestring.csv").getPath();

        offset = 0;
        splitter = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+RectangleRDDTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            queryEnvelope=new Envelope (-85.01,-84.01,34.01,35.01);
            loopTimes=5;
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

    /**
     * Test spatial range query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialRangeQuery() throws Exception {
    	LineStringRDD spatialRDD = new LineStringRDD(sc, InputLocation, splitter, true);
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, 0,false).count();
    		assert resultSize>-1;
    	}
    	assert RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, 0,false).take(10).get(1).getUserData().toString()!=null;
        
    }
    
    /**
     * Test spatial range query using index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialRangeQueryUsingIndex() throws Exception {
    	LineStringRDD spatialRDD = new LineStringRDD(sc, InputLocation, splitter, true);
    	spatialRDD.buildIndex(IndexType.RTREE,false);
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, 0,true).count();
    		assert resultSize>-1;
    	}
    	assert RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, 0,true).take(10).get(1).getUserData().toString() !=null;
    }

}