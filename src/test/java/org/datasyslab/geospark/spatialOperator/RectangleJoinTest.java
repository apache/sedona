/**
 * FILE: RectangleJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.RectangleJoinTest.java
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
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Envelope;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleJoinTest.
 */
public class RectangleJoinTest {
    
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

    /** The conf. */
    static SparkConf conf;
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
    	conf = new SparkConf().setAppName("RectangleJoin").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = RectangleJoinTest.class.getClassLoader().getResourceAsStream("rectangle.test.properties");
        InputLocation = "file://"+RectangleJoinTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        offset = 0;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);

            //InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+RectangleJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
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
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    /**
     * Test spatial join query with rectangle RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithRectangleRDD() throws Exception {    	
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocation, offset, splitter, true, numPartitions);

        RectangleRDD spatialRDD = new RectangleRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }


    
    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithRectangleRDDUsingRtreeIndex() throws Exception {
    	
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocation, offset, splitter, true, numPartitions);

        RectangleRDD spatialRDD = new RectangleRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.RTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }


    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithRectangleRDDUsingQuadtreeIndex() throws Exception {

        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocation, offset, splitter, true, numPartitions);

        RectangleRDD spatialRDD = new RectangleRDD(sc, InputLocation, offset, splitter, true, numPartitions);
  
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }


    
    /**
     * Test join correctness with rectangle RDD.
     *
     * @throws Exception the exception
     */
    @Ignore
    public void testJoinCorrectnessWithRectangleRDD() throws Exception {
    	
        RectangleRDD queryRDD1 = new RectangleRDD(sc, InputLocation, offset, splitter, true);

        RectangleRDD spatialRDD1 = new RectangleRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD1.spatialPartitioning(GridType.RTREE);
        
        queryRDD1.spatialPartitioning(spatialRDD1.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result1 = JoinQuery.SpatialJoinQuery(spatialRDD1,queryRDD1,false).collect();
        
        RectangleRDD queryRDD2 = new RectangleRDD(sc, InputLocation, offset, splitter, true);
        
        RectangleRDD spatialRDD2 = new RectangleRDD(sc, InputLocation, offset, splitter, true, 40);
        
        spatialRDD2.spatialPartitioning(GridType.RTREE);
        
        queryRDD2.spatialPartitioning(spatialRDD2.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result2 = JoinQuery.SpatialJoinQuery(spatialRDD2,queryRDD2,false).collect();
        
        
        RectangleRDD queryRDD3 = new RectangleRDD(sc, InputLocation, offset, splitter, true);
        
        RectangleRDD spatialRDD3 = new RectangleRDD(sc, InputLocation, offset, splitter, true, 50);
        
        spatialRDD3.spatialPartitioning(GridType.RTREE);
        
        queryRDD3.spatialPartitioning(spatialRDD3.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result3 = JoinQuery.SpatialJoinQuery(spatialRDD3,queryRDD3,false).collect();
        
        
        RectangleRDD queryRDD4 = new RectangleRDD(sc, InputLocation, offset, splitter, true);

        RectangleRDD spatialRDD4 = new RectangleRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD4.spatialPartitioning(GridType.VORONOI);
        
        queryRDD4.spatialPartitioning(spatialRDD4.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result4 = JoinQuery.SpatialJoinQuery(spatialRDD4,queryRDD4,false).collect();
        
        
        RectangleRDD queryRDD5 = new RectangleRDD(sc, InputLocation, offset, splitter, true);
        
        RectangleRDD spatialRDD5 = new RectangleRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD5.spatialPartitioning(GridType.HILBERT);
        
        queryRDD5.spatialPartitioning(spatialRDD5.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result5 = JoinQuery.SpatialJoinQuery(spatialRDD5,queryRDD5,false).collect();
        
        
        RectangleRDD queryRDD6 = new RectangleRDD(sc, InputLocation, offset, splitter, true);
        
        RectangleRDD spatialRDD6 = new RectangleRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD6.spatialPartitioning(GridType.RTREE);
        
        spatialRDD6.buildIndex(IndexType.RTREE, true);
        
        queryRDD6.spatialPartitioning(spatialRDD6.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result6 = JoinQuery.SpatialJoinQuery(spatialRDD6,queryRDD6,true).collect();
        
        
        RectangleRDD queryRDD7 = new RectangleRDD(sc, InputLocation, offset, splitter, false);

        RectangleRDD spatialRDD7 = new RectangleRDD(sc, InputLocation, offset, splitter, false, 20);
        
        spatialRDD7.spatialPartitioning(GridType.RTREE);
        
        spatialRDD7.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD7.spatialPartitioning(spatialRDD7.grids);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result7 = JoinQuery.SpatialJoinQuery(spatialRDD7,queryRDD7,true).collect();
        
        
        RectangleRDD queryRDD8 = new RectangleRDD(sc, InputLocation, offset, splitter, false);
        
        RectangleRDD spatialRDD8 = new RectangleRDD(sc, InputLocation, offset, splitter, false, 50);

        spatialRDD8.spatialPartitioning(GridType.RTREE);
        
        spatialRDD8.buildIndex(IndexType.RTREE,true);
        
        queryRDD8.spatialPartitioning(spatialRDD8.grids);
                
        List<Tuple2<Envelope, HashSet<Envelope>>> result8 = JoinQuery.SpatialJoinQuery(spatialRDD8,queryRDD8,true).collect();
        
        
        RectangleRDD queryRDD9 = new RectangleRDD(sc, InputLocation, offset, splitter, false);
        
        RectangleRDD spatialRDD9 = new RectangleRDD(sc, InputLocation, offset, splitter, false, 50);

        spatialRDD9.spatialPartitioning(GridType.RTREE);
        
        spatialRDD9.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD9.spatialPartitioning(spatialRDD9.grids);  
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result9 = JoinQuery.SpatialJoinQuery(spatialRDD9,queryRDD9,true).collect();
        
        RectangleRDD queryRDD10 = new RectangleRDD(sc, InputLocation, offset, splitter, false);
        
        RectangleRDD spatialRDD10 = new RectangleRDD(sc, InputLocation, offset, splitter, false, 50);

        spatialRDD10.spatialPartitioning(GridType.VORONOI);
        
        spatialRDD10.buildIndex(IndexType.RTREE,true);
        
        queryRDD10.spatialPartitioning(spatialRDD10.grids);
                
        List<Tuple2<Envelope, HashSet<Envelope>>> result10 = JoinQuery.SpatialJoinQuery(spatialRDD10,queryRDD10,true).collect();
        
        
        RectangleRDD queryRDD11 = new RectangleRDD(sc, InputLocation, offset, splitter, false);
        
        RectangleRDD spatialRDD11 = new RectangleRDD(sc, InputLocation, offset, splitter, false, 50);

        spatialRDD11.spatialPartitioning(GridType.VORONOI);
        
        spatialRDD11.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD11.spatialPartitioning(spatialRDD11.grids);  
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result11 = JoinQuery.SpatialJoinQuery(spatialRDD11,queryRDD11,true).collect();
      
        if (result1.size()!=result2.size() || result1.size()!=result3.size()
        		|| result1.size()!=result4.size()|| result1.size()!=result5.size()
        		|| result1.size()!=result6.size()|| result1.size()!=result7.size()
        		|| result1.size()!=result8.size()|| result1.size()!=result9.size()
        		|| result1.size()!=result10.size()|| result1.size()!=result11.size()
        		)
        {
        	System.out.println("-----Rectangle join Recntangle results are not consistent-----");
        	System.out.println(result1.size());
        	System.out.println(result2.size());
        	System.out.println(result3.size());
        	System.out.println(result4.size());
        	System.out.println(result5.size());
        	System.out.println(result6.size());
        	System.out.println(result7.size());
        	System.out.println(result8.size());
        	System.out.println(result9.size());
        	System.out.println(result10.size());
        	System.out.println(result11.size());
        	System.out.println("-----Rectangle join Rectangle results are not consistent--Done---");
        	throw new Exception("Rectangle join Rectangle results are not consistent!");
        }
    }

 
}