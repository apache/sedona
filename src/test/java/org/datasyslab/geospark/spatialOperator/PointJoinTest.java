/**
 * FILE: PointJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.PointJoinTest.java
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
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
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
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class PointJoinTest.
 */
public class PointJoinTest {
    
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
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
    	conf = new SparkConf().setAppName("PointJoin").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PointJoinTest.class.getClassLoader().getResourceAsStream("point.test.properties");
        InputLocation = "file://"+PointJoinTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        offset = 0;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);          
            InputLocation = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryWindow = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("queryWindowSet")).getPath();
            InputLocationQueryPolygon = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("queryPolygonSet")).getPath();
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
    /*
    @Test(expected = NullPointerException.class)
    public void testSpatialJoinQueryUsingIndexException() throws Exception {
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }

    }
    */
    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithRectangleRDD() throws Exception {
    	
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
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
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDD() throws Exception {

        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
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
    	
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.RTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
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
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDDUsingRTreeIndex() throws Exception {
    	
        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.RTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
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

        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
  
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
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
     * Test spatial join query with polygon RDD using quad tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDDUsingQuadTreeIndex() throws Exception {
    	
        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false).collect();
        
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
    @Test
    public void testJoinCorrectnessWithRectangleRDD() throws Exception {
    	
        RectangleRDD queryRDD1 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);

        PointRDD spatialRDD1 = new PointRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD1.spatialPartitioning(GridType.RTREE);
        
        queryRDD1.spatialPartitioning(spatialRDD1.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result1 = JoinQuery.SpatialJoinQuery(spatialRDD1,queryRDD1,false).collect();
        
        RectangleRDD queryRDD2 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD2 = new PointRDD(sc, InputLocation, offset, splitter, true, 40);
        
        spatialRDD2.spatialPartitioning(GridType.RTREE);
        
        queryRDD2.spatialPartitioning(spatialRDD2.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result2 = JoinQuery.SpatialJoinQuery(spatialRDD2,queryRDD2,false).collect();
        
        
        RectangleRDD queryRDD3 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD3 = new PointRDD(sc, InputLocation, offset, splitter, true, 80);
        
        spatialRDD3.spatialPartitioning(GridType.RTREE);
        
        queryRDD3.spatialPartitioning(spatialRDD3.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result3 = JoinQuery.SpatialJoinQuery(spatialRDD3,queryRDD3,false).collect();
        
        
        RectangleRDD queryRDD4 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);

        PointRDD spatialRDD4 = new PointRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD4.spatialPartitioning(GridType.VORONOI);
        
        queryRDD4.spatialPartitioning(spatialRDD4.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result4 = JoinQuery.SpatialJoinQuery(spatialRDD4,queryRDD4,false).collect();
        
        
        RectangleRDD queryRDD5 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD5 = new PointRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD5.spatialPartitioning(GridType.HILBERT);
        
        queryRDD5.spatialPartitioning(spatialRDD5.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result5 = JoinQuery.SpatialJoinQuery(spatialRDD5,queryRDD5,false).collect();
        
        
        RectangleRDD queryRDD6 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD6 = new PointRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD6.spatialPartitioning(GridType.RTREE);
        
        spatialRDD6.buildIndex(IndexType.RTREE, true);
        
        queryRDD6.spatialPartitioning(spatialRDD6.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result6 = JoinQuery.SpatialJoinQuery(spatialRDD6,queryRDD6,true).collect();
        
        
        RectangleRDD queryRDD7 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);

        PointRDD spatialRDD7 = new PointRDD(sc, InputLocation, offset, splitter, true, 20);
        
        spatialRDD7.spatialPartitioning(GridType.RTREE);
        
        spatialRDD7.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD7.spatialPartitioning(spatialRDD7.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result7 = JoinQuery.SpatialJoinQuery(spatialRDD7,queryRDD7,true).collect();
        
        
        RectangleRDD queryRDD8 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD8 = new PointRDD(sc, InputLocation, offset, splitter, true, 80);

        spatialRDD8.spatialPartitioning(GridType.RTREE);
        
        spatialRDD8.buildIndex(IndexType.RTREE,true);
        
        queryRDD8.spatialPartitioning(spatialRDD8.grids);
                
        List<Tuple2<Envelope, HashSet<Point>>> result8 = JoinQuery.SpatialJoinQuery(spatialRDD8,queryRDD8,true).collect();
        
        
        RectangleRDD queryRDD9 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD9 = new PointRDD(sc, InputLocation, offset, splitter, true, 80);

        spatialRDD9.spatialPartitioning(GridType.RTREE);
        
        spatialRDD9.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD9.spatialPartitioning(spatialRDD9.grids);  
        
        List<Tuple2<Envelope, HashSet<Point>>> result9 = JoinQuery.SpatialJoinQuery(spatialRDD9,queryRDD9,true).collect();
        
        RectangleRDD queryRDD10 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD10 = new PointRDD(sc, InputLocation, offset, splitter, true, 80);

        spatialRDD10.spatialPartitioning(GridType.VORONOI);
        
        spatialRDD10.buildIndex(IndexType.RTREE,true);
        
        queryRDD10.spatialPartitioning(spatialRDD10.grids);
                
        List<Tuple2<Envelope, HashSet<Point>>> result10 = JoinQuery.SpatialJoinQuery(spatialRDD10,queryRDD10,true).collect();
        
        
        RectangleRDD queryRDD11 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD11 = new PointRDD(sc, InputLocation, offset, splitter, true, 80);

        spatialRDD11.spatialPartitioning(GridType.VORONOI);
        
        spatialRDD11.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD11.spatialPartitioning(spatialRDD11.grids);  
        
        List<Tuple2<Envelope, HashSet<Point>>> result11 = JoinQuery.SpatialJoinQuery(spatialRDD11,queryRDD11,true).collect();
        
        RectangleRDD queryRDD12 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD12 = new PointRDD(sc, InputLocation, offset, splitter, true, 80);

        spatialRDD12.spatialPartitioning(GridType.EQUALGRID);
        
        spatialRDD12.buildIndex(IndexType.RTREE,true);
        
        queryRDD12.spatialPartitioning(spatialRDD12.grids);
                
        List<Tuple2<Envelope, HashSet<Point>>> result12 = JoinQuery.SpatialJoinQuery(spatialRDD12,queryRDD12,true).collect();
        
        
        RectangleRDD queryRDD13 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true);
        
        PointRDD spatialRDD13 = new PointRDD(sc, InputLocation, offset, splitter, true, 40);

        spatialRDD13.spatialPartitioning(GridType.EQUALGRID);
        
        spatialRDD13.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD13.spatialPartitioning(spatialRDD13.grids);  
        
        List<Tuple2<Envelope, HashSet<Point>>> result13 = JoinQuery.SpatialJoinQuery(spatialRDD13,queryRDD13,true).collect();
        
        if (result1.size()!=result2.size() || result1.size()!=result3.size()
        		|| result1.size()!=result4.size()|| result1.size()!=result5.size()
        		|| result1.size()!=result6.size()|| result1.size()!=result7.size()
        		|| result1.size()!=result8.size()|| result1.size()!=result9.size()
        		|| result1.size()!=result10.size()|| result1.size()!=result11.size()
        		//|| result1.size()!=result12.size()|| result1.size()!=result13.size()
        		)
        {
        	System.out.println("-----Point join Recntangle results are not consistent-----");
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
        	System.out.println(result12.size());
        	System.out.println(result13.size());
        	System.out.println("-----Point join Rectangle results are not consistent--Done---");
        	throw new Exception("Point join rectangle results are not consistent!");
        }
    }

    /**
     * Test join correctness with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Ignore
    public void testJoinCorrectnessWithPolygonRDD() throws Exception {
    	
        PolygonRDD queryRDD1 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD1 = new PointRDD(sc, InputLocation, offset, splitter, false, 20);
        
        spatialRDD1.spatialPartitioning(GridType.RTREE);
        
        queryRDD1.spatialPartitioning(spatialRDD1.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result1 = JoinQuery.SpatialJoinQuery(spatialRDD1,queryRDD1,false).collect();
        
        PolygonRDD queryRDD2 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD2 = new PointRDD(sc, InputLocation, offset, splitter, false, 40);
        
        spatialRDD2.spatialPartitioning(GridType.RTREE);
        
        queryRDD2.spatialPartitioning(spatialRDD2.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result2 = JoinQuery.SpatialJoinQuery(spatialRDD2,queryRDD2,false).collect();
        
        
        PolygonRDD queryRDD3 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD3 = new PointRDD(sc, InputLocation, offset, splitter, false, 80);
        
        spatialRDD3.spatialPartitioning(GridType.RTREE);
        
        queryRDD3.spatialPartitioning(spatialRDD3.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result3 = JoinQuery.SpatialJoinQuery(spatialRDD3,queryRDD3,false).collect();
        
        
        PolygonRDD queryRDD4 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD4 = new PointRDD(sc, InputLocation, offset, splitter, false, 20);
        
        spatialRDD4.spatialPartitioning(GridType.VORONOI);
        
        queryRDD4.spatialPartitioning(spatialRDD4.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result4 = JoinQuery.SpatialJoinQuery(spatialRDD4,queryRDD4,false).collect();
        
        
        PolygonRDD queryRDD5 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD5 = new PointRDD(sc, InputLocation, offset, splitter, false, 20);
        
        spatialRDD5.spatialPartitioning(GridType.HILBERT);
        
        queryRDD5.spatialPartitioning(spatialRDD5.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result5 = JoinQuery.SpatialJoinQuery(spatialRDD5,queryRDD5,false).collect();
        
        
        PolygonRDD queryRDD6 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD6 = new PointRDD(sc, InputLocation, offset, splitter, false, 20);
        
        spatialRDD6.spatialPartitioning(GridType.RTREE);
        
        spatialRDD6.buildIndex(IndexType.RTREE, true);
        
        queryRDD6.spatialPartitioning(spatialRDD6.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result6 = JoinQuery.SpatialJoinQuery(spatialRDD6,queryRDD6,true).collect();
        
        
        PolygonRDD queryRDD7 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD7 = new PointRDD(sc, InputLocation, offset, splitter, false, 20);
        
        spatialRDD7.spatialPartitioning(GridType.RTREE);
        
        spatialRDD7.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD7.spatialPartitioning(spatialRDD7.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result7 = JoinQuery.SpatialJoinQuery(spatialRDD7,queryRDD7,true).collect();
        
        
        PolygonRDD queryRDD8 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD8 = new PointRDD(sc, InputLocation, offset, splitter, false, 80);

        spatialRDD8.spatialPartitioning(GridType.RTREE);
        
        spatialRDD8.buildIndex(IndexType.RTREE,true);
        
        queryRDD8.spatialPartitioning(spatialRDD8.grids);
                
        List<Tuple2<Polygon, HashSet<Point>>> result8 = JoinQuery.SpatialJoinQuery(spatialRDD8,queryRDD8,true).collect();
        
        
        PolygonRDD queryRDD9 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD9 = new PointRDD(sc, InputLocation, offset, splitter, false, 80);

        spatialRDD9.spatialPartitioning(GridType.RTREE);
        
        spatialRDD9.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD9.spatialPartitioning(spatialRDD9.grids);  
        
        List<Tuple2<Polygon, HashSet<Point>>> result9 = JoinQuery.SpatialJoinQuery(spatialRDD9,queryRDD9,true).collect();
        
        PolygonRDD queryRDD10 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD10 = new PointRDD(sc, InputLocation, offset, splitter, false, 80);

        spatialRDD10.spatialPartitioning(GridType.VORONOI);
        
        spatialRDD10.buildIndex(IndexType.RTREE,true);
        
        queryRDD10.spatialPartitioning(spatialRDD10.grids);
                
        List<Tuple2<Polygon, HashSet<Point>>> result10 = JoinQuery.SpatialJoinQuery(spatialRDD10,queryRDD10,true).collect();
        
        
        PolygonRDD queryRDD11 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD11 = new PointRDD(sc, InputLocation, offset, splitter, false, 80);

        spatialRDD11.spatialPartitioning(GridType.VORONOI);
        
        spatialRDD11.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD11.spatialPartitioning(spatialRDD11.grids);  
        
        List<Tuple2<Polygon, HashSet<Point>>> result11 = JoinQuery.SpatialJoinQuery(spatialRDD11,queryRDD11,true).collect();
        
        
        PolygonRDD queryRDD12 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD12 = new PointRDD(sc, InputLocation, offset, splitter, false, 80);

        spatialRDD12.spatialPartitioning(GridType.EQUALGRID);
        
        spatialRDD12.buildIndex(IndexType.RTREE,true);
        
        queryRDD12.spatialPartitioning(spatialRDD12.grids);
                
        List<Tuple2<Polygon, HashSet<Point>>> result12 = JoinQuery.SpatialJoinQuery(spatialRDD12,queryRDD12,true).collect();
        
        PolygonRDD queryRDD13 = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);
        
        PointRDD spatialRDD13 = new PointRDD(sc, InputLocation, offset, splitter, false, 80);

        spatialRDD13.spatialPartitioning(GridType.EQUALGRID);
        
        spatialRDD13.buildIndex(IndexType.QUADTREE,true);
        
        queryRDD13.spatialPartitioning(spatialRDD13.grids);  
        
        List<Tuple2<Polygon, HashSet<Point>>> result13 = JoinQuery.SpatialJoinQuery(spatialRDD13,queryRDD13,true).collect();
        
        if (result1.size()!=result2.size() || result1.size()!=result3.size()
        		|| result1.size()!=result4.size()|| result1.size()!=result5.size()
        		|| result1.size()!=result6.size()|| result1.size()!=result7.size()
        		|| result1.size()!=result8.size()|| result1.size()!=result9.size()
        		|| result1.size()!=result10.size()|| result1.size()!=result11.size()
        		//|| result1.size()!=result12.size()|| result1.size()!=result13.size()
        		)
        {
        	System.out.println("-----Point join Polygon results are not consistent-----");
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
        	System.out.println(result12.size());
        	System.out.println(result13.size());
        	System.out.println("-----Point join Polygon results are not consistent--Done---");
        	throw new Exception("Point join polygon results are not consistent!");
        }
    }

    
    
}