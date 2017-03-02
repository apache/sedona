/**
 * FILE: Example.java
 * PATH: org.datasyslab.geospark.showcase.Example.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.showcase;

import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

// TODO: Auto-generated Javadoc
/**
 * The Class Example.
 */
public class Example implements Serializable{
    
    /** The sc. */
    public static JavaSparkContext sc;
    
    /** The fact. */
    static GeometryFactory fact=new GeometryFactory();
    
    /** The cores. */
    static String cores;
    
    /** The prop. */
    static Properties prop;
    
    /** The query name. */
    static String queryName;
    
    /** The input. */
    static InputStream input;
    
    /** The input location. */
    static String inputLocation;
    
    /** The offset. */
    static Integer offset;
    
    /** The splitter. */
    static FileDataSplitter splitter;
    
    /** The num partitions. */
    static Integer numPartitions;
    
    /** The input location 2. */
    static String inputLocation2;
    
    /** The offset 2. */
    static Integer offset2;
    
    /** The splitter 2. */
    static FileDataSplitter splitter2;
    
    /** The grid type. */
    static GridType gridType;
    
    /** The num partitions 2. */
    static int numPartitions2;
    
    /** The loop times. */
    static int loopTimes;
    
    /** The query point. */
    static Point queryPoint;
    
    /** The query envelope. */
    static Envelope queryEnvelope;
    
    /** The conf. */
    static SparkConf conf;
    
    /** The master name. */
    static String masterName;
    
    /** The jar path. */
    static String jarPath;
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		GeometryFactory fact=new GeometryFactory();
		queryPoint=fact.createPoint(new Coordinate(-84.01, 34.01));
		queryEnvelope=new Envelope (-90.01,-80.01,30.01,40.01);
		cores=args[0];
		masterName="spark://"+args[1]+":7077";
		jarPath=args[2]+"geospark-0.3.jar";
		inputLocation=args[3];
		offset=Integer.parseInt(args[4]);
		splitter=FileDataSplitter.getFileDataSplitter(args[5]);
		numPartitions=Integer.parseInt(args[6]);
		loopTimes=Integer.parseInt(args[7]);
		queryName=args[8];
		if(args.length>9)
		{
			inputLocation2=args[9];
			offset2=Integer.parseInt(args[10]);			
			splitter2=FileDataSplitter.getFileDataSplitter(args[11]);
			numPartitions2=Integer.parseInt(args[12]);
			gridType=GridType.getGridType(args[13]);
		}
		conf=new SparkConf().setAppName(queryName+"+"+inputLocation+"+"+gridType+"+"+cores+"+"+numPartitions).setMaster(masterName);
				//.set("spark.history.fs.logDirectory", "/home/ubuntu/sparkeventlog")
				//.set("spark.history.retainedApplications", "10000")
				//.set("spark.eventLog.enabled", "true")
				//.set("spark.eventLog.dir", "/home/ubuntu/sparkeventlog")
				//.set("spark.executor.memory", "50g")
				//.set("spark.core.connection.ack.wait.timeout","900")
				//.set("spark.akka.timeout","900")
				//.set("spark.akka.frameSize","256")
				//.set("spark.memory.storageFraction", "0.8")
				//.set("spark.driver.memory", "50g")
				//.set("spark.cores.max",cores)
				//.set("spark.driver.cores","8")
				//.set("spark.driver.maxResultSize", "10g")
				//.set("spark.tachyonStore.url", "tachyon://"+args[0]+":19998")
				//.set("spark.externalBlockStore.url", "alluxio://"+args[0]+":19998")
				//.set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.TachyonBlockManager")
				//.set("log4j.rootCategory","ERROR, console");
		sc = new JavaSparkContext(conf);
		sc.addJar(jarPath);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
		
		try {
		switch(queryName)
		{
		case "pointrange":
			testSpatialRangeQuery();
			break;
		case "pointrangeindex":
			testSpatialRangeQueryUsingIndex();
			break;
		case "pointknn":
			testSpatialKnnQuery();
			break;
		case "pointknnindex":
			testSpatialKnnQueryUsingIndex();
			break;
		case "pointjoin":
			testSpatialJoinQuery();
			break;
		case "pointjoinindex":
			testSpatialJoinQueryUsingIndex();
			break;
		default:
            throw new Exception("Query type is not recognized, ");
		}			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		TearDown();
	}
    
    /**
     * Test spatial range query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialRangeQuery() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryEnvelope=new Envelope (-90.01+randomNumber,-80.01+randomNumber,30.01+randomNumber,40.01+randomNumber);
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY());
    	objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0,false).count();
    		assert resultSize>-1;
    	}
    }
    

    
    /**
     * Test spatial range query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialRangeQueryUsingIndex() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryEnvelope=new Envelope (-90.01+randomNumber,-80.01+randomNumber,30.01+randomNumber,40.01+randomNumber);
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY());
    	objectRDD.buildIndex(IndexType.RTREE,false);
    	objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0,true).count();
    		assert resultSize>-1;
    	}
        
    }

    /**
     * Test spatial knn query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialKnnQuery() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryPoint=fact.createPoint(new Coordinate(-84.01+randomNumber, 34.01+randomNumber));
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY());
    	objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		List<Envelope> result = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 1000,false);
    		assert result.size()>-1;
    	}
    }

    /**
     * Test spatial knn query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialKnnQueryUsingIndex() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryPoint=fact.createPoint(new Coordinate(-84.01+randomNumber, 34.01+randomNumber));
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY());
    	objectRDD.buildIndex(IndexType.RTREE,false);
    	objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		List<Envelope> result = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 1000, true);
    		assert result.size()>-1;
    	}
    }
 
    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialJoinQuery() throws Exception {
    	RectangleRDD rectangleRDD = new RectangleRDD(sc, inputLocation2, offset2, splitter2, true);
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset ,splitter,true, numPartitions, StorageLevel.MEMORY_ONLY());
    	objectRDD.spatialPartitioning(GridType.RTREE);
    	rectangleRDD.spatialPartitioning(objectRDD.grids);
    	objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		
    		long resultSize = JoinQuery.SpatialJoinQuery(objectRDD,rectangleRDD,false).count();
    		assert resultSize>0;
    	}
    }
    
    /**
     * Test spatial join query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialJoinQueryUsingIndex() throws Exception {
    	RectangleRDD rectangleRDD = new RectangleRDD(sc, inputLocation2, offset2, splitter2, true);
  		RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset ,splitter,true, numPartitions, StorageLevel.MEMORY_ONLY());
  		objectRDD.spatialPartitioning(GridType.RTREE);
  		rectangleRDD.spatialPartitioning(objectRDD.grids);
    	objectRDD.buildIndex(IndexType.RTREE,true);
    	objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = JoinQuery.SpatialJoinQuery(objectRDD,rectangleRDD,true).count();
    		assert resultSize>0;
    	}
    }
    

    /**
     * Tear down.
     */
    public static void TearDown() {
        sc.stop();
    }

}