package org.datasyslab.geospark.showcase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class Example implements Serializable{
    public static JavaSparkContext sc;
    static GeometryFactory fact=new GeometryFactory();
    static String cores;
    
    static Properties prop;
    static String queryName;
    static InputStream input;
    
    static String inputLocation;
    static Integer offset;
    static String splitter;
    static Integer numPartitions;
    
    static String inputLocation2;
    static Integer offset2;
    static String splitter2;
    static String gridType="";
    static int numPartitions2;
    
    static int loopTimes;
    static Point queryPoint;
    static Envelope queryEnvelope;
    static SparkConf conf;
    static String masterName;
    static String jarPath;
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
		splitter=args[5];
		numPartitions=Integer.parseInt(args[6]);
		loopTimes=Integer.parseInt(args[7]);
		queryName=args[8];
		if(args.length>9)
		{
			inputLocation2=args[9];
			offset2=Integer.parseInt(args[10]);			
			splitter2=args[11];
			numPartitions2=Integer.parseInt(args[12]);
			gridType=args[13];
		}
		conf=new SparkConf().setAppName(queryName+"+"+inputLocation+"+"+gridType+"+"+cores+"+"+numPartitions).setMaster(masterName)
				.set("spark.history.fs.logDirectory", "/home/ubuntu/sparkeventlog")
				//.set("spark.history.fs.logDirectory", "/data1/jia/sparkeventlog")
				.set("spark.history.retainedApplications", "10000")
				.set("spark.eventLog.enabled", "true")
				.set("spark.eventLog.dir", "/home/ubuntu/sparkeventlog")
				//.set("spark.eventLog.dir", "/data1/jia/sparkeventlog")
				.set("spark.executor.memory", "50g")
				.set("spark.core.connection.ack.wait.timeout","900")
				.set("spark.akka.timeout","900")
				.set("spark.akka.frameSize","256")
				.set("spark.memory.storageFraction", "0.8")
				.set("spark.driver.memory", "50g")
				.set("spark.cores.max",cores)
				.set("spark.driver.cores","8")
				.set("spark.driver.maxResultSize", "10g")
				//.set("spark.tachyonStore.url", "tachyon://"+args[0]+":19998")
				//.set("spark.externalBlockStore.url", "alluxio://"+args[0]+":19998")
				//.set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.TachyonBlockManager")
				.set("log4j.rootCategory","ERROR, console");
		sc = new JavaSparkContext(conf);
		sc.addJar(jarPath);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
		
		try {
		switch(queryName)
		{
		case "formattransform":
			formatTransform(args[0]);
			break;
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
		case "pointjoinspark":
			testSpatialJoinQueryUsingSpark();
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
	public static void formatTransform(String outputname) throws Exception
	{
		PolygonRDD polygonRDD = new PolygonRDD(sc, inputLocation, offset, splitter);
		JavaRDD<String> result=polygonRDD.rawPolygonRDD.map(new Function<Polygon,String>()
				{
					@Override
					public String call(Polygon v1) throws Exception {
						// TODO Auto-generated method stub
						Envelope envelope=v1.getEnvelopeInternal();
						String result="";
						result=envelope.getMinX()+"\t"+envelope.getMinY()+"\t"+envelope.getMaxX()+"\t"+envelope.getMaxY();
						return result;
					}
			
				});
		FileWriter fw = new FileWriter(outputname+".new");
	    BufferedWriter bw = new BufferedWriter(fw);
	    PrintWriter out = new PrintWriter(bw);
		List<String> output=result.collect();
		for(String str:output)
		{
			out.println(str);
		}
		out.close();
		bw.close();
		fw.close();
	}
    public static void testSpatialRangeQuery() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryEnvelope=new Envelope (-90.01+randomNumber,-80.01+randomNumber,30.01+randomNumber,40.01+randomNumber);
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter);
    	objectRDD.rawRectangleRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0).getRawRectangleRDD().count();
    		assert resultSize>-1;
    	}
    }
    
    public static void testSpatialRangeQuery1() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryEnvelope=new Envelope (-90.01+randomNumber,-80.01+randomNumber,30.01+randomNumber,40.01+randomNumber);
    	PointRDD pointRDD = new PointRDD(sc, inputLocation, offset, splitter);
    	pointRDD.rawPointRDD.persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = RangeQuery.SpatialRangeQuery(pointRDD, queryEnvelope, 0).getRawPointRDD().count();
    		assert resultSize>-1;
    	}
    }
    
    public static void testSpatialRangeQueryUsingIndex() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryEnvelope=new Envelope (-90.01+randomNumber,-80.01+randomNumber,30.01+randomNumber,40.01+randomNumber);
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter);
    	objectRDD.buildIndex("rtree");
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = RangeQuery.SpatialRangeQueryUsingIndex(objectRDD, queryEnvelope, 0).getRawRectangleRDD().count();
    		assert resultSize>-1;
    	}
        
    }

    public static void testSpatialKnnQuery() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryPoint=fact.createPoint(new Coordinate(-84.01+randomNumber, 34.01+randomNumber));
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter);
    	objectRDD.getRawRectangleRDD().persist(StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		List<Envelope> result = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 1000);
    		assert result.size()>-1;
    	}
    }

    public static void testSpatialKnnQueryUsingIndex() throws Exception {
    	Random random=new Random();
    	double randomNumber=random.nextInt(10)+random.nextDouble();
    	queryPoint=fact.createPoint(new Coordinate(-84.01+randomNumber, 34.01+randomNumber));
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter);
    	objectRDD.buildIndex("rtree");
    	for(int i=0;i<loopTimes;i++)
    	{
    		List<Envelope> result = KNNQuery.SpatialKnnQueryUsingIndex(objectRDD, queryPoint, 1000);
    		assert result.size()>-1;
    	}
    }
 
    public static void testSpatialJoinQuery() throws Exception {
    	RectangleRDD rectangleRDD = new RectangleRDD(sc, inputLocation2, offset2, splitter2);
    	//polygonRDD.rawPolygonRDD.unpersist();
    	RectangleRDD objectRDD;
    	if(gridType=="voronoi"){
    		objectRDD = new RectangleRDD(sc, inputLocation, offset ,splitter,gridType,numPartitions2);
    	}
    	else
    	{
    		objectRDD = new RectangleRDD(sc, inputLocation, offset ,splitter,gridType,numPartitions);
    	}
    	objectRDD.gridRectangleRDD.persist(StorageLevel.MEMORY_ONLY());
    	JoinQuery joinQuery = new JoinQuery(sc,objectRDD,rectangleRDD); 
    	for(int i=0;i<loopTimes;i++)
    	{
    		
    		long resultSize = joinQuery.SpatialJoinQuery(objectRDD,rectangleRDD).count();
    		assert resultSize>0;
    	}
    }
    
    public static void testSpatialJoinQueryUsingIndex() throws Exception {
    	RectangleRDD rectangleRDD = new RectangleRDD(sc, inputLocation2, offset2, splitter2);
    	//polygonRDD.rawPolygonRDD.unpersist();
    	RectangleRDD objectRDD;
    	if(gridType=="voronoi"){
    		objectRDD = new RectangleRDD(sc, inputLocation, offset ,splitter,gridType,numPartitions2);
    	}
    	else
    	{
    		objectRDD = new RectangleRDD(sc, inputLocation, offset ,splitter,gridType,numPartitions);
    	}
    	objectRDD.buildIndex("rtree");
    	JoinQuery joinQuery = new JoinQuery(sc,objectRDD,rectangleRDD);
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize = joinQuery.SpatialJoinQueryUsingIndex(objectRDD,rectangleRDD).count();
    		assert resultSize>0;
    	}
    }
    

    public static void testSpatialJoinQueryUsingSpark() throws Exception {
    	RectangleRDD rectangleRDD = new RectangleRDD(sc, inputLocation2, offset2, splitter2);
    	//polygonRDD.rawPolygonRDD.unpersist();
    	RectangleRDD objectRDD = new RectangleRDD(sc, inputLocation, offset, splitter);
    	List<Envelope> queryWindows=objectRDD.rawRectangleRDD.collect();
    	final Broadcast<List<Envelope>> queryWindowsBroadcast=sc.broadcast(queryWindows);
    	for(int i=0;i<loopTimes;i++)
    	{

    		//JavaPairRDD<Envelope,Point> joinRDD=rectangleRDD.rawRectangleRDD.cartesian(pointRDD.rawPointRDD);
    		JavaPairRDD<Envelope,Envelope> filteredJoinRDD=objectRDD.rawRectangleRDD.flatMapToPair(new PairFlatMapFunction<Envelope,Envelope,Envelope>()
    				{

						@Override
						public Iterable<Tuple2<Envelope, Envelope>> call(Envelope t)
								throws Exception {
							// TODO Auto-generated method stub
							List<Envelope> queryWindowSet=queryWindowsBroadcast.getValue();
							Iterator<Envelope> queryWindowIterator=queryWindowSet.iterator();
							final HashSet<Tuple2<Envelope, Envelope>> result=new HashSet<Tuple2<Envelope, Envelope>>(); 
							while(queryWindowIterator.hasNext())
							{
								Envelope window=queryWindowIterator.next();
								if(window.contains(t))
								{
									result.add(new Tuple2<Envelope, Envelope>(window,t));
								}
								
							}
							return result;
						}
    			
    				});
    		queryWindowsBroadcast.destroy();
    		JavaPairRDD<Envelope,Iterable<Envelope>> resultRDD=filteredJoinRDD.groupByKey();
    		long resultSize=resultRDD.count();
    		assert resultSize>-1;
    	}
    }
   
    
    
    public static void TearDown() {
        sc.stop();
    }

}
