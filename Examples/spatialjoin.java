/* This is an example of GeoSpark spatial join query
 * Introduction: In spatial join query, one query area set is joined with one Spatial RDD. The query area set which is composed of rectangles or polygons can be also stored in Spatial RDD. GeoSpark then joins the two Spatial RDDs
and returns a new Spatial RDD instance which is extended from the original SRDDs. For one query area, the object
contained by it will be attached behind it in this instance.
 * Arguments: 
 * 1. Spark master IP;
 * 2. Input 1 location;
 * 3. Input 1 partitions number;
 * 4. Input 1 spatial info starting column;
 * 5. Format name "tsv || csv || wkt";
 * 6. Input 2 location;
 * 7. Input 2 partitions number;
 * 8. Input 2 spatial info starting column;
 * 9. Format name "tsv || csv || wkt";
 * 10. Condition: Overlap or inside
 * 11. Grid number on X-axis (Default: 100);
 * 12. Grid number on Y-axis (Default: 100)
 * Result is saved at "hdfs://"+IP+":54310/test/tempResult.txt";
 * */



import java.awt.Rectangle;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import Functions.PartitionAssignGridPoint;
import Functions.PartitionAssignGridRectangle;
import Functions.PointRangeFilter;
import Functions.RectangleRangeFilter;
import GeoSpark.Circle;
import GeoSpark.FilterCollection;
import GeoSpark.PointRDD;
import GeoSpark.RectangleRDD;
import GeoSpark.SpatialPairRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;


public class spatialjoin {

	public static void main(String[] args) throws IOException {
		String IP=args[0];
		String set1="hdfs://"+IP+":54310/"+args[1];
		Integer partitions1=Integer.parseInt(args[2]);
		Integer offset1=Integer.parseInt(args[3]);
		String Splitter1=args[4];
		String querywindowset="hdfs://"+IP+":54310/"+args[5];
		Integer partitions2=Integer.parseInt(args[6]);
		Integer offset2=Integer.parseInt(args[7]); 
		String Splitter2=args[8];
		Integer condition=Integer.parseInt(args[9]);
		Integer gridhorizontal=Integer.parseInt(args[10]);
		Integer gridvertical=Integer.parseInt(args[11]);
		String outputlocation="hdfs://"+IP+":54310/test/tempResult.txt";
		URI uri=URI.create(outputlocation);
		Path pathhadoop=new Path(uri);
		SparkConf conf=new SparkConf().setAppName("GeoSpark_SpatialJoin").setMaster("spark://"+IP+":7077").set("spark.cores.max", "136").set("spark.executor.memory", "50g").set("spark.executor.cores", "8").set("spark.local.ip", IP).set("spark.driver.memory", "13g").set("spark.driver.cores", "8").set("spark.driver.host", IP).set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/").set("spark.history.retainedApplications", "1000");//.set("spark.reducer.maxSizeInFlight","1000m" );//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(kryo).set("spark.shuffle.memoryFraction", "0.2").set("spark.storage.memoryFraction", "0.6");//.set("spark.akka.failuredetector.threshold", "300000").set("spark.akka.threads", "300").set("spark.akka.timeout", "30000").set("spark.akka.askTimeout", "300").set("spark.storage.blockManagerTimeoutIntervalMs", "180000").set("spark.network.timeout", "10000").set("spark.blockManagerHeartBeatMs", "80000");
		JavaSparkContext spark=new JavaSparkContext(conf);
		Configuration confhadoop=spark.hadoopConfiguration();
		confhadoop.set("fs.hdfs.impl", 
			        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
			    );
		confhadoop.set("fs.file.impl",
			        org.apache.hadoop.fs.LocalFileSystem.class.getName()
			    );
		FileSystem filehadoop =FileSystem.get(uri, confhadoop);
		filehadoop.delete(pathhadoop, true);
		System.out.println("Old output file has been deleted!");
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		PointRDD pointRDD=new PointRDD(spark,set1,offset1,Splitter1,partitions1);
		RectangleRDD rectangleRDD=new RectangleRDD(spark,querywindowset,offset2,Splitter2,partitions2);
/*------------------------ Spatial Join query without spatial index --------------------*/
		SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD.SpatialJoinQuery(rectangleRDD,condition, gridhorizontal, gridvertical);
/*------------------------ Spatial Join query with local Quad-Tree index --------------------*/
//  SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD.SpatialJoinQueryWithIndex(rectangleRDD, gridhorizontal, gridvertical,"quadtree");
/*------------------------ Spatial Join query with local R-Tree index --------------------*/ 
//		  SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD.SpatialJoinQueryWithIndex(rectangleRDD, gridhorizontal, gridvertical,"rtree");
		join.getSpatialPairRDD().saveAsTextFile(outputlocation);
		spark.stop();
		
	
	}

}
