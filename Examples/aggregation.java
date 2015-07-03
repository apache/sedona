/* This is an example of GeoSpark spatial aggregation
 * Introduction: Spatial aggregation aggregates the results from spatial join query. One common scenario is heat map. 
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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import GeoSpark.PointRDD;
import GeoSpark.RectangleRDD;
import GeoSpark.SpatialPairRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;


public class aggregation {

	public static void main(String[] args) {
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
		
		SparkConf conf=new SparkConf().setAppName("GeoSpark_Aggregation").setMaster("spark://"+IP+":7077").set("spark.cores.max", "136").set("spark.executor.memory", "50g").set("spark.executor.cores", "8").set("spark.local.ip", IP).set("spark.driver.memory", "13g").set("spark.driver.cores", "8").set("spark.driver.host", IP).set("spark.shuffle.memoryFraction", "0.2").set("spark.storage.memoryFraction", "0.6").set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/").set("spark.history.retainedApplications", "1000");		
		JavaSparkContext spark=new JavaSparkContext(conf);

		Configuration confhadoop=spark.hadoopConfiguration();
		confhadoop.set("fs.hdfs.impl", 
			        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
			    );
		confhadoop.set("fs.file.impl",
			        org.apache.hadoop.fs.LocalFileSystem.class.getName()
			    );
		try {
			FileSystem filehadoop =FileSystem.get(uri, confhadoop);
			filehadoop.delete(pathhadoop, true);
			System.out.println("Old output file has been deleted!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Add full jar which contains all the dependencies
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		PointRDD pointRDD=new PointRDD(spark,set1,offset1,Splitter1,partitions1);
		RectangleRDD rectangleRDD=new RectangleRDD(spark,querywindowset,offset2,Splitter2,partitions2);
/*------------------------ Spatial Join query without spatial index --------------------*/
		SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD.SpatialJoinQuery(rectangleRDD,condition, gridhorizontal, gridvertical);
/*------------------------ Spatial Join query with local Quad-Tree index --------------------*/
//  SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD.SpatialJoinQueryWithIndex(rectangleRDD, gridhorizontal, gridvertical,"quadtree");
/*------------------------ Spatial Join query with local R-Tree index --------------------*/ 
//		  SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD.SpatialJoinQueryWithIndex(rectangleRDD, gridhorizontal, gridvertical,"rtree");
/*--------------------------- Aggregate join query result ------------------------------*/
		join.countByKey().getSpatialPairRDD().saveAsTextFile(outputlocation);
		spark.stop();

	
	}

	

}
