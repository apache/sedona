/* This is an example of GeoSpark spatial co-location
 * Introduction: Spatial co-location is defined as two or more species are often located in a neighborhood relationship. 
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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import GeoSpark.Circle;
import GeoSpark.CircleRDD;
import GeoSpark.PointRDD;
import GeoSpark.SpatialPairRDD;

class PointToCircle implements Function<Point,Circle>, Serializable
{
	private Double radius;
	public PointToCircle(Double radius)
	{
		this.radius=radius;
	}
	public Circle call(Point v1) throws Exception {
		return new Circle(v1,radius);
	}
	
}
public class colocation {

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
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
		String input=args[12];
		String outputlocation="hdfs://"+IP+":54310/test/tempResult.txt";
		URI uri=URI.create(outputlocation);
		Path pathhadoop=new Path(uri);
		Configuration confhadoop=new Configuration();
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
		SparkConf conf=new SparkConf().setAppName("GeoSpark_Colocation").setMaster("spark://"+IP+":7077").set("spark.cores.max", "136").set("spark.executor.memory", "50g").set("spark.executor.cores", "8").set("spark.local.ip", IP).set("spark.driver.memory", "13g").set("spark.driver.cores", "8").set("spark.driver.host", IP).set("spark.shuffle.memoryFraction", "0.2").set("spark.storage.memoryFraction", "0.6").set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/").set("spark.history.retainedApplications", "1000").set("spark.akka.failuredetector.threshold", "300000");//.set("spark.default.parallelism", "8");
		JavaSparkContext spark=new JavaSparkContext(conf);
		//Add full jar which contains all the dependencies
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		Double start=Double.parseDouble(Arrays.asList(input.split(",")).get(0));
		Double end=Double.parseDouble(Arrays.asList(input.split(",")).get(1));
		Integer points=Integer.parseInt(Arrays.asList(input.split(",")).get(2));
		final Double[] distances=new Double[points];
		Double[] functions=new Double[points];
		Double increment=(end-start)/(points-1);
		for(int i=0;i<points;i++)
		{
		
			distances[i]=start+increment*i;
		}
		PointRDD pointRDD1=new PointRDD(spark,set1,offset1,Splitter1,partitions1);
		PointRDD pointRDD2=new PointRDD(spark,querywindowset,offset2,Splitter2,partitions2);
		Double minLongitude=0.0;
		Double maxLongitude=0.0;
		Double minLatitude=0.0;
		Double maxLatitude=0.0;
		Double[] b1={pointRDD1.boundary().getMinX(),pointRDD1.boundary().getMinY(),pointRDD1.boundary().getMaxX(),pointRDD1.boundary().getMaxY()};
		Double[] b2={pointRDD2.boundary().getMinX(),pointRDD2.boundary().getMinY(),pointRDD2.boundary().getMaxX(),pointRDD2.boundary().getMaxY()};
		if(b1[0]<b2[0])
		{
			minLongitude=b1[0];
		}
		else
		{
			minLongitude=b2[0];
		}
		if(b1[1]<b2[1])
		{
			minLatitude=b1[1];
		}
		else
		{
			minLatitude=b2[1];
		}
		if(b1[2]>b2[2])
		{
			maxLongitude=b1[2];
		}
		else
		{
			maxLongitude=b2[2];
		}

		if(b1[3]>b2[3])
		{
			maxLatitude=b1[3];
		}
		else
		{
			maxLatitude=b2[3];
		}
		Double area=(maxLongitude-minLongitude)*(maxLatitude-minLatitude);
		Long n1=pointRDD1.getPointRDD().count();
		Long n2=pointRDD2.getPointRDD().count();
		for(int i=0;i<points;i++){
		CircleRDD circleRDD=new CircleRDD(pointRDD2.getPointRDD().map(new PointToCircle(distances[i])));
		pointRDD2.getPointRDD().unpersist();
		/*------------------------ Spatial Join query without spatial index --------------------*/
		SpatialPairRDD<Point,Point> adjacentMatrix=pointRDD1.SpatialJoinQuery(circleRDD,distances[i], condition, gridhorizontal, gridvertical).FlatMapToPoint();
		/*------------------------ Spatial Join query with local Quad-Tree index --------------------*/
//		SpatialPairRDD<Envelope,ArrayList<Envelope>> adjacentMatrix=pointRDD1.SpatialJoinQueryWithIndex(circleRDD,gridhorizontal, gridvertical,"quadtree");
		/*------------------------ Spatial Join query with local R-Tree index --------------------*/ 
//		SpatialPairRDD<Envelope,ArrayList<Envelope>> adjacentMatrix=pointRDD1.SpatialJoinQueryWithIndex(circleRDD,gridhorizontal, gridvertical,"rtree");
		Long sum=adjacentMatrix.getSpatialPairRDD().count();
		functions[i]=Math.sqrt((area*sum)/(3.1415926*n1*n2))-distances[i];
		}
		spark.stop();


	}

}
