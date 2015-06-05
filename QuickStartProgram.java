package GeoSpark;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class QuickStartProgram {

	public static void main(String[] args) {

		Scanner scan = new Scanner(System.in);
		System.out.println("GeoSpark Quick Start - Range Query");
		System.out.println("Please enter the path of the source file in HDFS:");
		String sHadoop = scan.next();
		//System.out.println("Please enter the query window of range query in HDFS:");
		//String querywindowString = scan.next();
		System.out.println("Please enter the output location in HDFS:");
		//String outputlocation = scan.next();
		String outputlocation="hdfs://192.168.56.101:54310/test/tempResult.txt";
		//URI uri=URI.create("hdfs://192.168.56.101:54310/test/tempResult.txt");
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
		//Declare query window
		//Envelope querywindow=new Envelope(Double.parseDouble(Arrays.asList(querywindowString.split(",")).get(0)),Double.parseDouble(Arrays.asList(querywindowString.split(",")).get(2)),Double.parseDouble(Arrays.asList(querywindowString.split(",")).get(1)),Double.parseDouble(Arrays.asList(querywindowString.split(",")).get(3)));
		
		SparkConf conf=new SparkConf().setAppName("GeoSpark").setMaster("spark://192.168.56.101:7077").set("spark.executor.memory", "2g").set("spark.local.ip", "192.168.56.101").set("spark.driver.host", "192.168.56.101").set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/");
		JavaSparkContext spark=new JavaSparkContext(conf);
		//Add full jar which contains all the dependencies
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		RectangleRDD rectangleRDD1=new RectangleRDD(spark,"hdfs://192.168.56.101:54310/testjointarget");
		//rectangleRDD.rePartition(20);
		RectangleRDD rectangleRDD2=new RectangleRDD(spark,"hdfs://192.168.56.101:54310/testjoinquery");
		//pointRDD1.rePartition(20);
		SpatialPairRDD<Envelope,ArrayList<Envelope>> join=rectangleRDD1.SpatialJoinQuery(rectangleRDD2,0, 2, 2);
		try {
			join.countByKey().getSpatialPairRDD().repartition(1).saveAsTextFile("hdfs://192.168.56.101:54310/test/tempResult.txt");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Polygon polygon=polygonRDD.PolygonUnion();
		//System.out.println(polygon);
		//PointRDD pointRDD=new PointRDD(spark,sHadoop);
		//pointRDD.rePartition(8);
		//PolygonRDD polygonRDD=new PolygonRDD(spark,sHadoop);
		//polygonRDD.rePatitrtition(8);
		//JavaPairRDD<Polygon,String> join=pointRDD.SpatialJoinQueryWithMBR(polygonRDD, 1, 25, 25);
		
		
		
		//PolygonRDD polygonRDD2=new PolygonRDD(spark,sHadoop);
		//polygonRDD2.rePatitrtition(8);

		//PointRDD pointR=new PointRDD(spark,sHadoop);
		//rectangleR1.rePartition(20);
		//pointR.rePartition(20);
		//RectangleRDD rectangleR2=new RectangleRDD(spark,sHadoop);
		//rectangleR2.rePartition(20);
		//JavaPairRDD<Envelope,String> join=pointR.SpatialJoinQuery(rectangleR2, 0, 5, 5);
		
		//Call spatial range query. The number 1 is which means consider the overlap cases. If it is 0, that means only consider fully contain cases.
		//RectangleRDD rectangleR2=rectangleR1.SpatialRangeQuery(querywindow, 1);
		//rectangleR2.getRectangleRDD().saveAsTextFile("hdfs://192.168.56.101:54310/test/tempResult.txt");
		//rectangleR2.getRectangleRDD().saveAsTextFile(outputlocation);
		scan.close();
	
	}

}
