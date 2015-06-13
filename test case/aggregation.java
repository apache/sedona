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
	
		SparkConf conf=new SparkConf().setAppName("GeoSpark_Aggregation").setMaster("spark://"+IP+":7077").set("spark.cores.max", "136").set("spark.executor.memory", "13g").set("spark.executor.cores", "8").set("spark.local.ip", IP).set("spark.driver.memory", "13g").set("spark.driver.cores", "8").set("spark.driver.host", IP).set("spark.shuffle.memoryFraction", "0").set("spark.storage.memoryFraction", "0.6").set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/").set("spark.history.retainedApplications", "1000");		
		JavaSparkContext spark=new JavaSparkContext(conf);
		//Add full jar which contains all the dependencies
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		PointRDD pointRDD1=new PointRDD(spark,set1,offset1,Splitter1,partitions1);

		RectangleRDD rectangleRDD1=new RectangleRDD(spark,querywindowset,offset2,Splitter2,partitions2);

		
		SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD1.SpatialJoinQuery(rectangleRDD1,condition, gridhorizontal, gridvertical);
		try {
			join.countByKey().getSpatialPairRDD().saveAsTextFile(outputlocation);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.stop();
	
	}

	

}
