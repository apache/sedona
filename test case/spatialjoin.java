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
		SparkConf conf=new SparkConf().setAppName("GeoSpark_SpatialJoin").setMaster("spark://"+IP+":7077").set("spark.cores.max", "136").set("spark.executor.memory", "59g").set("spark.executor.cores", "8").set("spark.local.ip", IP).set("spark.driver.memory", "13g").set("spark.driver.cores", "8").set("spark.driver.host", IP).set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/").set("spark.history.retainedApplications", "1000");//.set("spark.reducer.maxSizeInFlight","1000m" );//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(kryo).set("spark.shuffle.memoryFraction", "0.2").set("spark.storage.memoryFraction", "0.6");//.set("spark.akka.failuredetector.threshold", "300000").set("spark.akka.threads", "300").set("spark.akka.timeout", "30000").set("spark.akka.askTimeout", "300").set("spark.storage.blockManagerTimeoutIntervalMs", "180000").set("spark.network.timeout", "10000").set("spark.blockManagerHeartBeatMs", "80000");
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
		//Add full jar which contains all the dependencies
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		PointRDD pointRDD1=new PointRDD(spark,set1,offset1,Splitter1,partitions1);
		RectangleRDD rectangleRDD2=new RectangleRDD(spark,querywindowset,offset2,Splitter2,partitions2);
		SpatialPairRDD<Envelope,ArrayList<Point>> join=pointRDD1.SpatialJoinQuery(rectangleRDD2,condition, gridhorizontal, gridvertical);
		join.getSpatialPairRDD().saveAsTextFile(outputlocation);
		spark.stop();
		
	
	}

}
