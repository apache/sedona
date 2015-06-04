import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
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
		// TODO Auto-generated method stub
		Scanner scan = new Scanner(System.in);
		System.out.println("GeoSpark Quick Start - Colocation");
		System.out.println("Please enter the first pointset in HDFS:");
		String set1 = scan.next();
		System.out.println("Please enter the second point in HDFS:");
		String querywindowset = scan.next();
		System.out.println("Please enter the start distance, end distance and number of points on X -axis");
		String input = scan.next();
		//System.out.println("Please enter the output location in HDFS:");
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
		PointRDD pointRDD1=new PointRDD(spark,set1);
		pointRDD1.rePartition(20);
		PointRDD pointRDD2=new PointRDD(spark,querywindowset);
		pointRDD2.rePartition(20);
		Double minLongitude=0.0;
		Double maxLongitude=0.0;
		Double minLatitude=0.0;
		Double maxLatitude=0.0;
		Double[] b1=pointRDD1.boundary();
		Double[] b2=pointRDD2.boundary();
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
		//System.out.println(area);
		Long n1=pointRDD1.getPointRDD().count();
		//System.out.println(n1);
		Long n2=pointRDD2.getPointRDD().count();
		//System.out.println(n2);
		for(int i=0;i<points;i++){
		CircleRDD circleRDD=new CircleRDD(pointRDD2.getPointRDD().map(new PointToCircle(distances[i])));
		SpatialPairRDD<Circle,Point> adjacentMatrix=pointRDD1.SpatialJoinQuery(circleRDD, 1, 100, 100).FlatMapToPoint();
		Long sum=adjacentMatrix.getSpatialPairRDD().count();
		functions[i]=Math.sqrt((area*sum)/(3.1415926*n1*n2))-distances[i];
		}
		System.out.println(area);
		System.out.println(n1);
		System.out.println(n2);
		System.out.println(distances[0]);
		System.out.println(distances[1]);
		System.out.println(distances[2]);
		System.out.println("--------------------");
		System.out.println(functions[0]);
		System.out.println(functions[1]);
		System.out.println(functions[2]);
	}

}