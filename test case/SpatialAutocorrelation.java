import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import GeoSpark.RectangleRDD;
import GeoSpark.SpatialPairRDD;

import com.vividsolutions.jts.geom.Envelope;


public class autocorrelation {

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		System.out.println("GeoSpark Quick Start - Autocorelation");
		System.out.println("Please enter the path of the source file in HDFS:");
		String sHadoop = scan.next();
		System.out.println("Please enter the query window of range query in HDFS:");
		String querywindowString = scan.next();
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
		
		SparkConf conf=new SparkConf().setAppName("GeoSpark-Autocorrelation").setMaster("spark://192.168.56.101:7077").set("spark.executor.memory", "2g").set("spark.local.ip", "192.168.56.101").set("spark.driver.host", "192.168.56.101").set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/");
		JavaSparkContext spark=new JavaSparkContext(conf);
		//Add full jar which contains all the dependencies
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		RectangleRDD rectangleRDD=new RectangleRDD(spark,sHadoop);
		rectangleRDD.rePartition(8);
		RectangleRDD rectangleRDD1=new RectangleRDD(spark,querywindowString);
		rectangleRDD1.rePartition(8);
		SpatialPairRDD<Envelope,ArrayList<Envelope>> join=rectangleRDD.SpatialJoinQuery(rectangleRDD1,1, 25, 25);
		
			SpatialPairRDD<Envelope,Integer> index1=join.countByKey();
			SpatialPairRDD<Envelope,Double> index=new SpatialPairRDD<Envelope,Double>(index1.getSpatialPairRDD().mapToPair(new PairFunction<Tuple2<Envelope,Integer>,Envelope,Double>()
					{

						public Tuple2<Envelope, Double> call(
								Tuple2<Envelope, Integer> t) {
							Double result=t._2()/t._1().getArea();
							return new Tuple2<Envelope, Double>(t._1(),result);
						}
						
					}));
		JavaPairRDD<Envelope,Envelope> AdjacentMatrix1=join.getSpatialPairRDD().flatMapToPair(new PairFlatMapFunction<Tuple2<Envelope,ArrayList<Envelope>>,Envelope,Envelope>()
				{

					public Iterable<Tuple2<Envelope, Envelope>> call(Tuple2<Envelope, ArrayList<Envelope>> t){
						ArrayList<Tuple2<Envelope,Envelope>> result=new ArrayList<Tuple2<Envelope,Envelope>>();
						Iterator<Envelope> targetIterator=t._2().iterator();
						while(targetIterator.hasNext())
						{
							result.add(new Tuple2<Envelope,Envelope>(t._1(),targetIterator.next()));
						}
						return result;
					}
			
				});
		JavaPairRDD<Envelope,Tuple2<Envelope,Double>> AdjacentMatrix2=AdjacentMatrix1.join(index.getSpatialPairRDD());
		JavaPairRDD<Envelope,Tuple2<Envelope,Double>> AdjacentMatrix3=AdjacentMatrix2.mapToPair(new PairFunction<Tuple2<Envelope,Tuple2<Envelope,Double>>,Envelope,Tuple2<Envelope,Double>>()
				{

					public Tuple2<Envelope, Tuple2<Envelope, Double>> call(
							Tuple2<Envelope, Tuple2<Envelope, Double>> t)
							{
						
						return new Tuple2<Envelope, Tuple2<Envelope, Double>>(t._2()._1(),new Tuple2<Envelope,Double>(t._1(),t._2()._2()));
					}
					
				});
		JavaPairRDD<Envelope,Tuple2<Tuple2<Envelope,Double>,Double>> AdjacentMatrix4=AdjacentMatrix3.join(index.getSpatialPairRDD());
		//Find the adjacent matrix, each member of each tuple has its own eco index
		JavaPairRDD<Tuple2<Envelope,Double>,Tuple2<Envelope,Double>> AdjacentMatrix=AdjacentMatrix4.mapToPair(new PairFunction<Tuple2<Envelope,Tuple2<Tuple2<Envelope,Double>,Double>>,Tuple2<Envelope,Double>,Tuple2<Envelope,Double>>()
				{

					public Tuple2<Tuple2<Envelope, Double>, Tuple2<Envelope, Double>> call(
							Tuple2<Envelope, Tuple2<Tuple2<Envelope, Double>, Double>> t){
						
						return new Tuple2(new Tuple2(t._1(),t._2()._2()),new Tuple2(t._2()._1()._1(),t._2()._1()._2()));
					}
					
				});
		JavaRDD<Double> total1=index.getSpatialPairRDD().map(new Function<Tuple2<Envelope,Double>,Double>()
				{

			public Double call(Tuple2<Envelope, Double> v1){
				return v1._2();
			}
			
		});
		Double total=total1.reduce(new Function2<Double,Double,Double>()
						{

							public Double call(Double v1, Double v2){

								return v1+v2;
							}
							
						});
	
		
		
		JavaRDD<Integer> totalNumber1=rectangleRDD1.getRectangleRDD().map(new Function<Envelope,Integer>()
				{

					public Integer call(Envelope v1) {
						
						return 1;
					}
						
				});
		//Find the total number of the query window set
		Integer totalNumber=totalNumber1.reduce(new Function2<Integer,Integer,Integer>()
				{

					public Integer call(Integer v1, Integer v2) {
						
						return v1+v2;
					}
			
				});
		System.out.println(totalNumber);
		//Get the average of eco index
		final double average=total/totalNumber;
		//System.out.println(average);
		//Calculate the element of Moran's I
		
		JavaRDD<Double> element1=AdjacentMatrix.map(new Function<Tuple2<Tuple2<Envelope,Double>,Tuple2<Envelope,Double>>,Double>()
				{

					public Double call(
							Tuple2<Tuple2<Envelope, Double>, Tuple2<Envelope, Double>> v1)
							{
					if(!v1._1()._1().equals(v1._2()._1()))
					{
						return (v1._1()._2()-average)*(v1._2()._2()-average);
					}
					else return 0.00;
					}
					
				});
		Double element2=element1.reduce(new Function2<Double,Double,Double>()
				{

					public Double call(Double v1, Double v2) throws Exception {
						
						return v1+v2;
					}
			
				});
		Double element=totalNumber*element2;
		//Calculate the denominator of Moran's I
		JavaRDD<Double> denominator1=index.getSpatialPairRDD().map(new Function<Tuple2<Envelope,Double>,Double>()
				{

			public Double call(Tuple2<Envelope, Double> v1)
					throws Exception {
				return (v1._2()-average)*(v1._2()-average);
			}
	
		});
		Double denominatorx=denominator1.reduce(new Function2<Double,Double,Double>()
						{

							public Double call(Double v1, Double v2){
							
								return v1+v2;
							}
							
						});
		final Double denominator2=denominatorx;
		JavaRDD<Double> denominator3=AdjacentMatrix1.map(new Function<Tuple2<Envelope,Envelope>,Double>()
				{

			public Double call(Tuple2<Envelope, Envelope> v1){
				
				if(!v1._1().equals(v1._2()))
				{
				return denominator2;
				}
				else return 0.0;
			}
			
		});
		Double denominator=denominator3.reduce(new Function2<Double,Double,Double>()
						{

							public Double call(Double v1, Double v2) {
								
								return v1+v2;
							}
					
						});
		Double moranI=element/denominator;
		System.out.println(moranI);
		
	}

}