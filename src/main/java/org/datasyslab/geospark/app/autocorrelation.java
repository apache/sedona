package org.datasyslab.geospark.app;
/* This is an example of GeoSpark spatial autocorrelation
 * Introduction: Spatial autocorrelation studies whether neighbor spatial data points might have correlations in some non-spatial attributes. 
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
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
import GeoSpark.PointRDD;
import GeoSpark.RectangleRDD;
import GeoSpark.SpatialPairRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

class SpatialPairFormatMapper implements Serializable,PairFunction<String,Envelope,Integer>
{
	Integer offset=0;
	String splitter="csv";
	public SpatialPairFormatMapper(Integer Offset,String Splitter)
	{
		this.offset=Offset;
		this.splitter=Splitter;
	}
	public Tuple2<Envelope,Integer> call(String s)
	{	
		
		String seperater=",";
		if(this.splitter.contains("tsv"))
		{
			seperater="\t";
		}
		else
		{
			seperater=",";
		}
		 List<String> input=Arrays.asList(s.split(seperater));
		 
		 Envelope envelope = new Envelope(Double.parseDouble(input.get(0+offset)),Double.parseDouble(input.get(2+offset)),Double.parseDouble(input.get(1+offset)),Double.parseDouble(input.get(3+offset)));
		 Integer coefficient=Integer.parseInt(input.get(4+offset));
		 return new Tuple2<Envelope,Integer>(envelope,coefficient);
	}
}
public class autocorrelation {

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
		SparkConf conf=new SparkConf().setAppName("GeoSpark_Autocorrelation").setMaster("spark://"+IP+":7077");//.set("spark.cores.max", "136").set("spark.executor.memory", "50g").set("spark.executor.cores", "8").set("spark.local.ip", IP).set("spark.driver.memory", "13g").set("spark.driver.cores", "8").set("spark.driver.host", IP).set("spark.shuffle.memoryFraction", "0.2").set("spark.storage.memoryFraction", "0.6").set("spark.eventLog.enabled","true").set("spark.eventLog.dir", "/tmp/spark-events/").set("spark.history.retainedApplications", "1000");
		JavaSparkContext spark=new JavaSparkContext(conf);
		//Add full jar which contains all the dependencies
		spark.addJar("target/GeoSpark-0.1-GeoSpark.jar");
		SpatialPairRDD<Envelope,Integer> index1=new SpatialPairRDD<Envelope,Integer>(spark.textFile(set1,partitions1).mapToPair(new SpatialPairFormatMapper(offset1,Splitter1)).cache());
		SpatialPairRDD<Envelope,Double> index=new SpatialPairRDD<Envelope,Double>(index1.getSpatialPairRDD().mapToPair(new PairFunction<Tuple2<Envelope,Integer>,Envelope,Double>()
					{

						public Tuple2<Envelope, Double> call(
								Tuple2<Envelope, Integer> t) {
							return new Tuple2<Envelope,Double>(t._1(),Double.parseDouble(t._2().toString()));
						}
						
					}));
			RectangleRDD index2=new RectangleRDD(index.getSpatialPairRDD().map(new Function<Tuple2<Envelope,Double>,Envelope>()
					{

						public Envelope call(Tuple2<Envelope, Double> v1){
							return v1._1();
						}
				
					}));


/*------------------------ Spatial Join query without spatial index --------------------*/
			SpatialPairRDD<Envelope,ArrayList<Envelope>> join1=index2.SpatialJoinQuery(condition, gridhorizontal, gridvertical);
			/*------------------------ Spatial Join query with local Quad-Tree index --------------------*/
//			SpatialPairRDD<Envelope,ArrayList<Envelope>> join1=index2.SpatialJoinQueryWithIndex(condition,gridhorizontal, gridvertical,"quadtree");
			/*------------------------ Spatial Join query with local R-Tree index --------------------*/ 
//			SpatialPairRDD<Envelope,ArrayList<Envelope>> join1=index2.SpatialJoinQueryWithIndex(condition,gridhorizontal, gridvertical,"rtree");
			JavaPairRDD<Envelope,Envelope> AdjacentMatrix1=join1.getSpatialPairRDD().flatMapToPair(new PairFlatMapFunction<Tuple2<Envelope,ArrayList<Envelope>>,Envelope,Envelope>()
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
		//Find the total number of the query window set
		Long totalNumber=index.getSpatialPairRDD().count();

		//Get the average of eco index
		final double average=total/totalNumber;

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
		spark.stop();
		Double moranI=element/denominator;
		System.out.println(moranI);
		
	}

}
