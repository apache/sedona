package GeoSpark;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class SpatialPairRDD<T1,T2> implements Serializable{
	private T1 t1;
	private T2 t2;
	private JavaPairRDD<T1,T2> spatialPairRDD;
	public SpatialPairRDD(JavaPairRDD<T1,T2> spatialPairRDD)
	{
		this.setSpatialPairRDD(spatialPairRDD);
	}

	public T1 getT1() {
		return t1;
	}
	public void setT1(T1 t1) {
		this.t1 = t1;
	}
	public T2 getT2() {
		return t2;
	}
	public void setT2(T2 t2) {
		this.t2 = t2;
	}
	public JavaPairRDD<T1,T2> getSpatialPairRDD() {
		return spatialPairRDD;
	}
	public void setSpatialPairRDD(JavaPairRDD<T1,T2> spatialPairRDD) {
		this.spatialPairRDD = spatialPairRDD.cache();
	}
	public void SavaAsFile(String OutputLocation)
	{
		this.getSpatialPairRDD().saveAsTextFile(OutputLocation);
	}
	public SpatialPairRDD<T1, Integer> countByKey() throws Exception
	{
			JavaPairRDD<T1,ArrayList> spatialPairRDDTemp=(JavaPairRDD<T1, ArrayList>) this.spatialPairRDD;
			SpatialPairRDD<T1,Integer> spatialPairRDDcount= new SpatialPairRDD<T1,Integer> (spatialPairRDDTemp.mapToPair(new PairFunction<Tuple2<T1,ArrayList>,T1,Integer>()
					{

						public Tuple2<T1, Integer> call(Tuple2<T1, ArrayList> t) {
							
							return new Tuple2<T1,Integer>(t._1(),t._2().size());
						}
						
					}));
			return spatialPairRDDcount;
	}
}
