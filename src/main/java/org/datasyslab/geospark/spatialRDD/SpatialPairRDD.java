/*
 * 
 */
package GeoSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class SpatialPairRDD.
 *
 * @param <T1> the generic type
 * @param <T2> the generic type
 */
public class SpatialPairRDD<T1,T2> implements Serializable{
	
	/** The t1. */
	private T1 t1;
	
	/** The t2. */
	private T2 t2;
	
	/** The spatial pair rdd. */
	private JavaPairRDD<T1,T2> spatialPairRDD;
	
	/**
	 * Instantiates a new spatial pair rdd.
	 *
	 * @param spatialPairRDD the spatial pair rdd
	 */
	public SpatialPairRDD(JavaPairRDD<T1,T2> spatialPairRDD)
	{
		this.setSpatialPairRDD(spatialPairRDD);
	}

	/**
	 * Gets the t1.
	 *
	 * @return the t1
	 */
	public T1 getT1() {
		return t1;
	}
	
	/**
	 * Sets the t1.
	 *
	 * @param t1 the new t1
	 */
	public void setT1(T1 t1) {
		this.t1 = t1;
	}
	
	/**
	 * Gets the t2.
	 *
	 * @return the t2
	 */
	public T2 getT2() {
		return t2;
	}
	
	/**
	 * Sets the t2.
	 *
	 * @param t2 the new t2
	 */
	public void setT2(T2 t2) {
		this.t2 = t2;
	}
	
	/**
	 * Gets the spatial pair rdd.
	 *
	 * @return the spatial pair rdd
	 */
	public JavaPairRDD<T1,T2> getSpatialPairRDD() {
		return spatialPairRDD;
	}
	
	/**
	 * Sets the spatial pair rdd.
	 *
	 * @param spatialPairRDD the spatial pair rdd
	 */
	public void setSpatialPairRDD(JavaPairRDD<T1,T2> spatialPairRDD) {
		this.spatialPairRDD = spatialPairRDD;
	}
	
	/**
	 * Sava as file.
	 *
	 * @param OutputLocation the output location
	 */
	public void SavaAsFile(String OutputLocation)
	{
		this.getSpatialPairRDD().saveAsTextFile(OutputLocation);
	}
	
	/**
	 * Flat map to point.
	 *
	 * @return the spatial pair rdd
	 */
	public SpatialPairRDD<T1,Point> FlatMapToPoint()
	{
		JavaPairRDD<T1,ArrayList<Point>> spatialPairRDDTemp=(JavaPairRDD<T1, ArrayList<Point>>) this.spatialPairRDD;
		SpatialPairRDD<T1,Point> spatialPairRDDflat=new SpatialPairRDD<T1,Point>(spatialPairRDDTemp.flatMapToPair(new PairFlatMapFunction<Tuple2<T1,ArrayList<Point>>,T1,Point>()
				{

					public Iterable<Tuple2<T1, Point>> call(
							Tuple2<T1, ArrayList<Point>> t) {
						ArrayList<Tuple2<T1,Point>> result=new ArrayList<Tuple2<T1,Point>>();
						Iterator<Point> targetIterator=t._2().iterator();
						while(targetIterator.hasNext())
						{
							result.add(new Tuple2<T1,Point>(t._1(),targetIterator.next()));
						}
						return result;
					}
			
				}));
		return spatialPairRDDflat;
	}
	
	/**
	 * Flat map to rectangle.
	 *
	 * @return the spatial pair rdd
	 */
	public SpatialPairRDD<T1,Envelope> FlatMapToRectangle()
	{
		JavaPairRDD<T1,ArrayList<Envelope>> spatialPairRDDTemp=(JavaPairRDD<T1, ArrayList<Envelope>>) this.spatialPairRDD;
		SpatialPairRDD<T1,Envelope> spatialPairRDDflat=new SpatialPairRDD<T1,Envelope>(spatialPairRDDTemp.flatMapToPair(new PairFlatMapFunction<Tuple2<T1,ArrayList<Envelope>>,T1,Envelope>()
				{

					public Iterable<Tuple2<T1, Envelope>> call(
							Tuple2<T1, ArrayList<Envelope>> t) {
						ArrayList<Tuple2<T1,Envelope>> result=new ArrayList<Tuple2<T1,Envelope>>();
						Iterator<Envelope> targetIterator=t._2().iterator();
						while(targetIterator.hasNext())
						{
							result.add(new Tuple2<T1,Envelope>(t._1(),targetIterator.next()));
						}
						return result;
					}
			
				}));
		return spatialPairRDDflat;
	}
	
	/**
	 * Flat map to polygon.
	 *
	 * @return the spatial pair rdd
	 */
	public SpatialPairRDD<T1,Polygon> FlatMapToPolygon()
	{
		JavaPairRDD<T1,ArrayList<Polygon>> spatialPairRDDTemp=(JavaPairRDD<T1, ArrayList<Polygon>>) this.spatialPairRDD;
		SpatialPairRDD<T1,Polygon> spatialPairRDDflat=new SpatialPairRDD<T1,Polygon>(spatialPairRDDTemp.flatMapToPair(new PairFlatMapFunction<Tuple2<T1,ArrayList<Polygon>>,T1,Polygon>()
				{

					public Iterable<Tuple2<T1, Polygon>> call(
							Tuple2<T1, ArrayList<Polygon>> t) {
						ArrayList<Tuple2<T1,Polygon>> result=new ArrayList<Tuple2<T1,Polygon>>();
						Iterator<Polygon> targetIterator=t._2().iterator();
						while(targetIterator.hasNext())
						{
							result.add(new Tuple2<T1,Polygon>(t._1(),targetIterator.next()));
						}
						return result;
					}
			
				}));
		return spatialPairRDDflat;
	}
	
	/**
	 * Count by key.
	 *
	 * @return the spatial pair rdd
	 */
	public SpatialPairRDD<T1, Integer> countByKey()
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
