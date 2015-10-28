/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.datasyslab.geospark.utils.*;
import org.datasyslab.geospark.boundryFilter.*;
import org.datasyslab.geospark.rangeFilter.*;
import org.datasyslab.geospark.partition.*;

// TODO: Auto-generated Javadoc
class RectangleFormatMapper implements Serializable,Function<String,Envelope>
{
	Integer offset=0;
	String splitter="csv";
	public RectangleFormatMapper(Integer Offset,String Splitter)
	{
		this.offset=Offset;
		this.splitter=Splitter;
	}
	public Envelope call(String s)
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
		 return envelope;
	}
}

/**
 * The Class RectangleRDD.
 */
public class RectangleRDD implements Serializable {

	/** The rectangle rdd. */
	private JavaRDD<Envelope> rectangleRDD;
	
	/**
	 * Instantiates a new rectangle rdd.
	 *
	 * @param rectangleRDD the rectangle rdd
	 */
	public RectangleRDD(JavaRDD<Envelope> rectangleRDD)
	{
		this.setRectangleRDD(rectangleRDD.cache());
	}
	
	/**
	 * Instantiates a new rectangle rdd.
	 *
	 * @param spark the spark
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param Splitter the splitter
	 * @param partitions the partitions
	 */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)
	{
		//final Integer offset=Offset;
		this.setRectangleRDD(spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,Splitter)).cache());
	}
	
	/**
	 * Instantiates a new rectangle rdd.
	 *
	 * @param spark the spark
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param Splitter the splitter
	 */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)
	{
		//final Integer offset=Offset;
		this.setRectangleRDD(spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,Splitter)).cache());
	}
	
	/**
	 * Gets the rectangle rdd.
	 *
	 * @return the rectangle rdd
	 */
	public JavaRDD<Envelope> getRectangleRDD() {
		return rectangleRDD;
	}
	
	/**
	 * Sets the rectangle rdd.
	 *
	 * @param rectangleRDD the new rectangle rdd
	 */
	public void setRectangleRDD(JavaRDD<Envelope> rectangleRDD) {
		this.rectangleRDD = rectangleRDD;
	}
	
	/**
	 * Re partition.
	 *
	 * @param partitions the partitions
	 * @return the java rdd
	 */
	public JavaRDD<Envelope> rePartition(Integer partitions)
	{
		return this.rectangleRDD.repartition(partitions);
	}
	
	
	
	/**
	 * Boundary.
	 *
	 * @return the envelope
	 */
	public Envelope boundary()
	{
		Double[] boundary = new Double[4];
		Double minLongtitude1=this.rectangleRDD.min((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min")).getMinX();
		Double maxLongtitude1=this.rectangleRDD.max((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min")).getMinX();
		Double minLatitude1=this.rectangleRDD.min((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min")).getMinY();
		Double maxLatitude1=this.rectangleRDD.max((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min")).getMinY();
		Double minLongtitude2=this.rectangleRDD.min((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max")).getMaxX();
		Double maxLongtitude2=this.rectangleRDD.max((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max")).getMaxX();
		Double minLatitude2=this.rectangleRDD.min((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max")).getMaxY();
		Double maxLatitude2=this.rectangleRDD.max((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max")).getMaxY();
		if(minLongtitude1<minLongtitude2)
		{
			boundary[0]=minLongtitude1;
		}
		else
		{
			boundary[0]=minLongtitude2;
		}
		if(minLatitude1<minLatitude2)
		{
			boundary[1]=minLatitude1;
		}
		else
		{
			boundary[1]=minLatitude2;
		}
		if(maxLongtitude1>maxLongtitude2)
		{
			boundary[2]=maxLongtitude1;
		}
		else
		{
			boundary[2]=maxLongtitude2;
		}
		if(maxLatitude1>maxLatitude2)
		{
			boundary[3]=maxLatitude1;
		}
		else
		{
			boundary[3]=maxLatitude2;
		}
		return new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
	}
	
	/**
	 * Spatial join query with index.
	 *
	 * @param Condition the condition
	 * @param GridNumberHorizontal the grid number horizontal
	 * @param GridNumberVertical the grid number vertical
	 * @param Index the index
	 * @return the spatial pair rdd
	 */
	public SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQueryWithIndex(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical,String Index)
	{
		//Find the border of both of the two datasets---------------
		final String index=Index;
		final Integer condition=Condition;
		//Find the border of both of the two datasets---------------
				//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
				//QueryAreaSet min/max longitude and latitude
				//TargetSet min/max longitude and latitude
				Envelope boundary=this.boundary();;
				//Border found
	
		//Build Grid file-------------------
				Double[] gridHorizontalBorder = new Double[GridNumberHorizontal+1];
				Double[] gridVerticalBorder=new Double[GridNumberVertical+1];
				double LongitudeIncrement=(boundary.getMaxX()-boundary.getMinX())/GridNumberHorizontal;
				double LatitudeIncrement=(boundary.getMaxY()-boundary.getMinY())/GridNumberVertical;
				for(int i=0;i<GridNumberHorizontal+1;i++)
				{
					gridHorizontalBorder[i]=boundary.getMinX()+LongitudeIncrement*i;
				}
				for(int i=0;i<GridNumberVertical+1;i++)
				{
					gridVerticalBorder[i]=boundary.getMinY()+LatitudeIncrement*i;
				}
		//Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer,Envelope> TargetSetWithIDtemp=this.rectangleRDD.mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithIDtemp=TargetSetWithIDtemp;
		//Remove cache from memory
		this.rectangleRDD.unpersist();
		JavaPairRDD<Integer,Envelope> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
		JavaPairRDD<Integer,Tuple2<Iterable<Envelope>,Iterable<Envelope>>> cogroupSet=QueryAreaSetWithID.cogroup(TargetSetWithID, TargetSetWithIDtemp.partitions().size()*2);
		
		JavaPairRDD<Envelope,ArrayList<Envelope>> queryResult=cogroupSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Envelope>,Iterable<Envelope>>>,Envelope,ArrayList<Envelope>>()
				{

					public Iterable<Tuple2<Envelope, ArrayList<Envelope>>> call(
							Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Envelope>>> t)
					{
						
							if(index=="quadtree")
							{
								Quadtree qt=new Quadtree();
								Iterator<Envelope> targetIterator=t._2()._2().iterator();
								Iterator<Envelope> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Envelope,ArrayList<Envelope>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Envelope currentTarget=targetIterator.next();
									qt.insert(currentTarget, currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Envelope currentQueryArea=queryAreaIterator.next();
									List<Envelope> queryList=qt.query(currentQueryArea);
									if(queryList.size()!=0){
									result.add(new Tuple2<Envelope,ArrayList<Envelope>>(currentQueryArea,new ArrayList<Envelope>(queryList)));
									}
								}
								return result;
							}
							else
							{
								STRtree rt=new STRtree();
								Iterator<Envelope> targetIterator=t._2()._2().iterator();
								Iterator<Envelope> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Envelope,ArrayList<Envelope>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Envelope currentTarget=targetIterator.next();
									rt.insert(currentTarget, currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Envelope currentQueryArea=queryAreaIterator.next();
									List<Envelope> queryList=rt.query(currentQueryArea);
									if(queryList.size()!=0){
									result.add(new Tuple2<Envelope,ArrayList<Envelope>>(currentQueryArea,new ArrayList<Envelope>(queryList)));
									}
								}
								return result;
							}
						
					}
			
				});
		//Delete the duplicate result
				JavaPairRDD<Envelope, ArrayList<Envelope>> aggregatedResult=queryResult.reduceByKey(new Function2<ArrayList<Envelope>,ArrayList<Envelope>,ArrayList<Envelope>>()
						{

							public ArrayList<Envelope> call(ArrayList<Envelope> v1,
									ArrayList<Envelope> v2) {
								ArrayList<Envelope> v3=v1;
								v3.addAll(v2);
								return v2;
							}
					
						});
				JavaPairRDD<Envelope,ArrayList<Envelope>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Envelope,ArrayList<Envelope>>,Envelope,ArrayList<Envelope>>()
						{

							public Tuple2<Envelope, ArrayList<Envelope>> call(Tuple2<Envelope, ArrayList<Envelope>> v){
								ArrayList<Envelope> result=new ArrayList<Envelope>();
								Iterator<Envelope> targetIterator=v._2().iterator();
								for(int i=0;i<v._2().size();i++)
								{
									Integer duplicationFlag=0;
									Envelope currentPointi=v._2().get(i);
									for(int j=i+1;j<v._2().size();j++)
									{
										Envelope currentPointj=v._2().get(j);
										if(currentPointi.equals(currentPointj))
										{
											duplicationFlag=1;
										}
									}
									if(duplicationFlag==0)
									{
										result.add(currentPointi);
									}
								}
								return new Tuple2<Envelope,ArrayList<Envelope>>(v._1(),result);
							}
					
						});
				SpatialPairRDD<Envelope,ArrayList<Envelope>> result=new SpatialPairRDD<Envelope,ArrayList<Envelope>>(refinedResult);
		return result;
	}
}
