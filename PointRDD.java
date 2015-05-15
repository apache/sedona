package GeoSpark;

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
import Functions.PartitionAssignGridPoint;
import Functions.PartitionAssignGridPolygon;
import Functions.PartitionAssignGridRectangle;
import Functions.PointRangeFilter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class PointRDD implements Serializable{
	private JavaRDD<Point> pointRDD;
	public PointRDD(JavaRDD<Point> pointRDD)
	{
		this.setPointRDD(pointRDD);
	}
	public PointRDD(JavaSparkContext spark, String InputLocation)
	{
		this.setPointRDD(spark.textFile(InputLocation).map(new Function<String,Point>()
				{
				
				public Point call(String s)
				{	
					GeometryFactory fact = new GeometryFactory();
					List<String> input=Arrays.asList(s.split(","));
					Coordinate coordinate = new Coordinate(Double.parseDouble(input.get(0)),Double.parseDouble(input.get(1)));
					Point point=fact.createPoint(coordinate);
					return point;
				}
				}));
	}
	public JavaRDD<Point> getPointRDD() {
		return pointRDD;
	}
	public void setPointRDD(JavaRDD<Point> pointRDD) {
		this.pointRDD = pointRDD;
	}
	public void rePartition(Integer partitions)
	{
		this.pointRDD=this.pointRDD.repartition(partitions);
	}
	public PointRDD SpatialRangeQuery(Envelope envelope,Integer condition)
	{
		JavaRDD<Point> result=this.pointRDD.filter(new PointRangeFilter(envelope,condition));
		return new PointRDD(result);
	}
	public PointRDD SpatialRangeQuery(Polygon polygon,Integer condition)
	{
		JavaRDD<Point> result=this.pointRDD.filter(new PointRangeFilter(polygon,condition));
		return new PointRDD(result);
	}
	public JavaPairRDD<Envelope,String> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
		final Integer condition=Condition;
		//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
		Double minLongitude;
		Double minLatitude;
		Double maxLongitude;
		Double maxLatitude;
		Double minLongtitude1QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleXMinComparator()).getMinX();
		Double maxLongtitude1QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleXMinComparator()).getMinX();
		Double minLatitude1QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleYMinComparator()).getMinY();
		Double maxLatitude1QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleYMinComparator()).getMinY();
		Double minLongtitude2QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleXMaxComparator()).getMaxX();
		Double maxLongtitude2QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleXMaxComparator()).getMaxX();
		Double minLatitude2QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleYMaxComparator()).getMaxY();
		Double maxLatitude2QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleYMaxComparator()).getMaxY();
		Double minLongtitudeQueryAreaSet;
		Double maxLongtitudeQueryAreaSet;
		Double minLatitudeQueryAreaSet;
		Double maxLatitudeQueryAreaSet;
		//TargetSet min/max longitude and latitude
		minLongitude=this.pointRDD.min(new PointXComparator()).getX();
		maxLongitude=this.pointRDD.max(new PointXComparator()).getX();
		minLatitude=this.pointRDD.min(new PointYComparator()).getY();
		maxLatitude=this.pointRDD.max(new PointYComparator()).getY();
		//QueryAreaSet min/max longitude and latitude
		if(minLongtitude1QueryAreaSet<minLongtitude2QueryAreaSet)
		{
			minLongtitudeQueryAreaSet=minLongtitude1QueryAreaSet;
		}
		else
		{
			minLongtitudeQueryAreaSet=minLongtitude2QueryAreaSet;
		}
		if(maxLongtitude1QueryAreaSet>maxLongtitude2QueryAreaSet)
		{
			maxLongtitudeQueryAreaSet=maxLongtitude1QueryAreaSet;
		}
		else
		{
			maxLongtitudeQueryAreaSet=maxLongtitude2QueryAreaSet;
		}
		if(minLatitude1QueryAreaSet<minLatitude2QueryAreaSet)
		{
			minLatitudeQueryAreaSet=minLatitude1QueryAreaSet;
		}
		else
		{
			minLatitudeQueryAreaSet=minLatitude2QueryAreaSet;
		}
		if(maxLatitude1QueryAreaSet>maxLatitude2QueryAreaSet)
		{
			maxLatitudeQueryAreaSet=maxLatitude1QueryAreaSet;
		}
		else
		{
			maxLatitudeQueryAreaSet=maxLatitude2QueryAreaSet;
		}
		//Border found
		if(minLongitude>minLongtitudeQueryAreaSet)
		{
			minLongitude=minLongtitudeQueryAreaSet;
		}
		if(maxLongitude<maxLongtitudeQueryAreaSet)
		{
			maxLongitude=maxLongtitudeQueryAreaSet;
		}
		if(minLatitude>minLatitudeQueryAreaSet)
		{
			minLatitude=minLatitudeQueryAreaSet;
		}
		if(maxLatitude<maxLatitudeQueryAreaSet)
		{
			maxLatitude=maxLatitudeQueryAreaSet;
		}
//Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal+1];
		Double[] gridVerticalBorder=new Double[GridNumberVertical+1];
		double LongitudeIncrement=(maxLongitude-minLongitude)/GridNumberHorizontal;
		double LatitudeIncrement=(maxLatitude-minLatitude)/GridNumberVertical;
		System.out.println(maxLongitude);
		System.out.println(minLongitude);
		System.out.println(maxLatitude);
		System.out.println(minLatitude);
		for(int i=0;i<GridNumberHorizontal+1;i++)
		{
			gridHorizontalBorder[i]=minLongitude+LongitudeIncrement*i;
		}
		for(int i=0;i<GridNumberVertical+1;i++)
		{
			gridVerticalBorder[i]=minLatitude+LatitudeIncrement*i;
		}
		//Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer,Point> TargetSetWithID=this.pointRDD.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithID=rectangleRDD.getRectangleRDD().mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
//Join two dataset
		JavaPairRDD<Integer, Tuple2<Iterable<Envelope>, Iterable<Point>>> jointSet=QueryAreaSetWithID.cogroup(TargetSetWithID).repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
//Calculate the relation between one point and one query area
				JavaPairRDD<Envelope,Point> queryResult=jointSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Envelope>, Iterable<Point>>>, Envelope,Point>()
						{

					public Iterable<Tuple2<Envelope, Point>> call(
							Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Point>>> t)
							throws Exception {
						ArrayList<Tuple2<Envelope, Point>> QueryAreaAndTarget=new ArrayList();
						Iterator<Envelope> QueryAreaIterator=t._2()._1().iterator();
						
						while(QueryAreaIterator.hasNext())
						{
							Envelope currentQueryArea=QueryAreaIterator.next();
							Iterator<Point> TargetIterator=t._2()._2().iterator();
							while(TargetIterator.hasNext())
							{
								Point currentTarget=TargetIterator.next();
								if(condition==0){
								if(currentQueryArea.contains(currentTarget.getCoordinate()))
								{
									QueryAreaAndTarget.add(new Tuple2<Envelope,Point>(currentQueryArea,currentTarget));
								}
								}
								else
								{
									if(currentQueryArea.intersects(currentTarget.getCoordinate()))
									{
										QueryAreaAndTarget.add(new Tuple2<Envelope,Point>(currentQueryArea,currentTarget));
									}
								}
							}
						}
						
						return QueryAreaAndTarget;
					}
			
				});
		//Delete the duplicate result
				JavaPairRDD<Envelope, Iterable<Point>> aggregatedResult=queryResult.groupByKey();
				JavaPairRDD<Envelope,String> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Point>>,Envelope,String>()
						{

							public Tuple2<Envelope, String> call(Tuple2<Envelope, Iterable<Point>> t)
									{
								Integer commaFlag=0;
								Iterator<Point> valueIterator=t._2().iterator();
								String result="";
								while(valueIterator.hasNext())
								{
									Point currentTarget=valueIterator.next();
									String currentTargetString=""+currentTarget.getX()+","+currentTarget.getY();
									if(!result.contains(currentTargetString))
									{
										
										if(commaFlag==0)
										{
											result=result+currentTargetString;
											commaFlag=1;
										}
										else result=result+","+currentTargetString;
									}
								}
								
								return new Tuple2<Envelope, String>(t._1(),result);
							}
					
						});
//Persist the result on HDFS
		//refinedResult.repartition(1).saveAsTextFile(OutputLocation);
		return refinedResult;
	}

	public JavaPairRDD<Polygon,String> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
		final Integer condition=Condition;
		//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
		Double minLongitude;
		Double minLatitude;
		Double maxLongitude;
		Double maxLatitude;
		Double minLongtitude1QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonXMinComparator()).getEnvelopeInternal().getMinX();
		Double maxLongtitude1QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonXMinComparator()).getEnvelopeInternal().getMinX();
		Double minLatitude1QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonYMinComparator()).getEnvelopeInternal().getMinY();
		Double maxLatitude1QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonYMinComparator()).getEnvelopeInternal().getMinY();
		Double minLongtitude2QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonXMaxComparator()).getEnvelopeInternal().getMaxX();
		Double maxLongtitude2QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonXMaxComparator()).getEnvelopeInternal().getMaxX();
		Double minLatitude2QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonYMaxComparator()).getEnvelopeInternal().getMaxY();
		Double maxLatitude2QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonYMaxComparator()).getEnvelopeInternal().getMaxY();
		Double minLongtitudeQueryAreaSet;
		Double maxLongtitudeQueryAreaSet;
		Double minLatitudeQueryAreaSet;
		Double maxLatitudeQueryAreaSet;
		//TargetSet min/max longitude and latitude
		minLongitude=this.pointRDD.min(new PointXComparator()).getX();
		maxLongitude=this.pointRDD.max(new PointXComparator()).getX();
		minLatitude=this.pointRDD.min(new PointYComparator()).getY();
		maxLatitude=this.pointRDD.max(new PointYComparator()).getY();
		//QueryAreaSet min/max longitude and latitude
		if(minLongtitude1QueryAreaSet<minLongtitude2QueryAreaSet)
		{
			minLongtitudeQueryAreaSet=minLongtitude1QueryAreaSet;
		}
		else
		{
			minLongtitudeQueryAreaSet=minLongtitude2QueryAreaSet;
		}
		if(maxLongtitude1QueryAreaSet>maxLongtitude2QueryAreaSet)
		{
			maxLongtitudeQueryAreaSet=maxLongtitude1QueryAreaSet;
		}
		else
		{
			maxLongtitudeQueryAreaSet=maxLongtitude2QueryAreaSet;
		}
		if(minLatitude1QueryAreaSet<minLatitude2QueryAreaSet)
		{
			minLatitudeQueryAreaSet=minLatitude1QueryAreaSet;
		}
		else
		{
			minLatitudeQueryAreaSet=minLatitude2QueryAreaSet;
		}
		if(maxLatitude1QueryAreaSet>maxLatitude2QueryAreaSet)
		{
			maxLatitudeQueryAreaSet=maxLatitude1QueryAreaSet;
		}
		else
		{
			maxLatitudeQueryAreaSet=maxLatitude2QueryAreaSet;
		}
		//Border found
		if(minLongitude>minLongtitudeQueryAreaSet)
		{
			minLongitude=minLongtitudeQueryAreaSet;
		}
		if(maxLongitude<maxLongtitudeQueryAreaSet)
		{
			maxLongitude=maxLongtitudeQueryAreaSet;
		}
		if(minLatitude>minLatitudeQueryAreaSet)
		{
			minLatitude=minLatitudeQueryAreaSet;
		}
		if(maxLatitude<maxLatitudeQueryAreaSet)
		{
			maxLatitude=maxLatitudeQueryAreaSet;
		}
//Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal+1];
		Double[] gridVerticalBorder=new Double[GridNumberVertical+1];
		double LongitudeIncrement=(maxLongitude-minLongitude)/GridNumberHorizontal;
		double LatitudeIncrement=(maxLatitude-minLatitude)/GridNumberVertical;
		System.out.println(maxLongitude);
		System.out.println(minLongitude);
		System.out.println(maxLatitude);
		System.out.println(minLatitude);
		for(int i=0;i<GridNumberHorizontal+1;i++)
		{
			gridHorizontalBorder[i]=minLongitude+LongitudeIncrement*i;
		}
		for(int i=0;i<GridNumberVertical+1;i++)
		{
			gridVerticalBorder[i]=minLatitude+LatitudeIncrement*i;
		}
		//Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer,Point> TargetSetWithID=this.pointRDD.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Polygon> QueryAreaSetWithID=polygonRDD.getPolygonRDD().mapPartitionsToPair(new PartitionAssignGridPolygon(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
//Join two dataset
		JavaPairRDD<Integer, Tuple2<Iterable<Polygon>, Iterable<Point>>> jointSet=QueryAreaSetWithID.cogroup(TargetSetWithID);
//Calculate the relation between one point and one query area
		JavaPairRDD<Polygon,Point> queryResult=jointSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Polygon>, Iterable<Point>>>, Polygon,Point>()
				{

			public Iterable<Tuple2<Polygon, Point>> call(
					Tuple2<Integer, Tuple2<Iterable<Polygon>, Iterable<Point>>> t)
					throws Exception {
				ArrayList<Tuple2<Polygon, Point>> QueryAreaAndTarget=new ArrayList();
				Iterator<Polygon> QueryAreaIterator=t._2()._1().iterator();
				
				while(QueryAreaIterator.hasNext())
				{
					Polygon currentQueryArea=QueryAreaIterator.next();
					Iterator<Point> TargetIterator=t._2()._2().iterator();
					while(TargetIterator.hasNext())
					{
						Point currentTarget=TargetIterator.next();
						if(condition==0){
						if(currentQueryArea.contains(currentTarget))
						{
							QueryAreaAndTarget.add(new Tuple2<Polygon,Point>(currentQueryArea,currentTarget));
						}
						}
						else
						{
							if(currentQueryArea.intersects(currentTarget))
							{
								QueryAreaAndTarget.add(new Tuple2<Polygon,Point>(currentQueryArea,currentTarget));
							}
						}
					}
				}
				
				return QueryAreaAndTarget;
			}
	
		});
//Delete the duplicate result
		JavaPairRDD<Polygon, Iterable<Point>> aggregatedResult=queryResult.groupByKey();
		JavaPairRDD<Polygon,String> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Point>>,Polygon,String>()
				{

					public Tuple2<Polygon, String> call(Tuple2<Polygon, Iterable<Point>> t)
							{
						Integer commaFlag=0;
						Iterator<Point> valueIterator=t._2().iterator();
						String result="";
						while(valueIterator.hasNext())
						{
							Point currentTarget=valueIterator.next();
							String currentTargetString=""+currentTarget.getX()+","+currentTarget.getY();
							if(!result.contains(currentTargetString))
							{
								if(commaFlag==0)
								{
									result=result+currentTargetString;
									commaFlag=1;
								}
								else result=result+","+currentTargetString;
							}
						}
						
						return new Tuple2<Polygon, String>(t._1(),result);
					}
			
				});
//Persist the result on HDFS
		//refinedResult.repartition(1).saveAsTextFile(OutputLocation);
		return refinedResult;
	}
	public JavaPairRDD<Polygon,String> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		final Integer condition=Condition;
		//Create mapping between polygons and their minimum bounding box
		JavaPairRDD<Envelope,Polygon> polygonRDDwithKey=polygonRDD.getPolygonRDD().mapToPair(new PairFunction<Polygon,Envelope,Polygon>(){
			
			public Tuple2<Envelope,Polygon> call(Polygon s)
			{
				Envelope MBR= s.getEnvelopeInternal();
				return new Tuple2<Envelope,Polygon>(MBR,s);
			}
		}).repartition(polygonRDD.getPolygonRDD().partitions().size()*2);

	//Filter phase
		RectangleRDD rectangleRDD=polygonRDD.MinimumBoundingRectangle();
		JavaPairRDD<Envelope,String> filterResult=this.SpatialJoinQuery(rectangleRDD, condition, GridNumberHorizontal, GridNumberVertical);
//Refine phase
		JavaPairRDD<Envelope, Tuple2<Iterable<Polygon>, Iterable<String>>> joinSet=polygonRDDwithKey.cogroup(filterResult).repartition((polygonRDDwithKey.partitions().size()+filterResult.partitions().size())*2);
		JavaPairRDD<Polygon,Point> RefineResult=joinSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Envelope,Tuple2<Iterable<Polygon>,Iterable<String>>>,Polygon,Point>(){
			public Iterable<Tuple2<Polygon, Point>> call(Tuple2<Envelope, Tuple2<Iterable<Polygon>, Iterable<String>>> t){
				ArrayList<Tuple2<Polygon, Point>> QueryAreaAndTarget=new ArrayList();
				Iterator<Polygon> QueryAreaIterator=t._2()._1().iterator();
				
				while(QueryAreaIterator.hasNext())
				{
					Polygon currentQueryArea=QueryAreaIterator.next();
					Iterator<String> TargetIteratorString=t._2()._2().iterator();
					while(TargetIteratorString.hasNext())
					{
						String currentTargetString=TargetIteratorString.next();
						ArrayList<Point> Target=new ArrayList<Point>();
						
						Iterator<String> stringIterator=Arrays.asList(currentTargetString.split(",")).iterator();
						
						while(stringIterator.hasNext())
						{
							GeometryFactory fact = new GeometryFactory();
							Coordinate coordinate = new Coordinate(Double.parseDouble(stringIterator.next()),Double.parseDouble(stringIterator.next()));
							Point point=fact.createPoint(coordinate);
							Target.add(point);
						}
						Iterator<Point> targetIterator=Target.iterator();
						while(targetIterator.hasNext()){
						Point currentTarget=targetIterator.next();
						
						if(condition==0){
						if(currentQueryArea.contains(currentTarget))
						{
							QueryAreaAndTarget.add(new Tuple2<Polygon,Point>(currentQueryArea,currentTarget));
						}
						}
						else
						{
							if(currentQueryArea.intersects(currentTarget))
							{
								QueryAreaAndTarget.add(new Tuple2<Polygon,Point>(currentQueryArea,currentTarget));
							}
						}
						}
					}
				}
				return QueryAreaAndTarget;
			}});

		//Delete the duplicate result
				JavaPairRDD<Polygon, Iterable<Point>> aggregatedResult=RefineResult.groupByKey();
				JavaPairRDD<Polygon,String> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Point>>,Polygon,String>()
						{

							public Tuple2<Polygon, String> call(Tuple2<Polygon, Iterable<Point>> t)
									{
								Integer commaFlag=0;
								Iterator<Point> valueIterator=t._2().iterator();
								String result="";
								while(valueIterator.hasNext())
								{
									Point currentTarget=valueIterator.next();
									String currentTargetString=""+currentTarget.getX()+","+currentTarget.getY();
									if(!result.contains(currentTargetString))
									{
										if(commaFlag==0)
										{
											result=result+currentTargetString;
											commaFlag=1;
										}
										else result=result+","+currentTargetString;
									}
								}
								
								return new Tuple2<Polygon, String>(t._1(),result);
							}
					
						});
		//Persist the result on HDFS
				//refinedResult.repartition(1).saveAsTextFile(OutputLocation);
				return refinedResult;
	}
}
