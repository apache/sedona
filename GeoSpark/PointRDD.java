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
import Functions.PartitionAssignGridCircle;
import Functions.PartitionAssignGridPoint;
import Functions.PartitionAssignGridPolygon;
import Functions.PartitionAssignGridRectangle;
import Functions.PointRangeFilter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
class PointFormatMapper implements Serializable,Function<String,Point>
{
	Integer offset=0;
	String splitter="csv";
	public PointFormatMapper(Integer Offset,String Splitter)
	{
		this.offset=Offset;
		this.splitter=Splitter;
	}
	public Point call(String s)
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
		GeometryFactory fact = new GeometryFactory();
		List<String> input=Arrays.asList(s.split(seperater));
		Coordinate coordinate = new Coordinate(Double.parseDouble(input.get(0+this.offset)),Double.parseDouble(input.get(1+this.offset)));
		Point point=fact.createPoint(coordinate);
		return point;
	}
}
 

public class PointRDD implements Serializable{
	private JavaRDD<Point> pointRDD;
	public PointRDD(JavaRDD<Point> pointRDD)
	{
		this.setPointRDD(pointRDD.cache());
	}
	public PointRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)
	{
		//final Integer offset=Offset;
		this.setPointRDD(spark.textFile(InputLocation,partitions).map(new PointFormatMapper(Offset,Splitter)).cache());
	}
	public PointRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)
	{
		//final Integer offset=Offset;
		this.setPointRDD(spark.textFile(InputLocation).map(new PointFormatMapper(Offset,Splitter)).cache());
	}
	public JavaRDD<Point> getPointRDD() {
		return pointRDD;
	}
	public void setPointRDD(JavaRDD<Point> pointRDD) {
		this.pointRDD = pointRDD;
	}
	public JavaRDD<Point> rePartition(Integer partitions)
	{
		return this.pointRDD.repartition(partitions);
	}
	public PointRDD SpatialRangeQuery(Envelope envelope,Integer condition)
	{
		JavaRDD<Point> result=this.pointRDD.filter(new PointRangeFilter(envelope,condition));
		return new PointRDD(result);
	}
	public Envelope boundary()
	{
		Double[] boundary=new Double[4];
		Double minLongitude=this.pointRDD.min(new PointXComparator()).getX();
		Double maxLongitude=this.pointRDD.max(new PointXComparator()).getX();
		Double minLatitude=this.pointRDD.min(new PointYComparator()).getY();
		Double maxLatitude=this.pointRDD.max(new PointYComparator()).getY();
		boundary[0]=minLongitude;
		boundary[1]=minLatitude;
		boundary[2]=maxLongitude;
		boundary[3]=maxLatitude;
		return new Envelope(minLongitude,maxLongitude,minLatitude,maxLatitude);
	}
	public PointRDD SpatialRangeQuery(Polygon polygon,Integer condition)
	{
		JavaRDD<Point> result=this.pointRDD.filter(new PointRangeFilter(polygon,condition));
		return new PointRDD(result);
	}
	public SpatialPairRDD<Point,ArrayList<Point>> SpatialJoinQuery(CircleRDD circleRDD,Double Radius,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
		//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
		//QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary=circleRDD.boundary();
		//TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary=this.boundary();
		Envelope boundary;
		//Border found
		JavaRDD<Point> TargetPreFiltered;
		JavaRDD<Circle> QueryAreaPreFiltered;
		//Integer currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
		//Integer currentPartitionQuerySet=circleRDD.getCircleRDD().partitions().size();
		if(QueryWindowSetBoundary.contains(TargetSetBoundary))
		{
			boundary=TargetSetBoundary;
			//TargetPreFiltered=this.pointRDD;
			//QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new CirclePreFilter(boundary));
		}
		else if(TargetSetBoundary.contains(QueryWindowSetBoundary))
		{
			boundary=QueryWindowSetBoundary;
			//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			//QueryAreaPreFiltered=circleRDD.getCircleRDD();
		}
		else if(QueryWindowSetBoundary.intersects(TargetSetBoundary))
		{
			boundary=QueryWindowSetBoundary.intersection(TargetSetBoundary);
			//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			//QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new CirclePreFilter(boundary));
		}
		else
		{
			System.out.println("Two input sets are not overlapped");
			return null;
		}

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
		
		JavaPairRDD<Integer,Point> TargetSetWithIDtemp=this.pointRDD.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Circle> QueryAreaSetWithID1=circleRDD.getCircleRDD().mapPartitionsToPair(new PartitionAssignGridCircle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		this.pointRDD.unpersist();
		circleRDD.getCircleRDD().unpersist();
		JavaPairRDD<Integer,Point> QueryAreaSetWithIDtemp=QueryAreaSetWithID1.mapToPair(new PairFunction<Tuple2<Integer,Circle>,Integer,Point>()
				{

					public Tuple2<Integer, Point> call(Tuple2<Integer, Circle> t){
						
						return new Tuple2<Integer,Point>(t._1(),t._2().getCentre());
					}
			
				});
				JavaPairRDD<Integer,Point> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Point> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
		JavaPairRDD<Integer, Tuple2<Point, Point>> jointSet=QueryAreaSetWithID.join(TargetSetWithID,TargetSetWithIDtemp.partitions().size()*2);//.repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
//Calculate the relation between one point and one query area
		JavaPairRDD<Point,Point> jointSet1=jointSet.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Point, Point>>,Point,Point>()
				{

					public Tuple2<Point, Point> call(
							Tuple2<Integer, Tuple2<Point, Point>> t){
						return t._2();
							
					}
			
				});
				JavaPairRDD<Point,Point> queryResult=jointSet1.filter(new CircleFilterPoint(Radius,Condition));
//Delete the duplicate result
				JavaPairRDD<Point, Iterable<Point>> aggregatedResult=queryResult.groupByKey();
				JavaPairRDD<Point,ArrayList<Point>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Point,Iterable<Point>>,Point,ArrayList<Point>>()
						{

							public Tuple2<Point, ArrayList<Point>> call(Tuple2<Point, Iterable<Point>> v){
								ArrayList<Point> list=new ArrayList<Point>();
								ArrayList<Point> result=new ArrayList<Point>();
								Iterator<Point> targetIterator=v._2().iterator();
								while(targetIterator.hasNext())
								{
									list.add(targetIterator.next());
								}
								
								for(int i=0;i<list.size();i++)
								{
									Integer duplicationFlag=0;
									Point currentPointi=list.get(i);
									for(int j=i+1;j<list.size();j++)
									{
										Point currentPointj=list.get(j);
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
								return new Tuple2<Point,ArrayList<Point>>(v._1(),result);
							}
					
						});
				SpatialPairRDD<Point,ArrayList<Point>> result=new SpatialPairRDD<Point,ArrayList<Point>>(refinedResult);
		return result;
	}
	public SpatialPairRDD<Envelope,ArrayList<Point>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
		final Integer condition=Condition;
		//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
		//QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary=rectangleRDD.boundary();
		//TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary=this.boundary();
		Envelope boundary=QueryWindowSetBoundary;
		//Integer currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
		//Integer currentPartitionQuerySet=rectangleRDD.getRectangleRDD().partitions().size();
		//Border found
		JavaRDD<Point> TargetPreFiltered;//=this.pointRDD.filter(new PointPreFilter(boundary));
		JavaRDD<Envelope> QueryAreaPreFiltered;//=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
		if(QueryWindowSetBoundary.contains(TargetSetBoundary))
		{
			boundary=TargetSetBoundary;
			//TargetPreFiltered=this.pointRDD;
			//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
		}
		else if(TargetSetBoundary.contains(QueryWindowSetBoundary))
		{
			boundary=QueryWindowSetBoundary;
			//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD();
		}
		else if(QueryWindowSetBoundary.intersects(TargetSetBoundary))
		{
			boundary=QueryWindowSetBoundary.intersection(TargetSetBoundary);
			//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
		}
		else
		{
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		//JavaRDD<Point> TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));
		//JavaRDD<Envelope> QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
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
		JavaPairRDD<Integer,Point> TargetSetWithIDtemp=this.pointRDD.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithIDtemp=rectangleRDD.getRectangleRDD().mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		//Remove cache from memory
		this.pointRDD.unpersist();
		rectangleRDD.getRectangleRDD().unpersist();
		JavaPairRDD<Integer,Point> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
		JavaPairRDD<Integer,Tuple2<Envelope,Point>> joinSet1=QueryAreaSetWithID.join(TargetSetWithID, TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Envelope,Point> joinSet=joinSet1.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Envelope,Point>>,Envelope,Point>()
				{

					public Tuple2<Envelope, Point> call(
							Tuple2<Integer, Tuple2<Envelope, Point>> t)
							throws Exception {
						return t._2();
					}
			
				});
		JavaPairRDD<Envelope,Point> queryResult=joinSet.filter(new Function<Tuple2<Envelope,Point>,Boolean>(){

			public Boolean call(Tuple2<Envelope, Point> v1) throws Exception {
				// TODO Auto-generated method stub
				if(condition==0){
					if(v1._1().intersects(v1._2().getCoordinate()))
					{
						return true;
					}
					else return false;
					}
				else
				{
						if(v1._1().contains(v1._2().getCoordinate()))
						{
							return true;
						}
						else return false;
				}
			}});
		//Delete the duplicate result
				JavaPairRDD<Envelope, Iterable<Point>> aggregatedResult=queryResult.groupByKey();
				JavaPairRDD<Envelope,ArrayList<Point>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Point>>,Envelope,ArrayList<Point>>()
						{

							public Tuple2<Envelope, ArrayList<Point>> call(Tuple2<Envelope, Iterable<Point>> v){
								ArrayList<Point> list=new ArrayList<Point>();
								ArrayList<Point> result=new ArrayList<Point>();
								Iterator<Point> targetIterator=v._2().iterator();
								while(targetIterator.hasNext())
								{
									list.add(targetIterator.next());
								}
								
								for(int i=0;i<list.size();i++)
								{
									Integer duplicationFlag=0;
									Point currentPointi=list.get(i);
									for(int j=i+1;j<list.size();j++)
									{
										Point currentPointj=list.get(j);
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
								return new Tuple2<Envelope,ArrayList<Point>>(v._1(),result);
							}
					
						});
				SpatialPairRDD<Envelope,ArrayList<Point>> result=new SpatialPairRDD<Envelope,ArrayList<Point>>(refinedResult);
		return result;

	}


	
	
	
	public SpatialPairRDD<Envelope,ArrayList<Point>> SpatialJoinQueryWithIndex(RectangleRDD rectangleRDD,Integer GridNumberHorizontal,Integer GridNumberVertical,String Index)
	{
		//Find the border of both of the two datasets---------------
		//final Integer condition=Condition;
		final String index=Index;
		//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
		//QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary=rectangleRDD.boundary();
		//TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary=this.boundary();
		Envelope boundary=QueryWindowSetBoundary;
		//Integer currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
		//Integer currentPartitionQuerySet=rectangleRDD.getRectangleRDD().partitions().size();
		//Border found
		JavaRDD<Point> TargetPreFiltered;//=this.pointRDD.filter(new PointPreFilter(boundary));
		JavaRDD<Envelope> QueryAreaPreFiltered;//=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
		if(QueryWindowSetBoundary.contains(TargetSetBoundary))
		{
			boundary=TargetSetBoundary;
			//TargetPreFiltered=this.pointRDD;
			//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
		}
		else if(TargetSetBoundary.contains(QueryWindowSetBoundary))
		{
			boundary=QueryWindowSetBoundary;
			//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD();
		}
		else if(QueryWindowSetBoundary.intersects(TargetSetBoundary))
		{
			boundary=QueryWindowSetBoundary.intersection(TargetSetBoundary);
			//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
		}
		else
		{
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		//JavaRDD<Point> TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));
		//JavaRDD<Envelope> QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
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
		JavaPairRDD<Integer,Point> TargetSetWithIDtemp=this.pointRDD.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithIDtemp=rectangleRDD.getRectangleRDD().mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		//Remove cache from memory
		this.pointRDD.unpersist();
		rectangleRDD.getRectangleRDD().unpersist();
		JavaPairRDD<Integer,Point> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
		//JavaPairRDD<Integer,Tuple2<Envelope,Point>> joinSet1=QueryAreaSetWithID.join(TargetSetWithID, TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Tuple2<Iterable<Envelope>,Iterable<Point>>> cogroupSet=QueryAreaSetWithID.cogroup(TargetSetWithID, TargetSetWithIDtemp.partitions().size()*2);
		
		JavaPairRDD<Envelope,ArrayList<Point>> queryResult=cogroupSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Envelope>,Iterable<Point>>>,Envelope,ArrayList<Point>>()
				{

					public Iterable<Tuple2<Envelope, ArrayList<Point>>> call(
							Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Point>>> t)
					{
						
							if(index=="quadtree")
							{
								Quadtree qt=new Quadtree();
								Iterator<Point> targetIterator=t._2()._2().iterator();
								Iterator<Envelope> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Envelope,ArrayList<Point>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Point currentTarget=targetIterator.next();
									qt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Envelope currentQueryArea=queryAreaIterator.next();
									List<Point> queryList=qt.query(currentQueryArea);
									if(queryList.size()!=0){
									result.add(new Tuple2<Envelope,ArrayList<Point>>(currentQueryArea,new ArrayList<Point>(queryList)));
									}
								}
								return result;
							}
							else
							{
								STRtree rt=new STRtree();
								Iterator<Point> targetIterator=t._2()._2().iterator();
								Iterator<Envelope> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Envelope,ArrayList<Point>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Point currentTarget=targetIterator.next();
									rt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Envelope currentQueryArea=queryAreaIterator.next();
									List<Point> queryList=rt.query(currentQueryArea);
									if(queryList.size()!=0){
									result.add(new Tuple2<Envelope,ArrayList<Point>>(currentQueryArea,new ArrayList<Point>(queryList)));
									}
								}
								return result;
							}
						
					}
			
				});
		//Delete the duplicate result
				JavaPairRDD<Envelope, ArrayList<Point>> aggregatedResult=queryResult.reduceByKey(new Function2<ArrayList<Point>,ArrayList<Point>,ArrayList<Point>>()
						{

							public ArrayList<Point> call(ArrayList<Point> v1,
									ArrayList<Point> v2) {
								ArrayList<Point> v3=v1;
								v3.addAll(v2);
								return v2;
							}
					
						});
				JavaPairRDD<Envelope,ArrayList<Point>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Envelope,ArrayList<Point>>,Envelope,ArrayList<Point>>()
						{

							public Tuple2<Envelope, ArrayList<Point>> call(Tuple2<Envelope, ArrayList<Point>> v){
								ArrayList<Point> result=new ArrayList<Point>();
								Iterator<Point> targetIterator=v._2().iterator();
								for(int i=0;i<v._2().size();i++)
								{
									Integer duplicationFlag=0;
									Point currentPointi=v._2().get(i);
									for(int j=i+1;j<v._2().size();j++)
									{
										Point currentPointj=v._2().get(j);
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
								return new Tuple2<Envelope,ArrayList<Point>>(v._1(),result);
							}
					
						});
				SpatialPairRDD<Envelope,ArrayList<Point>> result=new SpatialPairRDD<Envelope,ArrayList<Point>>(refinedResult);
		return result;
	}
	
	
	
	
	public SpatialPairRDD<Point,ArrayList<Point>> SpatialJoinQueryWithIndex(CircleRDD circleRDD,Integer GridNumberHorizontal,Integer GridNumberVertical,String Index)
	{
		//Find the border of both of the two datasets---------------
				//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
				//QueryAreaSet min/max longitude and latitude
				final String index=Index;
				Envelope QueryWindowSetBoundary=circleRDD.boundary();
				//TargetSet min/max longitude and latitude
				Envelope TargetSetBoundary=this.boundary();
				Envelope boundary;
				//Border found
				JavaRDD<Point> TargetPreFiltered;
				JavaRDD<Circle> QueryAreaPreFiltered;
				//Integer currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
				//Integer currentPartitionQuerySet=circleRDD.getCircleRDD().partitions().size();
				if(QueryWindowSetBoundary.contains(TargetSetBoundary))
				{
					boundary=TargetSetBoundary;
					//TargetPreFiltered=this.pointRDD;
					//QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new CirclePreFilter(boundary));
				}
				else if(TargetSetBoundary.contains(QueryWindowSetBoundary))
				{
					boundary=QueryWindowSetBoundary;
					//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
					//QueryAreaPreFiltered=circleRDD.getCircleRDD();
				}
				else if(QueryWindowSetBoundary.intersects(TargetSetBoundary))
				{
					boundary=QueryWindowSetBoundary.intersection(TargetSetBoundary);
					//TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
					//QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new CirclePreFilter(boundary));
				}
				else
				{
					System.out.println("Two input sets are not overlapped");
					return null;
				}

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
				
				JavaPairRDD<Integer,Point> TargetSetWithIDtemp=this.pointRDD.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
				JavaPairRDD<Integer,Circle> QueryAreaSetWithIDtemp=circleRDD.getCircleRDD().mapPartitionsToPair(new PartitionAssignGridCircle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
				this.pointRDD.unpersist();
				circleRDD.getCircleRDD().unpersist();
				JavaPairRDD<Integer,Point> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
				JavaPairRDD<Integer,Circle> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
				JavaPairRDD<Integer,Tuple2<Iterable<Circle>,Iterable<Point>>> cogroupSet=QueryAreaSetWithID.cogroup(TargetSetWithID, TargetSetWithIDtemp.partitions().size()*2);
		
		JavaPairRDD<Circle,ArrayList<Point>> queryResult=cogroupSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Circle>,Iterable<Point>>>,Circle,ArrayList<Point>>()
				{

					public Iterable<Tuple2<Circle, ArrayList<Point>>> call(
							Tuple2<Integer, Tuple2<Iterable<Circle>, Iterable<Point>>> t)
					{
						
							if(index=="quadtree")
							{
								Quadtree qt=new Quadtree();
								Iterator<Point> targetIterator=t._2()._2().iterator();
								Iterator<Circle> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Circle,ArrayList<Point>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Point currentTarget=targetIterator.next();
									qt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Circle currentQueryArea=queryAreaIterator.next();
									List<Point> queryList=qt.query(currentQueryArea.getMBR());
									if(queryList.size()!=0){
									result.add(new Tuple2<Circle,ArrayList<Point>>(currentQueryArea,new ArrayList<Point>(queryList)));
									}
								}
								return result;
							}
							else
							{
								STRtree rt=new STRtree();
								Iterator<Point> targetIterator=t._2()._2().iterator();
								Iterator<Circle> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Circle,ArrayList<Point>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Point currentTarget=targetIterator.next();
									rt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Circle currentQueryArea=queryAreaIterator.next();
									List<Point> queryList=rt.query(currentQueryArea.getMBR());
									if(queryList.size()!=0){
									result.add(new Tuple2<Circle,ArrayList<Point>>(currentQueryArea,new ArrayList<Point>(queryList)));
									}
								}
								return result;
							}
						
					}
			
				});
		//Delete the duplicate result
				JavaPairRDD<Circle, ArrayList<Point>> aggregatedResult=queryResult.reduceByKey(new Function2<ArrayList<Point>,ArrayList<Point>,ArrayList<Point>>()
						{

							public ArrayList<Point> call(ArrayList<Point> v1,
									ArrayList<Point> v2) {
								ArrayList<Point> v3=v1;
								v3.addAll(v2);
								return v2;
							}
					
						});
				JavaPairRDD<Point,ArrayList<Point>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Circle,ArrayList<Point>>,Point,ArrayList<Point>>()
						{

							public Tuple2<Point, ArrayList<Point>> call(Tuple2<Circle, ArrayList<Point>> v){
								ArrayList<Point> result=new ArrayList<Point>();
								Iterator<Point> targetIterator=v._2().iterator();
								for(int i=0;i<v._2().size();i++)
								{
									Integer duplicationFlag=0;
									Point currentPointi=v._2().get(i);
									for(int j=i+1;j<v._2().size();j++)
									{
										Point currentPointj=v._2().get(j);
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
								return new Tuple2<Point,ArrayList<Point>>(v._1().getCentre(),result);
							}
					
						});
				SpatialPairRDD<Point,ArrayList<Point>> result=new SpatialPairRDD<Point,ArrayList<Point>>(refinedResult);
		return result;
	}
	
	public SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
		final Integer condition=Condition;
		//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
		//QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary=polygonRDD.boundary();
		//TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary=this.boundary();
		Envelope boundary=QueryWindowSetBoundary;
		
		//Border found
		
		if(QueryWindowSetBoundary.contains(TargetSetBoundary))
		{
			boundary=TargetSetBoundary;
		}
		else if(TargetSetBoundary.contains(QueryWindowSetBoundary))
		{
			boundary=QueryWindowSetBoundary;
		}
		else if(QueryWindowSetBoundary.intersects(TargetSetBoundary))
		{
			boundary=QueryWindowSetBoundary.intersection(TargetSetBoundary);
		}
		else
		{
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		JavaRDD<Point> TargetPreFiltered=this.pointRDD.filter(new PointPreFilter(boundary));
		JavaRDD<Polygon> QueryAreaPreFiltered=polygonRDD.getPolygonRDD().filter(new PolygonPreFilter(boundary));
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
		JavaPairRDD<Integer,Point> TargetSetWithIDtemp=TargetPreFiltered.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Polygon> QueryAreaSetWithIDtemp=QueryAreaPreFiltered.mapPartitionsToPair(new PartitionAssignGridPolygon(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		//Remove cache from memory
		this.pointRDD.unpersist();
		polygonRDD.getPolygonRDD().unpersist();
		JavaPairRDD<Integer,Point> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Polygon> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
		JavaPairRDD<Integer,Tuple2<Polygon,Point>> joinSet1=QueryAreaSetWithID.join(TargetSetWithID, TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Polygon,Point> joinSet=joinSet1.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Polygon,Point>>,Polygon,Point>()
				{

					public Tuple2<Polygon, Point> call(
							Tuple2<Integer, Tuple2<Polygon, Point>> t)
							throws Exception {
						return t._2();
					}
			
				});
//Delete the duplicate result
		JavaPairRDD<Polygon, Iterable<Point>> aggregatedResult=joinSet.groupByKey();
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
		SpatialPairRDD<Polygon,ArrayList<Point>> result=new SpatialPairRDD<Polygon,ArrayList<Point>>(refinedResult.mapToPair(new PairFunction<Tuple2<Polygon,String>,Polygon,ArrayList<Point>>()
				{

			public Tuple2<Polygon, ArrayList<Point>> call(Tuple2<Polygon, String> t)
			{
				List<String> resultListString= Arrays.asList(t._2().split(","));
				Iterator<String> targetIterator=resultListString.iterator();
				ArrayList<Point> resultList=new ArrayList<Point>();
				while(targetIterator.hasNext())
				{
					GeometryFactory fact = new GeometryFactory();
					Coordinate coordinate=new Coordinate(Double.parseDouble(targetIterator.next()),Double.parseDouble(targetIterator.next()));
					Point currentTarget=fact.createPoint(coordinate);
					resultList.add(currentTarget);
				}
				return new Tuple2<Polygon,ArrayList<Point>>(t._1(),resultList);
			}
			
		}));
		return result;
	}
	
	public SpatialPairRDD<Polygon,ArrayList<Point>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
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
		SpatialPairRDD<Envelope,ArrayList<Point>> filterResultPairRDD=this.SpatialJoinQuery(rectangleRDD, condition, GridNumberHorizontal, GridNumberVertical);
		JavaPairRDD<Envelope,ArrayList<Point>> filterResult=filterResultPairRDD.getSpatialPairRDD();
	//Refine phase
		JavaPairRDD<Envelope, Tuple2<Iterable<Polygon>, Iterable<ArrayList<Point>>>> joinSet=polygonRDDwithKey.cogroup(filterResult).repartition((polygonRDDwithKey.partitions().size()+filterResult.partitions().size())*2);
		JavaPairRDD<Polygon,Point> RefineResult=joinSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Envelope,Tuple2<Iterable<Polygon>,Iterable<ArrayList<Point>>>>,Polygon,Point>(){
			public Iterable<Tuple2<Polygon, Point>> call(Tuple2<Envelope, Tuple2<Iterable<Polygon>, Iterable<ArrayList<Point>>>> t){
				ArrayList<Tuple2<Polygon, Point>> QueryAreaAndTarget=new ArrayList<Tuple2<Polygon, Point>>();
				Iterator<Polygon> QueryAreaIterator=t._2()._1().iterator();
				
				while(QueryAreaIterator.hasNext())
				{
					Polygon currentQueryArea=QueryAreaIterator.next();
					Iterator<ArrayList<Point>> TargetIteratorOutLoop=t._2()._2().iterator();
					while(TargetIteratorOutLoop.hasNext())
					{
						ArrayList<Point> currentTargetOutLoop=TargetIteratorOutLoop.next();
						
						Iterator<Point> targetOutLoopIterator=currentTargetOutLoop.iterator();
						while(targetOutLoopIterator.hasNext()){
						Point currentTarget=targetOutLoopIterator.next();
						
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
				SpatialPairRDD<Polygon,ArrayList<Point>> result = new SpatialPairRDD<Polygon,ArrayList<Point>>(refinedResult.mapToPair(new PairFunction<Tuple2<Polygon,String>,Polygon,ArrayList<Point>>()
				{

					public Tuple2<Polygon, ArrayList<Point>> call(Tuple2<Polygon, String> t)
					{
						List<String> resultListString= Arrays.asList(t._2().split(","));
						Iterator<String> targetIterator=resultListString.iterator();
						ArrayList<Point> resultList=new ArrayList<Point>();
						while(targetIterator.hasNext())
						{
							GeometryFactory fact = new GeometryFactory();
							Coordinate coordinate=new Coordinate(Double.parseDouble(targetIterator.next()),Double.parseDouble(targetIterator.next()));
							Point currentTarget=fact.createPoint(coordinate);
							resultList.add(currentTarget);
						}
						return new Tuple2<Polygon,ArrayList<Point>>(t._1(),resultList);
					}
					
				}));
				return result;
	}
}
