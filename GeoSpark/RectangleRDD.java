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
import Functions.GridFileMaker;
import Functions.PartitionAssignGridPoint;
import Functions.PartitionAssignGridRectangle;
import Functions.RectangleRangeFilter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
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
public class RectangleRDD implements Serializable {

	private JavaRDD<Envelope> rectangleRDD;
	
	public RectangleRDD(JavaRDD<Envelope> rectangleRDD)
	{
		this.setRectangleRDD(rectangleRDD.cache());
	}
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)
	{
		//final Integer offset=Offset;
		this.setRectangleRDD(spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,Splitter)).cache());
	}
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)
	{
		//final Integer offset=Offset;
		this.setRectangleRDD(spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,Splitter)).cache());
	}
	public JavaRDD<Envelope> getRectangleRDD() {
		return rectangleRDD;
	}
	public void setRectangleRDD(JavaRDD<Envelope> rectangleRDD) {
		this.rectangleRDD = rectangleRDD;
	}
	public JavaRDD<Envelope> rePartition(Integer partitions)
	{
		return this.rectangleRDD.repartition(partitions);
	}
	public RectangleRDD SpatialRangeQuery(Envelope envelope,Integer condition)
	{
		JavaRDD<Envelope> result=this.rectangleRDD.filter(new RectangleRangeFilter(envelope,condition));
		return new RectangleRDD(result);
	}
	public RectangleRDD SpatialRangeQuery(Polygon polygon,Integer condition)
	{
		JavaRDD<Envelope> result=this.rectangleRDD.filter(new RectangleRangeFilter(polygon,condition));
		return new RectangleRDD(result);
	}
	public Envelope boundary()
	{
		Double[] boundary = new Double[4];
		Double minLongtitude1=this.rectangleRDD.min(new RectangleXMinComparator()).getMinX();
		Double maxLongtitude1=this.rectangleRDD.max(new RectangleXMinComparator()).getMinX();
		Double minLatitude1=this.rectangleRDD.min(new RectangleYMinComparator()).getMinY();
		Double maxLatitude1=this.rectangleRDD.max(new RectangleYMinComparator()).getMinY();
		Double minLongtitude2=this.rectangleRDD.min(new RectangleXMaxComparator()).getMaxX();
		Double maxLongtitude2=this.rectangleRDD.max(new RectangleXMaxComparator()).getMaxX();
		Double minLatitude2=this.rectangleRDD.min(new RectangleYMaxComparator()).getMaxY();
		Double maxLatitude2=this.rectangleRDD.max(new RectangleYMaxComparator()).getMaxY();
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
	public SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
		final Integer condition=Condition;
		//Find the border of both of the two datasets---------------
				//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
				//QueryAreaSet min/max longitude and latitude
				Envelope QueryWindowSetBoundary=rectangleRDD.boundary();
				//TargetSet min/max longitude and latitude
				Envelope TargetSetBoundary=this.boundary();
				Envelope boundary;
				//Border found
				JavaRDD<Envelope> TargetPreFiltered;
				JavaRDD<Envelope> QueryAreaPreFiltered;
				if(QueryWindowSetBoundary.contains(TargetSetBoundary))
				{
					boundary=TargetSetBoundary;
					//TargetPreFiltered=this.rectangleRDD;
					//QueryAreaPreFiltered=rectangleRDD.rectangleRDD.filter(new RectanglePreFilter(boundary));
				}
				else if(TargetSetBoundary.contains(QueryWindowSetBoundary))
				{
					boundary=QueryWindowSetBoundary;
					//TargetPreFiltered=this.rectangleRDD.filter(new RectanglePreFilter(boundary));
					//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD();
				}
				else if(QueryWindowSetBoundary.intersects(TargetSetBoundary))
				{
					boundary=QueryWindowSetBoundary.intersection(TargetSetBoundary);
					//TargetPreFiltered=this.rectangleRDD.filter(new RectanglePreFilter(boundary));
					//QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new RectanglePreFilter(boundary));
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
		JavaPairRDD<Integer,Envelope> TargetSetWithIDtemp=this.rectangleRDD.mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithIDtemp=rectangleRDD.getRectangleRDD().mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		//Remove cache from memory
		this.rectangleRDD.unpersist();
		rectangleRDD.getRectangleRDD().unpersist();
		JavaPairRDD<Integer,Envelope> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
		JavaPairRDD<Integer, Tuple2<Envelope,Envelope>> jointSet1=QueryAreaSetWithID.join(TargetSetWithID,TargetSetWithIDtemp.partitions().size()*2);//.repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
//Calculate the relation between one point and one query area
		JavaPairRDD<Envelope,Envelope> jointSet=jointSet1.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Envelope,Envelope>>,Envelope,Envelope>()
				{

					public Tuple2<Envelope, Envelope> call(
							Tuple2<Integer, Tuple2<Envelope, Envelope>> t){
						return t._2();
					}
			
				});
		JavaPairRDD<Envelope,Envelope> queryResult=jointSet.filter(new Function<Tuple2<Envelope,Envelope>,Boolean>()
				{

					public Boolean call(Tuple2<Envelope, Envelope> v1){
						if(condition==0){
							if(v1._1().contains(v1._2()))
							{
								return true;
							}
							else return false;
							}
							else
							{
								if(v1._1().contains(v1._2())||v1._1().intersects(v1._2()))
								{
									return true;
								}
								else return false;
							}
					}
					
				});
//Delete the duplicate result
		JavaPairRDD<Envelope, Iterable<Envelope>> aggregatedResult=queryResult.groupByKey();
		JavaPairRDD<Envelope,ArrayList<Envelope>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Envelope>>,Envelope,ArrayList<Envelope>>()
				{

					public Tuple2<Envelope, ArrayList<Envelope>> call(Tuple2<Envelope, Iterable<Envelope>> v){
						ArrayList<Envelope> list=new ArrayList<Envelope>();
						ArrayList<Envelope> result=new ArrayList<Envelope>();
						Iterator<Envelope> targetIterator=v._2().iterator();
						while(targetIterator.hasNext())
						{
							list.add(targetIterator.next());
						}
						
						for(int i=0;i<list.size();i++)
						{
							Integer duplicationFlag=0;
							Envelope currentTargeti=list.get(i);
							for(int j=i+1;j<list.size();j++)
							{
								Envelope currentTargetj=list.get(j);
								if(currentTargeti.equals(currentTargetj))
								{
									duplicationFlag=1;
								}
							}
							if(duplicationFlag==0)
							{
								result.add(currentTargeti);
							}
						}
						return new Tuple2<Envelope,ArrayList<Envelope>>(v._1(),result);
					}
			
				});
		SpatialPairRDD<Envelope,ArrayList<Envelope>> result=new SpatialPairRDD<Envelope,ArrayList<Envelope>>(refinedResult);
return result;
	}
	public SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialJoinQuery(Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
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
		JavaPairRDD<Integer, Tuple2<Envelope,Envelope>> jointSet1=QueryAreaSetWithID.join(TargetSetWithID,TargetSetWithIDtemp.partitions().size()*2);//.repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
//Calculate the relation between one point and one query area
		JavaPairRDD<Envelope,Envelope> jointSet=jointSet1.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Envelope,Envelope>>,Envelope,Envelope>()
				{

					public Tuple2<Envelope, Envelope> call(
							Tuple2<Integer, Tuple2<Envelope, Envelope>> t){
						return t._2();
					}
			
				});
		JavaPairRDD<Envelope,Envelope> queryResult=jointSet.filter(new Function<Tuple2<Envelope,Envelope>,Boolean>()
				{

					public Boolean call(Tuple2<Envelope, Envelope> v1){
						if(condition==0){
							if(v1._1().contains(v1._2()))
							{
								return true;
							}
							else return false;
							}
							else
							{
								if(v1._1().contains(v1._2())||v1._1().intersects(v1._2()))
								{
									return true;
								}
								else return false;
							}
					}
					
				});
//Delete the duplicate result
		JavaPairRDD<Envelope, Iterable<Envelope>> aggregatedResult=queryResult.groupByKey();
		JavaPairRDD<Envelope,ArrayList<Envelope>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Envelope>>,Envelope,ArrayList<Envelope>>()
				{

					public Tuple2<Envelope, ArrayList<Envelope>> call(Tuple2<Envelope, Iterable<Envelope>> v){
						ArrayList<Envelope> list=new ArrayList<Envelope>();
						ArrayList<Envelope> result=new ArrayList<Envelope>();
						Iterator<Envelope> targetIterator=v._2().iterator();
						while(targetIterator.hasNext())
						{
							list.add(targetIterator.next());
						}
						
						for(int i=0;i<list.size();i++)
						{
							Integer duplicationFlag=0;
							Envelope currentPointi=list.get(i);
							for(int j=i+1;j<list.size();j++)
							{
								Envelope currentPointj=list.get(j);
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
	
	
	
	
	public SpatialPairRDD<Polygon,ArrayList<Envelope>> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
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
		SpatialPairRDD<Envelope,ArrayList<Envelope>> filterResultPairRDD=this.SpatialJoinQuery(rectangleRDD, Condition, GridNumberHorizontal, GridNumberVertical);
		JavaPairRDD<Envelope,ArrayList<Envelope>> filterResult=filterResultPairRDD.getSpatialPairRDD();
	//Refine phase
		JavaPairRDD<Envelope, Tuple2<Iterable<Polygon>, Iterable<ArrayList<Envelope>>>> joinSet=polygonRDDwithKey.cogroup(filterResult).repartition((polygonRDDwithKey.partitions().size()+filterResult.partitions().size())*2);
		JavaPairRDD<Polygon,Envelope> RefineResult=joinSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Envelope,Tuple2<Iterable<Polygon>,Iterable<ArrayList<Envelope>>>>,Polygon,Envelope>(){
			//GeometryFactory fact = new GeometryFactory();
			public Iterable<Tuple2<Polygon, Envelope>> call(Tuple2<Envelope, Tuple2<Iterable<Polygon>, Iterable<ArrayList<Envelope>>>> t){
				ArrayList<Tuple2<Polygon, Envelope>> QueryAreaAndTarget=new ArrayList();
				Iterator<Polygon> QueryAreaIterator=t._2()._1().iterator();
				
				while(QueryAreaIterator.hasNext())
				{
					Polygon currentQueryArea=QueryAreaIterator.next();
					Iterator<ArrayList<Envelope>> TargetIteratorOutLoop=t._2()._2().iterator();
					while(TargetIteratorOutLoop.hasNext())
					{
						ArrayList<Envelope> currentTargetOutLoop=TargetIteratorOutLoop.next();
						
						
						Iterator<Envelope> targetIterator=currentTargetOutLoop.iterator();
						while(targetIterator.hasNext()){
						Envelope currentTarget=targetIterator.next();
						ArrayList<Coordinate> coordinatesList = new ArrayList<Coordinate>();
						coordinatesList.add(new Coordinate(currentTarget.getMinX(),currentTarget.getMinY()));
						coordinatesList.add(new Coordinate(currentTarget.getMinX(),currentTarget.getMaxY()));
						coordinatesList.add(new Coordinate(currentTarget.getMaxX(),currentTarget.getMaxY()));
						coordinatesList.add(new Coordinate(currentTarget.getMaxX(),currentTarget.getMinY()));
						coordinatesList.add(new Coordinate(currentTarget.getMinX(),currentTarget.getMinY()));
						Coordinate[] coordinates=new Coordinate[coordinatesList.size()];
						coordinates=coordinatesList.toArray(coordinates);
						GeometryFactory fact = new GeometryFactory();
						LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
						Polygon polygon = new Polygon(linear, null, fact);
						if(condition==0){
						if(currentQueryArea.contains(polygon))
						{
							QueryAreaAndTarget.add(new Tuple2<Polygon,Envelope>(currentQueryArea,currentTarget));
						}
						}
						else
						{
							if(currentQueryArea.intersects(polygon))
							{
								QueryAreaAndTarget.add(new Tuple2<Polygon,Envelope>(currentQueryArea,currentTarget));
							}
						}
						}
					}
				}
				return QueryAreaAndTarget;
			}});
		//Delete the duplicate result
				JavaPairRDD<Polygon, Iterable<Envelope>> aggregatedResult=RefineResult.groupByKey();
				JavaPairRDD<Polygon,String> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Envelope>>,Polygon,String>()
						{

							public Tuple2<Polygon, String> call(Tuple2<Polygon, Iterable<Envelope>> t)
									{
								Integer commaFlag=0;
								Iterator<Envelope> valueIterator=t._2().iterator();
								String result="";
								while(valueIterator.hasNext())
								{
									Envelope currentTarget=valueIterator.next();
									String currentTargetString=""+currentTarget.getMinX()+","+currentTarget.getMaxX()+","+currentTarget.getMinY()+","+currentTarget.getMaxY();
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
				//return refinedResult;
				SpatialPairRDD<Polygon,ArrayList<Envelope>> result=new SpatialPairRDD<Polygon,ArrayList<Envelope>>(refinedResult.mapToPair(new PairFunction<Tuple2<Polygon,String>,Polygon,ArrayList<Envelope>>()
				{

					public Tuple2<Polygon, ArrayList<Envelope>> call(Tuple2<Polygon, String> t)
					{
						List<String> resultListString= Arrays.asList(t._2().split(","));
						Iterator<String> targetIterator=resultListString.iterator();
						ArrayList<Envelope> resultList=new ArrayList<Envelope>();
						while(targetIterator.hasNext())
						{
				
							Envelope currentTarget=new Envelope(Double.parseDouble(targetIterator.next()),Double.parseDouble(targetIterator.next()),Double.parseDouble(targetIterator.next()),Double.parseDouble(targetIterator.next()));
							resultList.add(currentTarget);
						}
						return new Tuple2<Polygon,ArrayList<Envelope>>(t._1(),resultList);
					}
					
				}));
				return result;
	}
}
