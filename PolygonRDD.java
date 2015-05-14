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
import Functions.PartitionAssignGridPolygon;
import Functions.PolygonRangeFilter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

public class PolygonRDD implements Serializable{
	private JavaRDD<Polygon> polygonRDD;
	public PolygonRDD(JavaRDD<Polygon> polygonRDD)
	{
		this.setPolygonRDD(polygonRDD);
	}
	public PolygonRDD(JavaSparkContext spark, String InputLocation)
	{
		this.setPolygonRDD(spark.textFile(InputLocation).map(new Function<String,Polygon>()
			{
			public Polygon call(String s)
			{	
				List<String> input=Arrays.asList(s.split(","));
				List<Coordinate> coordinates = null;
				for(int i=0;i<input.size();i=i+2)
				{
					coordinates.add(new Coordinate(Double.parseDouble(input.get(i)),Double.parseDouble(input.get(i+1))));
				}
				coordinates.add(new Coordinate(Double.parseDouble(input.get(input.size()-2)),Double.parseDouble(input.get(input.size()-1))));
				 GeometryFactory fact = new GeometryFactory();
				 LinearRing linear = new GeometryFactory().createLinearRing((Coordinate[]) coordinates.toArray());
				 Polygon polygon = new Polygon(linear, null, fact);
				 return polygon;
			}
			}));
	}
	public JavaRDD<Polygon> getPolygonRDD() {
		return polygonRDD;
	}
	public void setPolygonRDD(JavaRDD<Polygon> polygonRDD) {
		this.polygonRDD = polygonRDD;
	}
	public PolygonRDD SpatialRangeQuery(Envelope envelope,Integer condition)
	{
		JavaRDD<Polygon> result=this.polygonRDD.filter(new PolygonRangeFilter(envelope,condition));
		return new PolygonRDD(result);
	}
	public PolygonRDD SpatialRangeQuery(Polygon polygon,Integer condition)
	{
		JavaRDD<Polygon> result=this.polygonRDD.filter(new PolygonRangeFilter(polygon,condition));
		return new PolygonRDD(result);
	}
/*	public PolygonRDD SpatialRangeQueryFilterRefine(Polygon polygon,Integer condition)
	{
		
	}*/
	public RectangleRDD MinimumBoundingRectangle()
	{
	JavaRDD<Envelope> rectangleRDD=this.polygonRDD.map(new Function<Polygon,Envelope>(){
			
			public Envelope call(Polygon s)
			{
				Envelope MBR= s.getEnvelopeInternal();
				return MBR;
			}
		});
		return new RectangleRDD(rectangleRDD);
	}
	public JavaPairRDD<Polygon,String> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
	{
		//Find the border of both of the two datasets---------------
		final Integer condition=Condition;
		//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
	
		Double minLongtitudeQueryAreaSet;
		Double maxLongtitudeQueryAreaSet;
		Double minLatitudeQueryAreaSet;
		Double maxLatitudeQueryAreaSet;
		Double minLongtitudeTargetSet;
		Double maxLongtitudeTargetSet;
		Double minLatitudeTargetSet;
		Double maxLatitudeTargetSet;
		Double minLongtitude1QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonXMinComparator()).getEnvelopeInternal().getMinX();
		Double maxLongtitude1QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonXMinComparator()).getEnvelopeInternal().getMinX();
		Double minLatitude1QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonYMinComparator()).getEnvelopeInternal().getMinY();
		Double maxLatitude1QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonYMinComparator()).getEnvelopeInternal().getMinY();
		Double minLongtitude2QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonXMaxComparator()).getEnvelopeInternal().getMaxX();
		Double maxLongtitude2QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonXMaxComparator()).getEnvelopeInternal().getMaxX();
		Double minLatitude2QueryAreaSet=polygonRDD.getPolygonRDD().min(new PolygonYMaxComparator()).getEnvelopeInternal().getMaxY();
		Double maxLatitude2QueryAreaSet=polygonRDD.getPolygonRDD().max(new PolygonYMaxComparator()).getEnvelopeInternal().getMaxY();
		Double minLongtitude1TargetSet=this.polygonRDD.min(new PolygonXMinComparator()).getEnvelopeInternal().getMinX();
		Double maxLongtitude1TargetSet=this.polygonRDD.max(new PolygonXMinComparator()).getEnvelopeInternal().getMinX();
		Double minLatitude1TargetSet=this.polygonRDD.min(new PolygonYMinComparator()).getEnvelopeInternal().getMinY();
		Double maxLatitude1TargetSet=this.polygonRDD.max(new PolygonYMinComparator()).getEnvelopeInternal().getMinY();
		Double minLongtitude2TargetSet=this.polygonRDD.min(new PolygonXMaxComparator()).getEnvelopeInternal().getMaxX();
		Double maxLongtitude2TargetSet=this.polygonRDD.max(new PolygonXMaxComparator()).getEnvelopeInternal().getMaxX();
		Double minLatitude2TargetSet=this.polygonRDD.min(new PolygonYMaxComparator()).getEnvelopeInternal().getMaxY();
		Double maxLatitude2TargetSet=this.polygonRDD.max(new PolygonYMaxComparator()).getEnvelopeInternal().getMaxY();
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
		//TargetSet min/max longitude and latitude
		if(minLongtitude1TargetSet<minLongtitude2TargetSet)
		{
			minLongtitudeTargetSet=minLongtitude1TargetSet;
		}
		else
		{
			minLongtitudeTargetSet=minLongtitude2TargetSet;
		}
		if(maxLongtitude1TargetSet>maxLongtitude2TargetSet)
		{
			maxLongtitudeTargetSet=maxLongtitude1TargetSet;
		}
		else
		{
			maxLongtitudeTargetSet=maxLongtitude2TargetSet;
		}
		if(minLatitude1TargetSet<minLatitude2TargetSet)
		{
			minLatitudeTargetSet=minLatitude1TargetSet;
		}
		else
		{
			minLatitudeTargetSet=minLatitude2TargetSet;
		}
		if(maxLatitude1TargetSet>maxLatitude2TargetSet)
		{
			maxLatitudeTargetSet=maxLatitude1TargetSet;
		}
		else
		{
			maxLatitudeTargetSet=maxLatitude2TargetSet;
		}
		//Border found
		Double minLongitude=minLongtitudeTargetSet;
		Double minLatitude=minLatitudeTargetSet;
		Double maxLongitude=maxLongtitudeTargetSet;
		Double maxLatitude=maxLatitudeTargetSet;
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
		JavaPairRDD<Integer,Polygon> TargetSetWithID=this.polygonRDD.mapPartitionsToPair(new PartitionAssignGridPolygon(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Polygon> QueryAreaSetWithID=polygonRDD.getPolygonRDD().mapPartitionsToPair(new PartitionAssignGridPolygon(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
//Join two dataset
		JavaPairRDD<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>> jointSet=QueryAreaSetWithID.cogroup(TargetSetWithID).repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
		//Calculate the relation between one point and one query area
				JavaPairRDD<Polygon,Polygon> queryResult=jointSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Polygon>, Iterable<Polygon>>>, Polygon,Polygon>()
						{

					public Iterable<Tuple2<Polygon, Polygon>> call(
							Tuple2<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>> t)
							throws Exception {
						ArrayList<Tuple2<Polygon, Polygon>> QueryAreaAndTarget=new ArrayList();
						Iterator<Polygon> QueryAreaIterator=t._2()._1().iterator();
						
						while(QueryAreaIterator.hasNext())
						{
							Polygon currentQueryArea=QueryAreaIterator.next();
							Iterator<Polygon> TargetIterator=t._2()._2().iterator();
							while(TargetIterator.hasNext())
							{
								Polygon currentTarget=TargetIterator.next();
								if(condition==0){
								if(currentQueryArea.contains(currentTarget))
								{
									QueryAreaAndTarget.add(new Tuple2<Polygon,Polygon>(currentQueryArea,currentTarget));
								}
								}
								else
								{
									if(currentQueryArea.intersects(currentTarget))
									{
										QueryAreaAndTarget.add(new Tuple2<Polygon,Polygon>(currentQueryArea,currentTarget));
									}
								}
							}
						}
						
						return QueryAreaAndTarget;
					}
			
				});
		//Delete the duplicate result
				JavaPairRDD<Polygon, Iterable<Polygon>> aggregatedResult=queryResult.groupByKey();
				JavaPairRDD<Polygon,String> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Polygon>>,Polygon,String>()
						{

							public Tuple2<Polygon, String> call(Tuple2<Polygon, Iterable<Polygon>> t)
									{
								Integer commaFlag=0;
								Iterator<Polygon> valueIterator=t._2().iterator();
								String result="";
								while(valueIterator.hasNext())
								{
									Polygon currentTarget=valueIterator.next();
									Coordinate[] polygonCoordinate=currentTarget.getCoordinates();
									Integer count=polygonCoordinate.length;
									String currentTargetString="";
									for(int i=0;i<count;i++)
									{
										currentTargetString=currentTargetString+polygonCoordinate[i].x+","+polygonCoordinate[i].y;
									}
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
				return refinedResult;

	}
}