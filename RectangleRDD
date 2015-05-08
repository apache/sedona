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

import scala.Tuple2;
import Functions.PartitionAssignGridRectangle;
import Functions.RectangleRangeFilter;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

public class RectangleRDD implements Serializable {
	private JavaRDD<Envelope> rectangleRDD;
	public RectangleRDD(JavaRDD<Envelope> rectangleRDD)
	{
		this.setRectangleRDD(rectangleRDD);
	}
	public RectangleRDD(JavaSparkContext spark, String InputLocation)
	{
		this.setRectangleRDD(spark.textFile(InputLocation).map(new Function<String,Envelope>()
			{
			public Envelope call(String s)
			{	
				List<String> input=Arrays.asList(s.split(","));
				 Envelope envelope = new Envelope(Double.parseDouble(input.get(0)),Double.parseDouble(input.get(2)),Double.parseDouble(input.get(1)),Double.parseDouble(input.get(3)));
				 return envelope;
			}
			}));
	}
	public JavaRDD<Envelope> getRectangleRDD() {
		return rectangleRDD;
	}
	public void setRectangleRDD(JavaRDD<Envelope> rectangleRDD) {
		this.rectangleRDD = rectangleRDD;
	}
	public void rePartition(Integer partitions)
	{
		this.rectangleRDD=this.rectangleRDD.repartition(partitions);
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
	public JavaPairRDD<Envelope,String> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)
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
		Double minLongtitude1QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleXMinComparator()).getMinX();
		Double maxLongtitude1QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleXMinComparator()).getMinX();
		Double minLatitude1QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleYMinComparator()).getMinY();
		Double maxLatitude1QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleYMinComparator()).getMinY();
		Double minLongtitude2QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleXMaxComparator()).getMaxX();
		Double maxLongtitude2QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleXMaxComparator()).getMaxX();
		Double minLatitude2QueryAreaSet=rectangleRDD.getRectangleRDD().min(new RectangleYMaxComparator()).getMaxY();
		Double maxLatitude2QueryAreaSet=rectangleRDD.getRectangleRDD().max(new RectangleYMaxComparator()).getMaxY();
		Double minLongtitude1TargetSet=this.rectangleRDD.min(new RectangleXMinComparator()).getMinX();
		Double maxLongtitude1TargetSet=this.rectangleRDD.max(new RectangleXMinComparator()).getMinX();
		Double minLatitude1TargetSet=this.rectangleRDD.min(new RectangleYMinComparator()).getMinY();
		Double maxLatitude1TargetSet=this.rectangleRDD.max(new RectangleYMinComparator()).getMinY();
		Double minLongtitude2TargetSet=this.rectangleRDD.min(new RectangleXMaxComparator()).getMaxX();
		Double maxLongtitude2TargetSet=this.rectangleRDD.max(new RectangleXMaxComparator()).getMaxX();
		Double minLatitude2TargetSet=this.rectangleRDD.min(new RectangleYMaxComparator()).getMaxY();
		Double maxLatitude2TargetSet=this.rectangleRDD.max(new RectangleYMaxComparator()).getMaxY();
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
		JavaPairRDD<Integer,Envelope> TargetSetWithID=this.rectangleRDD.mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithID=rectangleRDD.getRectangleRDD().mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
//Join two dataset
		JavaPairRDD<Integer, Tuple2<Iterable<Envelope>, Iterable<Envelope>>> jointSet=QueryAreaSetWithID.cogroup(TargetSetWithID);
//Calculate the relation between one point and one query area
		JavaPairRDD<Envelope,String> result=jointSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Envelope>, Iterable<Envelope>>>, Envelope,String>()
				{

			public Iterable<Tuple2<Envelope, String>> call(
					Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Envelope>>> t)
					throws Exception {
				ArrayList<Tuple2<Envelope, String>> QueryAreaAndPoint=new ArrayList();
				Iterator<Envelope> QueryAreaIterator=t._2()._1().iterator();
				
				while(QueryAreaIterator.hasNext())
				{
					Envelope currentQueryArea=QueryAreaIterator.next();
					String QueryArea="";
					Iterator<Envelope> TargetIterator=t._2()._2().iterator();
					while(TargetIterator.hasNext())
					{
						Envelope currentTarget=TargetIterator.next();
						if(condition==0){
						if(currentQueryArea.contains(currentTarget))
						{
							QueryArea=QueryArea+"|"+currentTarget.getMinX()+","+currentTarget.getMinY()+","+currentTarget.getMaxX()+","+currentTarget.getMaxY()+"|";
						}
						}
						else
						{
							if(currentQueryArea.intersects(currentTarget))
							{
								QueryArea=QueryArea+"|"+currentTarget.getMinX()+","+currentTarget.getMinY()+","+currentTarget.getMaxX()+","+currentTarget.getMaxY()+"|";
							}
						}
					}
					
					QueryAreaAndPoint.add(new Tuple2<Envelope, String>(currentQueryArea,QueryArea));
				}
				
				return QueryAreaAndPoint;
			}
	
		});
//Delete the duplicate result
		JavaPairRDD<Envelope,String> refinedResult=result.reduceByKey(new Function2<String,String,String>(){

			public String call(String v1, String v2) throws Exception {
				if(v1=="" && v2!="")
				{
					return v2;
				}
				else if(v1!="" && v2=="")
				{
					return v1;
				}
				else if(v1!="" && v2!="")
				{
					return v1+""+v2;
				}
				else
				{
					return "";
				}
			}});
		//Persist the result on HDFS
		//refinedResult.repartition(1).saveAsTextFile(OutputLocation);
		return refinedResult;
	}
}
