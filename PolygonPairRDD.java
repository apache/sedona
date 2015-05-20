package GeoSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class PolygonPairRDD implements Serializable {
	private JavaPairRDD<Polygon,String> polygonPairRDD;
	public PolygonPairRDD(JavaPairRDD<Polygon,String> polygonPairRDD)
	{
		this.polygonPairRDD=polygonPairRDD;
	}
	public JavaPairRDD<Polygon,String> getPolygonPairRDD() {
		return polygonPairRDD;
	}

	public void setPolygonPairRDD(JavaPairRDD<Polygon,String> polygonPairRDD) {
		this.polygonPairRDD = polygonPairRDD;
	}
	public JavaPairRDD<Polygon,ArrayList<Point>> ParseValueToPoint()
	{
		return this.polygonPairRDD.mapToPair(new PairFunction<Tuple2<Polygon,String>,Polygon,ArrayList<Point>>()
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
					
				});
	}
	public JavaPairRDD<Polygon,ArrayList<Envelope>> ParseValueToRectangle()
	{
		return this.polygonPairRDD.mapToPair(new PairFunction<Tuple2<Polygon,String>,Polygon,ArrayList<Envelope>>()
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
					
				});
	}
	public JavaPairRDD<Polygon,ArrayList<Polygon>> ParseValueToPolygon()
	{
		return this.polygonPairRDD.mapToPair(new PairFunction<Tuple2<Polygon,String>,Polygon,ArrayList<Polygon>>()
				{

					public Tuple2<Polygon, ArrayList<Polygon>> call(Tuple2<Polygon, String> t){
						List<String> input=Arrays.asList(t._2().split(";"));
						Iterator<String> inputIterator=input.iterator();
						ArrayList<Polygon> resultList=new ArrayList<Polygon>();
						while(inputIterator.hasNext())
						{
						List<String> resultListString=Arrays.asList(inputIterator.next().split(","));
						Iterator<String> targetIterator=resultListString.iterator();
						ArrayList<Coordinate> coordinatesList = new ArrayList<Coordinate>();
						while(targetIterator.hasNext())
						{
							coordinatesList.add(new Coordinate(Double.parseDouble(targetIterator.next()),Double.parseDouble(targetIterator.next())));
						}
						Coordinate[] coordinates=new Coordinate[coordinatesList.size()];
						coordinates=coordinatesList.toArray(coordinates);
						GeometryFactory fact = new GeometryFactory();
						 LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
						 Polygon polygon = new Polygon(linear, null, fact);
						 resultList.add(polygon);
						}
						return new Tuple2<Polygon,ArrayList<Polygon>>(t._1(),resultList);
					}
			
				});
	}
	public void PersistOnFile(String OutputLocation)
	{
		this.polygonPairRDD.saveAsTextFile(OutputLocation);
	}
}
