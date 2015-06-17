package GeoSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class FilterCollection {

}
class PointPreFilter implements Function<Point,Boolean>,Serializable
{
	Envelope boundary;
	public PointPreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Point v1){
		if(boundary.contains(v1.getCoordinate()))
		{
			return true;
		}
		else return false;
	}
	
}
class RectanglePreFilter  implements Function<Envelope,Boolean>,Serializable
{
	Envelope boundary;
	public RectanglePreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Envelope v1){
		if(boundary.contains(v1)||boundary.intersects(v1)||v1.contains(boundary))
		{
			return true;
		}
		else return false;
	}
}
class PolygonPreFilter  implements Function<Polygon,Boolean>,Serializable
{
	Envelope boundary;
	public PolygonPreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Polygon v1){
		if(boundary.contains(v1.getEnvelopeInternal())||boundary.intersects(v1.getEnvelopeInternal())||v1.getEnvelopeInternal().contains(boundary))
		{
			return true;
		}
		else return false;
	}
}
class CircleFilterPoint implements Serializable,Function<Tuple2<Point,Point>,Boolean>
{
	Double radius=0.0;
	Integer condition=1;
	public CircleFilterPoint(Double radius,Integer condition)
	{
		this.radius=radius;
		this.condition=condition;
	}

	public Boolean call(Tuple2<Point, Point> t){
		if(condition==0){
			if(t._1().distance(t._2())<radius)
			{
				return true;
			}
			else return false;
			}
			else
			{
				if(t._1().distance(t._2())<=radius)
				{
					return true;
				}
				return false;
			}
	}
}
class CirclePreFilter implements Function<Circle,Boolean>,Serializable
{
	Envelope boundary;
	public CirclePreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Circle v1){
		
		if(boundary.contains(v1.getMBR())||boundary.intersects(v1.getMBR())||v1.getMBR().contains(boundary))
		{
			return true;
		}
		else return false;
	}

}