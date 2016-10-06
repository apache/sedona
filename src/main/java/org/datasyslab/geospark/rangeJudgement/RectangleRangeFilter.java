package org.datasyslab.geospark.rangeJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

public class RectangleRangeFilter implements Function<Envelope,Boolean>,Serializable
{
	Integer condition=0;
	Envelope rangeRectangle=new Envelope();
	Polygon rangePolygon;
	Integer rangeFlag=0;
	
	public RectangleRangeFilter(Envelope envelope,Integer condition)
	{
		this.condition=condition;
		this.rangeRectangle=envelope;
		this.rangeFlag=0;
	}
//	public RectangleRangeFilter(Polygon polygon,Integer condition)
//	{
//		this.condition=condition;
//		this.rangePolygon=polygon;
//		this.rangeFlag=1;
//	}
	public Boolean call(Envelope tuple) throws Exception {
	if(rangeFlag==0){
			if(condition==0)
			{
				if(rangeRectangle.contains(tuple))
				{
					return true;
				}
				else return false;
			}
			else
			{
				if(rangeRectangle.intersects(tuple))
				{
					return true;
				}
				else return false; 
			}
	}
	//todo:  fix later;
	else {
		return false;
	}
//	else
//	{
//		ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
//		coordinates.add(new Coordinate(tuple.getMinX(),tuple.getMinY()));
//		coordinates.add(new Coordinate(tuple.getMinX(),tuple.getMaxY()));
//		coordinates.add(new Coordinate(tuple.getMaxX(),tuple.getMaxY()));
//		coordinates.add(new Coordinate(tuple.getMaxX(),tuple.getMinY()));
//		coordinates.add(new Coordinate(tuple.getMinX(),tuple.getMinY()));
//		GeometryFactory fact = new GeometryFactory();
//		LinearRing linear = new GeometryFactory().createLinearRing((Coordinate[]) coordinates.toArray());
//		Polygon polygon = new Polygon(linear, null, fact);
//		if(condition==0)
//		{
//
//			if(rangePolygon.contains(polygon))
//			{
//				return true;
//			}
//			else return false;
//		}
//		else
//		{
//			if(rangePolygon.intersects(polygon))
//			{
//				return true;
//			}
//			else return false;
//		}
//	}
	}
}