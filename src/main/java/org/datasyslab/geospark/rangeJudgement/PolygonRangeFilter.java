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

public class PolygonRangeFilter implements Function<Polygon,Boolean>,Serializable {
	Integer condition=0;
	Envelope rangeRectangle=new Envelope();
	Polygon rangePolygon;
	Integer rangeFlag=0;
	
	public PolygonRangeFilter(Envelope envelope,Integer condition)
	{
		this.condition=condition;
		this.rangeRectangle=envelope;
		this.rangeFlag=0;
	}
//	public PolygonRangeFilter(Polygon polygon,Integer condition)
//	{
//		this.condition=condition;
//		this.rangePolygon=polygon;
//		this.rangeFlag=1;
//	}
	public Boolean call(Polygon tuple) throws Exception {
	if(rangeFlag==0){
//		ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
//		coordinates.add(new Coordinate(rangeRectangle.getMaxX(),rangeRectangle.getMaxY()));
//		coordinates.add(new Coordinate(rangeRectangle.getMinX(),rangeRectangle.getMinY()));
//		GeometryFactory fact = new GeometryFactory();
//		LinearRing linear = new GeometryFactory().createLinearRing((Coordinate[]) coordinates.toArray());
//		Polygon polygon = new Polygon(linear, null, fact);
		GeometryFactory fact = new GeometryFactory();
		Polygon polygon = (Polygon) fact.toGeometry(rangeRectangle);
			if(condition==0)
			{
				if(polygon.contains(tuple))
				{
					return true;
				}
				else return false;
			}
			else
			{
				if(polygon.intersects(tuple))
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
//
//		if(condition==0)
//		{
//
//			if(rangePolygon.contains(tuple))
//			{
//				return true;
//			}
//			else return false;
//		}
//		else
//		{
//			if(rangePolygon.intersects(tuple))
//			{
//				return true;
//			}
//			else return false;
//		}
//	}
	}
}
