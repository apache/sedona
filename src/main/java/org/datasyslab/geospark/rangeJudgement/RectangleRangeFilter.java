/**
 * FILE: RectangleRangeFilter.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RectangleRangeFilter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleRangeFilter.
 */
public class RectangleRangeFilter implements Function<Envelope,Boolean>,Serializable
{
	
	/** The condition. */
	Integer condition=0;
	
	/** The range rectangle. */
	Envelope rangeRectangle=new Envelope();
	
	/** The range polygon. */
	Polygon rangePolygon;
	
	/** The range flag. */
	Integer rangeFlag=0;
	
	/**
	 * Instantiates a new rectangle range filter.
	 *
	 * @param envelope the envelope
	 * @param condition the condition
	 */
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
/* (non-Javadoc)
 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
 */
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