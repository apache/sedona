/**
 * FILE: PolygonRangeFilter.java
 * PATH: org.datasyslab.geospark.rangeJudgement.PolygonRangeFilter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonRangeFilter.
 */
public class PolygonRangeFilter implements Function<Polygon,Boolean>,Serializable {
	
	/** The condition. */
	Integer condition=0;
	
	/** The range rectangle. */
	Envelope rangeRectangle=new Envelope();
	
	/** The range polygon. */
	Polygon rangePolygon;
	
	/** The range flag. */
	Integer rangeFlag=0;
	
	/**
	 * Instantiates a new polygon range filter.
	 *
	 * @param envelope the envelope
	 * @param condition the condition
	 */
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
/* (non-Javadoc)
 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
 */
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
