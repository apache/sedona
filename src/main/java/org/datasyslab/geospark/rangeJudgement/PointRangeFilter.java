/**
 * FILE: PointRangeFilter.java
 * PATH: org.datasyslab.geospark.rangeJudgement.PointRangeFilter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class PointRangeFilter.
 */
public class PointRangeFilter implements Function<Point,Boolean>,Serializable {
	
	/** The condition. */
	Integer condition=0;
	
	/** The y 2. */
	Double x1,y1,x2,y2;
	
	/** The range rectangle. */
	Envelope rangeRectangle=new Envelope();
	
	/** The range polygon. */
	Polygon rangePolygon;
	
	/** The range flag. */
	Integer rangeFlag=0;;
	
	/**
	 * Instantiates a new point range filter.
	 *
	 * @param envelope the envelope
	 * @param condition the condition
	 */
	public PointRangeFilter(Envelope envelope,Integer condition)
	{
		this.condition=condition;
		this.rangeRectangle=envelope;
		this.rangeFlag=0;
	}
	
	
	
//	public PointRangeFilter(Polygon polygon,Integer condition)
//	{
//		this.condition=condition;
//		this.rangePolygon=polygon;
//		this.rangeFlag=1;
/* (non-Javadoc)
 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
 */
//	}
	public Boolean call(Point tuple) throws Exception {
		if(rangeFlag==0){
			if(condition==0)
			{
				if(rangeRectangle.contains(tuple.getCoordinate()))
				{
					return true;
				}
				else return false;
			}
			else
			{
				if(rangeRectangle.intersects(tuple.getCoordinate()))
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
//		if(condition==0)
//		{
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
