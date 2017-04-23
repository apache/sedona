/**
 * FILE: RangeFilter.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RangeFilter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class GeometryRangeFilter.
 */
public class RangeFilter implements Function<Object,Boolean>,Serializable {
	
	/** The condition. */
	boolean considerBoundaryIntersection=false;
	
	/** The query window. */
	Object queryWindow;
	
	/**
	 * Instantiates a new geometry range filter.
	 *
	 * @param queryWindow the query window
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public RangeFilter(Envelope queryWindow,boolean considerBoundaryIntersection)
	{
		this.considerBoundaryIntersection=considerBoundaryIntersection;
		this.queryWindow=queryWindow;
	}
	
	/**
	 * Instantiates a new geometry range filter.
	 *
	 * @param queryWindow the query window
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public RangeFilter(Polygon queryWindow,boolean considerBoundaryIntersection)
	{
		this.considerBoundaryIntersection=considerBoundaryIntersection;
		this.queryWindow=queryWindow;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	public Boolean call(Object tuple) throws Exception {
			if(considerBoundaryIntersection==false)
			{
				if(queryWindow instanceof Envelope)
				{
					if(((Envelope)queryWindow).contains(((Geometry) tuple).getEnvelopeInternal()))
					{
						return true;
					}
					else return false;
				}
				else
				{
					if(((Polygon)queryWindow).contains((Geometry) tuple))
					{
						return true;
					}
					else return false;
				}
			}
			else
			{
				if(queryWindow instanceof Envelope)
				{
					if(((Envelope)queryWindow).intersects(((Geometry) tuple).getEnvelopeInternal()))
					{
						return true;
					}
					else return false;
				}
				else
				{
					if(((Polygon)queryWindow).intersects((Geometry) tuple))
					{
						return true;
					}
					else return false;
				}
			}
	}
}
