/**
 * FILE: RangeFilter.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RangeFilter.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class GeometryRangeFilter.
 */
public class RangeFilter<T extends Geometry> implements Function<T, Boolean>,Serializable {
	
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
	public Boolean call(T geometry) throws Exception {
			if(considerBoundaryIntersection==false)
			{
				if(queryWindow instanceof Envelope)
				{
					return (((Envelope)queryWindow).contains(geometry.getEnvelopeInternal()));
				}
				else
				{
					return (((Polygon)queryWindow).contains(geometry));
				}
			}
			else
			{
				if(queryWindow instanceof Envelope)
				{
					return (((Envelope)queryWindow).intersects(geometry.getEnvelopeInternal()));
				}
				else
				{
					return (((Polygon)queryWindow).intersects(geometry));
				}
			}
	}
}
