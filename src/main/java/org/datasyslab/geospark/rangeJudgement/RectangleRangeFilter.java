/**
 * FILE: RectangleRangeFilter.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RectangleRangeFilter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleRangeFilter.
 */
public class RectangleRangeFilter implements Function<Object,Boolean>,Serializable
{
	
	/** The condition. */
	Integer condition=0;
	
	/** The query window. */
	Object queryWindow;
	
	/**
	 * Instantiates a new rectangle range filter.
	 *
	 * @param queryWindow the query window
	 * @param condition the condition
	 */
	public RectangleRangeFilter(Envelope queryWindow,Integer condition)
	{
		this.condition=condition;
		this.queryWindow=queryWindow;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	public Boolean call(Object tuple) throws Exception {
			if(condition==0)
			{
				if(((Envelope)queryWindow).contains((Envelope) tuple))
				{
					return true;
				}
				else return false;
			}
			else
			{
				if(((Envelope)queryWindow).intersects(((Envelope) tuple)))
				{
					return true;
				}
				else return false;
			}
	}
}