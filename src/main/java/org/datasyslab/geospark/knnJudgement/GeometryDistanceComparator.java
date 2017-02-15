/**
 * FILE: GeometryDistanceComparator.java
 * PATH: org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

// TODO: Auto-generated Javadoc
/**
 * The Class GeometryDistanceComparator.
 */
public class GeometryDistanceComparator implements Comparator<Object>, Serializable{
	
	/** The query center. */
	Point queryCenter;
	
	/** The normal order. */
	boolean normalOrder;
	
	/**
	 * Instantiates a new geometry distance comparator.
	 *
	 * @param queryCenter the query center
	 * @param normalOrder the normal order
	 */
	public GeometryDistanceComparator(Point queryCenter,boolean normalOrder)
	{
		this.queryCenter = queryCenter;
		this.normalOrder = normalOrder;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(Object p1, Object p2) {
		double distance1 = ((Geometry) p1).getEnvelopeInternal().distance(queryCenter.getEnvelopeInternal());
		double distance2 = ((Geometry) p2).getEnvelopeInternal().distance(queryCenter.getEnvelopeInternal());
		if(this.normalOrder)
		{
			if (distance1 > distance2) {
				return 1;
			} else if (distance1 == distance2) {
				return 0;
			}
			return -1;
		}
		else
		{
			if (distance1 > distance2) {
				return -1;
			} else if (distance1 == distance2) {
				return 0;
			}
			return 1;
		}

	}
}
