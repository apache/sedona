/**
 * FILE: RectangleDistanceComparator.java
 * PATH: org.datasyslab.geospark.knnJudgement.RectangleDistanceComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleDistanceComparator.
 */
public class RectangleDistanceComparator implements Comparator<Polygon>, Serializable{
	
	/** The query center. */
	Point queryCenter;
	
	/** The normal order. */
	boolean normalOrder;
	
	/**
	 * Instantiates a new rectangle distance comparator.
	 *
	 * @param queryCenter the query center
	 * @param normalOrder the normal order
	 */
	public RectangleDistanceComparator(Point queryCenter, boolean normalOrder)
	{
		this.queryCenter = queryCenter;
		this.normalOrder = normalOrder;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(Polygon p1, Polygon p2) {
		double distance1 = p1.distance(queryCenter);
		double distance2 = p2.distance(queryCenter);
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
