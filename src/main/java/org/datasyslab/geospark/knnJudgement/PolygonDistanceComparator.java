/**
 * FILE: PolygonDistanceComparator.java
 * PATH: org.datasyslab.geospark.knnJudgement.PolygonDistanceComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonDistanceComparator.
 */
public class PolygonDistanceComparator implements Comparator<Polygon>, Serializable{
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new polygon distance comparator.
	 *
	 * @param queryCenter the query center
	 */
	public PolygonDistanceComparator(Point queryCenter)
	{
		this.queryCenter=queryCenter;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(Polygon p1, Polygon p2) {
		double distance1 = p1.distance(queryCenter);
		double distance2 = p2.distance(queryCenter);
		if (distance1 > distance2) {
			return 1;
		} else if (distance1 == distance2) {
			return 0;
		}
		return -1;
	}
}
