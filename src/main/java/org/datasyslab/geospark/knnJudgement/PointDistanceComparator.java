/**
 * FILE: PointDistanceComparator.java
 * PATH: org.datasyslab.geospark.knnJudgement.PointDistanceComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Point;

// TODO: Auto-generated Javadoc
/**
 * The Class PointDistanceComparator.
 */
public class PointDistanceComparator implements Comparator<Point>, Serializable{
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new point distance comparator.
	 *
	 * @param queryCenter the query center
	 */
	public PointDistanceComparator(Point queryCenter)
	{
		this.queryCenter=queryCenter;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(Point p1, Point p2) {
		double distance1 = p1.getCoordinate().distance(queryCenter.getCoordinate());
		double distance2 = p2.getCoordinate().distance(queryCenter.getCoordinate());
		if (distance1 > distance2) {
			return 1;
		} else if (distance1 == distance2) {
			return 0;
		}
		return -1;
	}
}
