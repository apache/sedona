package org.datasyslab.geospark.knnJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

public class PointDistanceComparator implements Comparator<Point>, Serializable{
	Point queryCenter;
	public PointDistanceComparator(Point queryCenter)
	{
		this.queryCenter=queryCenter;
	}
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
