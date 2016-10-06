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

public class RectangleDistanceComparator implements Comparator<Envelope>, Serializable{
	Point queryCenter;
	public RectangleDistanceComparator(Point queryCenter)
	{
		this.queryCenter=queryCenter;
	}
	public int compare(Envelope p1, Envelope p2) {
		double distance1 = p1.distance(queryCenter.getEnvelopeInternal());
		double distance2 = p2.distance(queryCenter.getEnvelopeInternal());
		if (distance1 > distance2) {
			return 1;
		} else if (distance1 == distance2) {
			return 0;
		}
		return -1;
	}
}
