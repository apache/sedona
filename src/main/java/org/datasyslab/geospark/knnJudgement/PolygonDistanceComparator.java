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
import com.vividsolutions.jts.geom.Polygon;

public class PolygonDistanceComparator implements Comparator<Polygon>, Serializable{
	Point queryCenter;
	public PolygonDistanceComparator(Point queryCenter)
	{
		this.queryCenter=queryCenter;
	}
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
