package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Point;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by jinxuanw on 9/29/15.
 */
class PointYComparator extends GemotryComparator implements Comparator<Point>, Serializable {

	 public int compare(Point point1, Point point2) {
	    if(point1.getY()>point2.getY())
	    {
	    	return 1;
	    }
	    else if (point1.getY()<point2.getY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
