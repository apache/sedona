package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Point;

public class PointXComparator extends GemotryComparator implements Comparator<Point>, Serializable {
    
	 public int compare(Point point1, Point point2) {
	    if(point1.getX()>point2.getX())
	    {
	    	return 1;
	    }
	    else if (point1.getX()<point2.getX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}