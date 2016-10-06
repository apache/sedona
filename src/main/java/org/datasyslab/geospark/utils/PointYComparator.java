package org.datasyslab.geospark.utils;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;

import java.io.Serializable;
import java.util.Comparator;


public class PointYComparator extends GeometryComparator implements Comparator<Point>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
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
