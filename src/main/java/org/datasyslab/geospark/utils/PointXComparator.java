/**
 * FILE: PointXComparator.java
 * PATH: org.datasyslab.geospark.utils.PointXComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Point;

// TODO: Auto-generated Javadoc
/**
 * The Class PointXComparator.
 */
public class PointXComparator extends GeometryComparator implements Comparator<Point>, Serializable {
    

 	/* (non-Javadoc)
	  * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	  */
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