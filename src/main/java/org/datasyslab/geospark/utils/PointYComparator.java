/**
 * FILE: PointYComparator.java
 * PATH: org.datasyslab.geospark.utils.PointYComparator.java
 * Copyright (c) 2016 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;


// TODO: Auto-generated Javadoc
/**
 * The Class PointYComparator.
 */
public class PointYComparator extends GeometryComparator implements Comparator<Object>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
 	public int compare(Object point1, Object point2) {
	    if(((Point) point1).getY()>((Point) point2).getY())
	    {
	    	return 1;
	    }
	    else if (((Point) point1).getY()<((Point) point2).getY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
