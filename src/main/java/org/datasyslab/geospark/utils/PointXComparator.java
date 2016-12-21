/**
 * FILE: PointXComparator.java
 * PATH: org.datasyslab.geospark.utils.PointXComparator.java
 * Copyright (c) 2016 Arizona State University Data Systems Lab.
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
public class PointXComparator extends GeometryComparator implements Comparator<Object>, Serializable {
    

 	/* (non-Javadoc)
	  * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	  */
	 public int compare(Object point1, Object point2) {
	    if(((Point)point1).getX()>((Point)point2).getX())
	    {
	    	return 1;
	    }
	    else if (((Point)point1).getX()<((Point)point2).getX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}