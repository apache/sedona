/**
 * FILE: PolygonYMaxComparator.java
 * PATH: org.datasyslab.geospark.utils.PolygonYMaxComparator.java
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

import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonYMaxComparator.
 */
public class PolygonYMaxComparator extends GeometryComparator implements Comparator<Object>, Serializable
{

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(Object polygon1, Object polygon2) {
	    if(((Polygon) polygon1).getEnvelopeInternal().getMaxY()>((Polygon) polygon2).getEnvelopeInternal().getMaxY())
	    {
	    	return 1;
	    }
	    else if (((Polygon) polygon1).getEnvelopeInternal().getMaxY()<((Polygon) polygon2).getEnvelopeInternal().getMaxY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
