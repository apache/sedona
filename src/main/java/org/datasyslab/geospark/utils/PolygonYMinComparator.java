/**
 * FILE: PolygonYMinComparator.java
 * PATH: org.datasyslab.geospark.utils.PolygonYMinComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
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
 * The Class PolygonYMinComparator.
 */
public class PolygonYMinComparator extends GeometryComparator implements Comparator<Polygon>, Serializable
{

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(Polygon polygon1, Polygon polygon2) {
	    if(polygon1.getEnvelopeInternal().getMinY()>polygon2.getEnvelopeInternal().getMinY())
	    {
	    	return 1;
	    }
	    else if (polygon1.getEnvelopeInternal().getMinY()<polygon2.getEnvelopeInternal().getMinY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
