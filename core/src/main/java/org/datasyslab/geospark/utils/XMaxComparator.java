/**
 * FILE: XMaxComparator.java
 * PATH: org.datasyslab.geospark.utils.XMaxComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Geometry;

/**
 * The Class XMaxComparator.
 */
public class XMaxComparator implements Comparator<Object>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
 	public int compare(Object spatialObject1, Object spatialObject2) {
 		if(((Geometry) spatialObject1).getEnvelopeInternal().getMaxX()> ((Geometry) spatialObject2).getEnvelopeInternal().getMaxX())
 		{
 			return 1;
 		}
 		else if (((Geometry) spatialObject1).getEnvelopeInternal().getMaxX()<((Geometry) spatialObject2).getEnvelopeInternal().getMaxX())
 		{
 			return -1;
 		}
 		else
 		{
 			return 0;
 		}
 	}
}

