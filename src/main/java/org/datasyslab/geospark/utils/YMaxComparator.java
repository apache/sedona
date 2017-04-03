/**
 * FILE: YMaxComparator.java
 * PATH: org.datasyslab.geospark.utils.YMaxComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Geometry;

/**
 * The Class YMaxComparator.
 */
public class YMaxComparator implements Comparator<Object>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
 	public int compare(Object spatialObject1, Object spatialObject2) {
 		if(((Geometry) spatialObject1).getEnvelopeInternal().getMaxY()>((Geometry) spatialObject2).getEnvelopeInternal().getMaxY())
 		{
 			return 1;
 		}
 		else if (((Geometry) spatialObject1).getEnvelopeInternal().getMaxY()<((Geometry) spatialObject2).getEnvelopeInternal().getMaxY())
 		{
 			return -1;
 		}
 		else
 		{
 			return 0;
 		}
 	}
}
