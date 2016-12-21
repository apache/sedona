/**
 * FILE: RectangleXMaxComparator.java
 * PATH: org.datasyslab.geospark.utils.RectangleXMaxComparator.java
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

import com.vividsolutions.jts.geom.Envelope;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleXMaxComparator.
 */
public class RectangleXMaxComparator extends GeometryComparator implements Comparator<Object>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
 	public int compare(Object envelope1, Object envelope2) {
	    if(((Envelope) envelope1).getMaxX()>((Envelope) envelope2).getMaxX())
	    {
	    	return 1;
	    }
	    else if (((Envelope) envelope1).getMaxX()<((Envelope) envelope2).getMaxX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
