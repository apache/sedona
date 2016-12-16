/**
 * FILE: CircleYMinComparator.java
 * PATH: org.datasyslab.geospark.utils.CircleYMinComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

import org.datasyslab.geospark.geometryObjects.Circle;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Envelope;

// TODO: Auto-generated Javadoc
/**
 * The Class CircleYMinComparator.
 */
public class CircleYMinComparator extends GeometryComparator implements Comparator<Circle>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
 	public int compare(Circle circle1, Circle circle2) {
		 Envelope envelope1=circle1.getMBR();
		 Envelope envelope2=circle1.getMBR();
	    if(envelope1.getMinY()>envelope2.getMinY())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMinY()<envelope2.getMinY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}