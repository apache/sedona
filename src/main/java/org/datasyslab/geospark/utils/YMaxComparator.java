/**
 * FILE: YMaxComparator.java
 * PATH: org.datasyslab.geospark.utils.YMaxComparator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

import org.datasyslab.geospark.geometryObjects.Circle;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

// TODO: Auto-generated Javadoc
/**
 * The Class YMaxComparator.
 */
public class YMaxComparator implements Comparator<Object>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
 	public int compare(Object spatialObject1, Object spatialObject2) {
 		if(spatialObject1 instanceof Envelope)
 		{
 		    if(((Envelope) spatialObject1).getMaxY()>((Envelope) spatialObject2).getMaxY())
 		    {
 		    	return 1;
 		    }
 		    else if (((Envelope) spatialObject1).getMaxY()<((Envelope) spatialObject2).getMaxY())
 		    {
 		    	return -1;
 		    }
 		    else
 		    {
 		    	return 0;
 		    }
 		}
 		else if(spatialObject1 instanceof Circle)
 		{
 		    if(((Circle) spatialObject1).getMBR().getMaxY()>((Circle) spatialObject2).getMBR().getMaxY())
 		    {
 		    	return 1;
 		    }
 		    else if (((Circle) spatialObject1).getMBR().getMaxY()<((Circle) spatialObject2).getMBR().getMaxY())
 		    {
 		    	return -1;
 		    }
 		    else
 		    {
 		    	return 0;
 		    }
 		}
 		else
 		{
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
}
