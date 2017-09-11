/**
 * FILE: XMinComparator.java
 * PATH: org.datasyslab.geospark.utils.XMinComparator.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Comparator;

// TODO: Auto-generated Javadoc
/**
 * The Class XMinComparator.
 */
public class XMinComparator<T extends Geometry> implements Comparator<T>, Serializable {

	 /* (non-Javadoc)
 	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 	 */
 	public int compare(T spatialObject1, T spatialObject2) {
		final Envelope envelope1 = spatialObject1.getEnvelopeInternal();
		final Envelope envelope2 = spatialObject2.getEnvelopeInternal();
		if(envelope1.getMinX() > envelope2.getMinX())
 		{
 			return 1;
 		}
 		else if (envelope1.getMinX() < envelope2.getMinX())
 		{
 			return -1;
 		}
 		else
 		{
 			return 0;
 		}
 	}
}
