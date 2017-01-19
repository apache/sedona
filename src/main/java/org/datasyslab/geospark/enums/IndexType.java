/**
 * FILE: IndexType.java
 * PATH: org.datasyslab.geospark.enums.IndexType.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.enums;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Enum IndexType.
 */
public enum IndexType implements Serializable{
	
	/** The quadtree. */
	QUADTREE,
	
	/** The rtree. */
	RTREE;
	
	/**
	 * Gets the index type.
	 *
	 * @param str the str
	 * @return the index type
	 */
	public static IndexType getIndexType(String str) {
	    for (IndexType me : IndexType.values()) {
	        if (me.name().equalsIgnoreCase(str))
	            return me;
	    }
	    return null;
	}
}
