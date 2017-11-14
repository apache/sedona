/**
 * FILE: GridType.java
 * PATH: org.datasyslab.geospark.enums.GridType.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.enums;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Enum GridType.
 */
public enum GridType implements Serializable {

	/**
	 * The equalgrid.
	 */
	EQUALGRID,

	/**
	 * The hilbert.
	 */
	HILBERT,

	/**
	 * The rtree.
	 */
	RTREE,

	/**
	 * The voronoi.
	 */
	VORONOI,
	/**
	 * The voronoi.
	 */
	QUADTREE,

	/**
	 * K-D-B-tree (k-dimensional B-tree)
	 */
	KDBTREE;

	/**
	 * Gets the grid type.
	 *
	 * @param str the str
	 * @return the grid type
	 */
	public static GridType getGridType(String str) {
		for (GridType me : GridType.values()) {
			if (me.name().equalsIgnoreCase(str))
				return me;
		}
		return null;
	}
}
