package org.datasyslab.geospark.enums;

public enum GridType {
	EQUALGRID,
	HILBERT,
	RTREE,
	VORONOI;
	
	public static GridType getGridType(String str) {
	    for (GridType me : GridType.values()) {
	        if (me.name().equalsIgnoreCase(str))
	            return me;
	    }
	    return null;
	}
}
