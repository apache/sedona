package org.datasyslab.geospark.enums;

public enum IndexType {
	QUADTREE,
	RTREE;
	public static IndexType getIndexType(String str) {
	    for (IndexType me : IndexType.values()) {
	        if (me.name().equalsIgnoreCase(str))
	            return me;
	    }
	    return null;
	}
}
