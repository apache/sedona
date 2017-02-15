package org.datasyslab.babylon.utils;

import java.io.Serializable;

public enum ImageType implements Serializable{
	
	png,
	jpg,
	gif;

	public static ImageType getImageType(String str) {
	    for (ImageType me : ImageType.values()) {
	        if (me.name().equalsIgnoreCase(str))
	            return me;
	    }
	    return null;
	}
}
