/**
 * FILE: ImageType.java
 * PATH: org.datasyslab.babylon.utils.ImageType.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.utils;

import java.io.Serializable;

/**
 * The Enum ImageType.
 */
public enum ImageType implements Serializable{
	
	/** The png. */
	PNG("png"),
	
	/** The gif. */
	GIF("gif");

	/** The type name. */
	private String typeName="png";
	
	/**
	 * Instantiates a new image type.
	 *
	 * @param typeName the type name
	 */
	private ImageType(String typeName)
	{
		this.setTypeName(typeName);
	}
	
	/**
	 * Gets the image type.
	 *
	 * @param str the str
	 * @return the image type
	 */
	public static ImageType getImageType(String str) {
	    for (ImageType me : ImageType.values()) {
	        if (me.name().equalsIgnoreCase(str))
	            return me;
	    }
	    return null;
	}

	/**
	 * Gets the type name.
	 *
	 * @return the type name
	 */
	public String getTypeName() {
		return typeName;
	}

	/**
	 * Sets the type name.
	 *
	 * @param typeName the new type name
	 */
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
}
