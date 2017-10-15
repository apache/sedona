/**
 * FILE: ColorizeOption.java
 * PATH: org.datasyslab.babylon.utils.ColorizeOption.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.babylon.utils;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Enum ColorizeOption.
 */
public enum ColorizeOption implements Serializable{
	
	/** The earthobservation. */
	EARTHOBSERVATION("EARTHOBSERVATION"),
	
	/** The spatialaggregation. */
	SPATIALAGGREGATION("spatialaggregation"),
	
	/** The normal. */
	NORMAL("normal");

	/** The type name. */
	private String typeName="normal";
	
	/**
	 * Instantiates a new colorize option.
	 *
	 * @param typeName the type name
	 */
	private ColorizeOption(String typeName)
	{
		this.setTypeName(typeName);
	}
	
	/**
	 * Gets the colorize option.
	 *
	 * @param str the str
	 * @return the colorize option
	 */
	public static ColorizeOption getColorizeOption(String str) {
	    for (ColorizeOption me : ColorizeOption.values()) {
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
