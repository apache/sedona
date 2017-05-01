package org.datasyslab.babylon.utils;

import java.io.Serializable;

public enum ColorizeOption implements Serializable{
	
	ZAXIS("zaxis"),
	
	SPATIALAGGREGATION("spatialaggregation"),
	
	UNIFORMCOLOR("uniformcolor");

	/** The type name. */
	private String typeName="uniformcolor";
	
	/**
	 * Instantiates a new image type.
	 *
	 * @param typeName the type name
	 */
	private ColorizeOption(String typeName)
	{
		this.setTypeName(typeName);
	}
	
	/**
	 * Gets the image type.
	 *
	 * @param str the str
	 * @return the image type
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
