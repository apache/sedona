/**
 * FILE: FileDataSplitter.java
 * PATH: org.datasyslab.geospark.enums.FileDataSplitter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */

package org.datasyslab.geospark.enums;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Enum FileDataSplitter.
 */
public enum FileDataSplitter implements Serializable{
	
	/** The csv. */
	CSV(","),
	
	/** The tsv. */
	TSV("\t"),
	
	/** The geojson. */
	GEOJSON(""),
	
	/** The wkt. */
	WKT("\t");
	
	/**
	 * Gets the file data splitter.
	 *
	 * @param str the str
	 * @return the file data splitter
	 */
	public static FileDataSplitter getFileDataSplitter(String str) {
	    for (FileDataSplitter me : FileDataSplitter.values()) {
	        if (me.name().equalsIgnoreCase(str))
	            return me;
	    }
	    return null;
	}
	
	/** The splitter. */
	private String splitter;

	/**
	 * Instantiates a new file data splitter.
	 *
	 * @param splitter the splitter
	 */
	private FileDataSplitter(String splitter) {
		this.splitter = splitter;
	}
	
	/**
	 * Gets the delimiter.
	 *
	 * @return the delimiter
	 */
	public String getDelimiter() {
		return this.splitter;
	}
}

