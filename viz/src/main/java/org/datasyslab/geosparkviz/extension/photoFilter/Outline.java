/**
 * FILE: Outline.java
 * PATH: org.datasyslab.geosparkviz.extension.photoFilter.Outline.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.extension.photoFilter;

import org.datasyslab.geosparkviz.core.PhotoFilter;

// TODO: Auto-generated Javadoc
/**
 * The Class Outline.
 */
public class Outline extends PhotoFilter{

	/**
	 * Instantiates a new outline.
	 */
	public Outline() {
		super(1);
		this.convolutionMatrix[0][0]=-1.0;
		this.convolutionMatrix[1][0]=-1.0;
		this.convolutionMatrix[2][0]=-1.0;
		this.convolutionMatrix[0][1]=-1.0;
		this.convolutionMatrix[1][1]=8.0;
		this.convolutionMatrix[2][1]=-1.0;
		this.convolutionMatrix[0][2]=-1.0;
		this.convolutionMatrix[1][2]=-1.0;
		this.convolutionMatrix[2][2]=-1.0;
	}

}
