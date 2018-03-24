/**
 * FILE: Embose.java
 * PATH: org.datasyslab.geosparkviz.extension.photoFilter.Embose.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.extension.photoFilter;

import org.datasyslab.geosparkviz.core.PhotoFilter;

// TODO: Auto-generated Javadoc
/**
 * The Class Embose.
 */
public class Embose extends PhotoFilter{

	/**
	 * Instantiates a new embose.
	 */
	public Embose() {
		super(1);
		this.convolutionMatrix[0][0]=-2.0;
		this.convolutionMatrix[1][0]=-1.0;
		this.convolutionMatrix[2][0]=0.0;
		this.convolutionMatrix[0][1]=-1.0;
		this.convolutionMatrix[1][1]=1.0;
		this.convolutionMatrix[2][1]=1.0;
		this.convolutionMatrix[0][2]=0.0;
		this.convolutionMatrix[1][2]=1.0;
		this.convolutionMatrix[2][2]=2.0;
	}

}
