/**
 * FILE: Embose.java
 * PATH: org.datasyslab.babylon.extension.photoFilter.Embose.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.photoFilter;

import org.datasyslab.babylon.core.PhotoFilter;

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
