/**
 * FILE: Sharpen.java
 * PATH: org.datasyslab.babylon.extension.photoFilter.Sharpen.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.photoFilter;

import org.datasyslab.babylon.core.PhotoFilter;

/**
 * The Class Sharpen.
 */
public class Sharpen extends PhotoFilter{
	
	/**
	 * Instantiates a new sharpen.
	 */
	public Sharpen() {
		super(1);
		this.convolutionMatrix[0][0]=0.0;
		this.convolutionMatrix[1][0]=-1.0;
		this.convolutionMatrix[2][0]=0.0;
		this.convolutionMatrix[0][1]=-1.0;
		this.convolutionMatrix[1][1]=5.0;
		this.convolutionMatrix[2][1]=-1.0;
		this.convolutionMatrix[0][2]=0.0;
		this.convolutionMatrix[1][2]=-1.0;
		this.convolutionMatrix[2][2]=0.0;
	}

}
