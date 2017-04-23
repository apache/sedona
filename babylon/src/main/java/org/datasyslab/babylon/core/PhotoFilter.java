/**
 * FILE: PhotoFilter.java
 * PATH: org.datasyslab.babylon.core.PhotoFilter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

/**
 * The Class PhotoFilter.
 */
public abstract class PhotoFilter {
	
	/** The filter radius. */
	protected int filterRadius;
	
	/** The convolution matrix. */
	protected Double[][] convolutionMatrix;
	
	/**
	 * Instantiates a new photo filter.
	 *
	 * @param filterRadius the filter radius
	 */
	public PhotoFilter(int filterRadius)
	{
		this.filterRadius=filterRadius;
		this.convolutionMatrix = new Double[2*filterRadius+1][2*filterRadius+1];
	}
	
	/**
	 * Gets the filter radius.
	 *
	 * @return the filter radius
	 */
	public int getFilterRadius() {
		return filterRadius;
	}
	
	/**
	 * Gets the convolution matrix.
	 *
	 * @return the convolution matrix
	 */
	public Double[][] getConvolutionMatrix() {
		return convolutionMatrix;
	}
}
