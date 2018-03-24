/**
 * FILE: PhotoFilter.java
 * PATH: org.datasyslab.geosparkviz.core.PhotoFilter.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.core;

// TODO: Auto-generated Javadoc
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
