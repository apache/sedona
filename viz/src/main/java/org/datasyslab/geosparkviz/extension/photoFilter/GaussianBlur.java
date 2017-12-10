/**
 * FILE: GaussianBlur.java
 * PATH: org.datasyslab.geosparkviz.extension.photoFilter.GaussianBlur.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.extension.photoFilter;

import org.datasyslab.geosparkviz.core.PhotoFilter;

// TODO: Auto-generated Javadoc
/**
 * The Class GaussianBlur.
 */
public class GaussianBlur extends PhotoFilter{
	
	/** The stdev. */
	double stdev = 0.5;
	
	/**
	 * Instantiates a new gaussian blur.
	 *
	 * @param blurRadius the blur radius
	 */
	public GaussianBlur(int blurRadius)
	{
		super(blurRadius);
		double originalConvolutionMatrixSum=0.0;
		for (int x = -filterRadius; x <= filterRadius; x++) {
			for (int y = -filterRadius; y <= filterRadius; y++) {
				convolutionMatrix[x + filterRadius][y + filterRadius] = Math.exp(-(x * x + y * y) / (2.0 * stdev * stdev))/(2*stdev*stdev*Math.PI);
				originalConvolutionMatrixSum+=convolutionMatrix[x + filterRadius][y + filterRadius];
			}
		}
		for (int x = -filterRadius; x <= filterRadius; x++) {
			for (int y = -filterRadius; y <= filterRadius; y++) {
				convolutionMatrix[x + filterRadius][y + filterRadius] = convolutionMatrix[x + filterRadius][y + filterRadius] / originalConvolutionMatrixSum;
			}
		}
	}

}
