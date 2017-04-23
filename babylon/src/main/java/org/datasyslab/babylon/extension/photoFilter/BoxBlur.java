/**
 * FILE: BoxBlur.java
 * PATH: org.datasyslab.babylon.extension.photoFilter.BoxBlur.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.photoFilter;

import org.datasyslab.babylon.core.PhotoFilter;

/**
 * The Class BoxBlur.
 */
public class BoxBlur extends PhotoFilter{

	/**
	 * Instantiates a new box blur.
	 *
	 * @param filterRadius the filter radius
	 */
	public BoxBlur(int filterRadius) {
		super(filterRadius);
		for (int x = -filterRadius; x <= filterRadius; x++) {
			for (int y = -filterRadius; y <= filterRadius; y++) {
				convolutionMatrix[x + filterRadius][y + filterRadius] = 1.0/this.convolutionMatrix.length;
			}
		}
	}


}
