/*
 * FILE: GaussianBlur
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geosparkviz.extension.photoFilter;

import org.datasyslab.geosparkviz.core.PhotoFilter;

// TODO: Auto-generated Javadoc

/**
 * The Class GaussianBlur.
 */
public class GaussianBlur
        extends PhotoFilter
{

    /**
     * The stdev.
     */
    double stdev = 0.5;

    /**
     * Instantiates a new gaussian blur.
     *
     * @param blurRadius the blur radius
     */
    public GaussianBlur(int blurRadius)
    {
        super(blurRadius);
        double originalConvolutionMatrixSum = 0.0;
        for (int x = -filterRadius; x <= filterRadius; x++) {
            for (int y = -filterRadius; y <= filterRadius; y++) {
                convolutionMatrix[x + filterRadius][y + filterRadius] = Math.exp(-(x * x + y * y) / (2.0 * stdev * stdev)) / (2 * stdev * stdev * Math.PI);
                originalConvolutionMatrixSum += convolutionMatrix[x + filterRadius][y + filterRadius];
            }
        }
        for (int x = -filterRadius; x <= filterRadius; x++) {
            for (int y = -filterRadius; y <= filterRadius; y++) {
                convolutionMatrix[x + filterRadius][y + filterRadius] = convolutionMatrix[x + filterRadius][y + filterRadius] / originalConvolutionMatrixSum;
            }
        }
    }
}
