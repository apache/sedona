/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.viz.core;

// TODO: Auto-generated Javadoc

/**
 * The Class PhotoFilter.
 */
public abstract class PhotoFilter
{

    /**
     * The filter radius.
     */
    protected int filterRadius;

    /**
     * The convolution matrix.
     */
    protected Double[][] convolutionMatrix;

    /**
     * Instantiates a new photo filter.
     *
     * @param filterRadius the filter radius
     */
    public PhotoFilter(int filterRadius)
    {
        this.filterRadius = filterRadius;
        this.convolutionMatrix = new Double[2 * filterRadius + 1][2 * filterRadius + 1];
    }

    /**
     * Gets the filter radius.
     *
     * @return the filter radius
     */
    public int getFilterRadius()
    {
        return filterRadius;
    }

    /**
     * Gets the convolution matrix.
     *
     * @return the convolution matrix
     */
    public Double[][] getConvolutionMatrix()
    {
        return convolutionMatrix;
    }
}
