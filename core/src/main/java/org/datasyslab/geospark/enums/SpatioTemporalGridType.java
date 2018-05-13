/*
 * FILE: GridType
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
package org.datasyslab.geospark.enums;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Enum GridType.
 */
public enum SpatioTemporalGridType
        implements Serializable
{


    /**
     * The Octree.
     */
    OCTREE,

    /**
     * 3d R tree
     */
    RTREE3D,

    /**
     * EQUAL TIME SLICE
     */
    EQUALTIMESLICE,

    /**
     * EQUAL DATA SLICE
     */
    EQUALDATASLICE;


    /**
     * Gets the grid type.
     *
     * @param str the str
     * @return the grid type
     */
    public static SpatioTemporalGridType getGridType(String str)
    {
        for (SpatioTemporalGridType me : SpatioTemporalGridType.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }
}
