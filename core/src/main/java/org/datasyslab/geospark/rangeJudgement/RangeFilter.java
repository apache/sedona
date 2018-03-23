/*
 * FILE: RangeFilter
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
package org.datasyslab.geospark.rangeJudgement;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.function.Function;

// TODO: Auto-generated Javadoc

public class RangeFilter<U extends Geometry, T extends Geometry>
        extends JudgementBase
        implements Function<T, Boolean>
{

    public RangeFilter(U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight)
    {
        super(queryWindow, considerBoundaryIntersection, leftCoveredByRight);
    }

    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    public Boolean call(T geometry)
            throws Exception
    {
        if (leftCoveredByRight) {
            return match(geometry, queryGeometry);
        }
        else {
            return match(queryGeometry, queryGeometry);
        }
    }
}
