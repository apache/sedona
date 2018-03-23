/*
 * FILE: GeometryDistanceComparator
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
package org.datasyslab.geospark.knnJudgement;

import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Comparator;

// TODO: Auto-generated Javadoc

/**
 * The Class GeometryDistanceComparator.
 */
public class GeometryDistanceComparator<T extends Geometry>
        implements Comparator<T>, Serializable
{

    /**
     * The query center.
     */
    T queryCenter;

    /**
     * The normal order.
     */
    boolean normalOrder;

    /**
     * Instantiates a new geometry distance comparator.
     *
     * @param queryCenter the query center
     * @param normalOrder the normal order
     */
    public GeometryDistanceComparator(T queryCenter, boolean normalOrder)
    {
        this.queryCenter = queryCenter;
        this.normalOrder = normalOrder;
    }

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(T p1, T p2)
    {
        double distance1 = (p1).distance(queryCenter);
        double distance2 = (p2).distance(queryCenter);
        if (this.normalOrder) {
            if (distance1 > distance2) {
                return 1;
            }
            else if (distance1 == distance2) {
                return 0;
            }
            return -1;
        }
        else {
            if (distance1 > distance2) {
                return -1;
            }
            else if (distance1 == distance2) {
                return 0;
            }
            return 1;
        }
    }
}
