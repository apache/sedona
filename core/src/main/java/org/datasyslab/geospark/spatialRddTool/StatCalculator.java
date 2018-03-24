/*
 * FILE: StatCalculator
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

package org.datasyslab.geospark.spatialRddTool;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Objects;

public class StatCalculator
        implements Serializable
{
    private Envelope boundary;
    private long count;

    public StatCalculator(Envelope boundary, long count)
    {
        Objects.requireNonNull(boundary, "Boundary cannot be null");
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be > 0");
        }
        this.boundary = boundary;
        this.count = count;
    }

    public static StatCalculator combine(StatCalculator agg1, StatCalculator agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new StatCalculator(
                StatCalculator.combine(agg1.boundary, agg2.boundary),
                agg1.count + agg2.count);
    }

    public static Envelope combine(Envelope agg1, Envelope agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new Envelope(
                Math.min(agg1.getMinX(), agg2.getMinX()),
                Math.max(agg1.getMaxX(), agg2.getMaxX()),
                Math.min(agg1.getMinY(), agg2.getMinY()),
                Math.max(agg1.getMaxY(), agg2.getMaxY()));
    }

    public static Envelope add(Envelope agg, Geometry object)
            throws Exception
    {
        return combine(object.getEnvelopeInternal(), agg);
    }

    public static StatCalculator add(StatCalculator agg, Geometry object)
            throws Exception
    {
        return combine(new StatCalculator(object.getEnvelopeInternal(), 1), agg);
    }

    public Envelope getBoundary()
    {
        return boundary;
    }

    public long getCount()
    {
        return count;
    }
}
