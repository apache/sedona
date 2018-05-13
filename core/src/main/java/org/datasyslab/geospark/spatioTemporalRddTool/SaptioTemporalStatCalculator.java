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

package org.datasyslab.geospark.spatioTemporalRddTool;

import java.io.Serializable;
import java.util.Objects;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.SpatioTemporalObject;

public class SaptioTemporalStatCalculator
        implements Serializable
{
    private Cube boundary;
    private long count;

    public SaptioTemporalStatCalculator(Cube boundary, long count)
    {
        Objects.requireNonNull(boundary, "Boundary cannot be null");
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be > 0");
        }
        this.boundary = boundary;
        this.count = count;
    }

    public static SaptioTemporalStatCalculator combine(SaptioTemporalStatCalculator agg1, SaptioTemporalStatCalculator agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new SaptioTemporalStatCalculator(
                SaptioTemporalStatCalculator.combine(agg1.boundary, agg2.boundary),
                agg1.count + agg2.count);
    }

    public static Cube combine(Cube agg1, Cube agg2)
            throws Exception
    {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new Cube(
                Math.min(agg1.getMinX(), agg2.getMinX()),
                Math.max(agg1.getMaxX(), agg2.getMaxX()),
                Math.min(agg1.getMinY(), agg2.getMinY()),
                Math.max(agg1.getMaxY(), agg2.getMaxY()),
                Math.min(agg1.getMinZ(), agg2.getMinZ()),
                Math.max(agg1.getMaxZ(), agg2.getMaxZ()));
    }

    public static Cube add(Cube agg, SpatioTemporalObject object)
            throws Exception
    {
        return combine(object.getCubeInternal(), agg);
    }

    public static SaptioTemporalStatCalculator add(SaptioTemporalStatCalculator agg, SpatioTemporalObject object)
            throws Exception
    {
        return combine(new SaptioTemporalStatCalculator(object.getCubeInternal(), 1), agg);
    }

    public Cube getBoundary()
    {
        return boundary;
    }

    public long getCount()
    {
        return count;
    }
}
