/*
 * FILE: SpatioTemporalBoundaryAggregationTest
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
package org.datasyslab.geospark.spatioTemporal.RDD;

import static org.junit.Assert.assertEquals;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.datasyslab.geospark.spatioTemporalRddTool.SaptioTemporalStatCalculator;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

/**
 * The Class SpatioTemporalBoundaryAggregationTest.
 */
public class SpatioTemporalBoundaryAggregationTest
{

    private final GeometryFactory factory = new GeometryFactory();

    @Test
    public void testAddPoints()
            throws Exception
    {
        Cube agg = null;

        agg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(0, 0));
        assertEquals(new Cube(0, 0, 0, 0, 0, 0), agg);

        agg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(0, 1));
        assertEquals(new Cube(0, 0, 0, 1, 0, 0), agg);

        agg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(1, 1));
        assertEquals(new Cube(0, 1, 0, 1, 0, 0), agg);

        // Add point inside
        {
            Cube newAgg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(0.5, 0.7));
            assertEquals(new Cube(0, 1, 0, 1, 0, 0), newAgg);
        }

        // Add point on the border
        {
            Cube newAgg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(0.5, 0));
            assertEquals(new Cube(0, 1, 0, 1, 0, 0), newAgg);
        }

        // Add point outside, on the right
        {
            Cube newAgg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(1.5, 0.2));
            assertEquals(new Cube(0, 1.5, 0, 1, 0, 0), newAgg);
        }

        // Add point outside, on the top
        {
            Cube newAgg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(0.5, 1.2));
            assertEquals(new Cube(0, 1, 0, 1.2, 0, 0), newAgg);
        }

        // Add point outside, on the left and on the bottom
        {
            Cube newAgg = SaptioTemporalStatCalculator.add(agg, makeSTPoint(-4, -1));
            assertEquals(new Cube(-4, 1, -1, 1, 0, 0), newAgg);
        }
    }

    @Test
    public void testCombine()
            throws Exception
    {
        Cube agg = new Cube(0, 1, 0, 1, 0, 0);
        agg = SaptioTemporalStatCalculator.combine(null, agg);
        assertEquals(new Cube(0, 1, 0, 1, 0, 0), agg);

        // Add inner rectangle
        {
            Cube newAgg = SaptioTemporalStatCalculator.combine(agg, new Cube(0.1, 0.5, 0.3, 0.8, 0, 0));
            assertEquals(new Cube(0, 1, 0, 1, 0, 0), newAgg);
        }

        // Add disjoint rectangle
        {
            Cube newAgg = SaptioTemporalStatCalculator.combine(agg, new Cube(2, 2.5, 3, 8, 0, 0));
            assertEquals(new Cube(0, 2.5, 0, 8, 0, 0), newAgg);
        }

        // Add a rectangle intersecting on the left side
        {
            Cube newAgg = SaptioTemporalStatCalculator.combine(agg, new Cube(-1, 0.5, 0.2, 0.4, 0, 0));
            assertEquals(new Cube(-1, 1, 0, 1, 0, 0), newAgg);
        }

        // Add a rectangle intersecting on the right side
        {
            Cube newAgg = SaptioTemporalStatCalculator.combine(agg, new Cube(0.7, 3.4, 0.1, 0.3, 0, 0));
            assertEquals(new Cube(0, 3.4, 0, 1, 0, 0), newAgg);
        }

        // Add a rectangle intersecting both top and bottom sides
        {
            Cube newAgg = SaptioTemporalStatCalculator.combine(agg, new Cube(0.1, 0.5, -1, 10, 0, 0));
            assertEquals(new Cube(0, 1, -1, 10, 0, 0), newAgg);
        }

        // Add containing rectangle
        {
            Cube newAgg = SaptioTemporalStatCalculator.combine(agg, new Cube(-1, 2, -0.3, 5, 0, 0));
            assertEquals(new Cube(-1, 2, -0.3, 5, 0, 0), newAgg);
        }
    }

    private Point3D makeSTPoint(double x, double y)
    {
        Point point = factory.createPoint(new Coordinate(x, y));
        Point3D point3D = new Point3D(point, 0);
        return point3D;
    }

}
