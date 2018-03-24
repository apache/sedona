/*
 * FILE: BoundaryAggregationTest
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
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.datasyslab.geospark.spatialRddTool.StatCalculator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BoundaryAggregationTest
{

    private final GeometryFactory factory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader();

    @Test
    public void testAddPoints()
            throws Exception
    {
        Envelope agg = null;

        agg = StatCalculator.add(agg, makePoint(0, 0));
        assertEquals(new Envelope(0, 0, 0, 0), agg);

        agg = StatCalculator.add(agg, makePoint(0, 1));
        assertEquals(new Envelope(0, 0, 0, 1), agg);

        agg = StatCalculator.add(agg, makePoint(1, 1));
        assertEquals(new Envelope(0, 1, 0, 1), agg);

        // Add point inside
        {
            Envelope newAgg = StatCalculator.add(agg, makePoint(0.5, 0.7));
            assertEquals(new Envelope(0, 1, 0, 1), newAgg);
        }

        // Add point on the border
        {
            Envelope newAgg = StatCalculator.add(agg, makePoint(0.5, 0));
            assertEquals(new Envelope(0, 1, 0, 1), newAgg);
        }

        // Add point outside, on the right
        {
            Envelope newAgg = StatCalculator.add(agg, makePoint(1.5, 0.2));
            assertEquals(new Envelope(0, 1.5, 0, 1), newAgg);
        }

        // Add point outside, on the top
        {
            Envelope newAgg = StatCalculator.add(agg, makePoint(0.5, 1.2));
            assertEquals(new Envelope(0, 1, 0, 1.2), newAgg);
        }

        // Add point outside, on the left and on the bottom
        {
            Envelope newAgg = StatCalculator.add(agg, makePoint(-4, -1));
            assertEquals(new Envelope(-4, 1, -1, 1), newAgg);
        }
    }

    @Test
    public void testAddPolygons()
            throws Exception
    {
        Envelope agg = null;

        // Add a triangle
        agg = StatCalculator.add(agg, parseWkt("POLYGON ((0 0, 0 1, 1 1, 0 0))"));
        assertEquals(new Envelope(0, 1, 0, 1), agg);

        // Add inner polygon
        agg = StatCalculator.add(agg, parseWkt("POLYGON ((0.1 0.1, 0.1 0.7, 0.7 0.7, 0.1 0.1))"));
        assertEquals(new Envelope(0, 1, 0, 1), agg);

        // Add intersecting polygon
        {
            Envelope newAgg = StatCalculator.add(agg, parseWkt("POLYGON ((0.5 1, 1.2 2, 3 0.8, 1.1 0.4, 0.5 1))"));
            assertEquals(new Envelope(0, 3, 0, 2), newAgg);
        }

        // Add disjoint polygon
        {
            Envelope newAgg = StatCalculator.add(agg, parseWkt("POLYGON ((-2 -0.5, -1 0.5, -0.4 -1, -2 -0.5))"));
            assertEquals(new Envelope(-2, 1, -1, 1), newAgg);
        }

        // Add containing polygon
        {
            Envelope newAgg = StatCalculator.add(agg, parseWkt("POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))"));
            assertEquals(new Envelope(-1, 2, -1, 2), newAgg);
        }
    }

    @Test
    public void testCombine()
            throws Exception
    {
        Envelope agg = new Envelope(0, 1, 0, 1);
        agg = StatCalculator.combine(null, agg);
        assertEquals(new Envelope(0, 1, 0, 1), agg);

        // Add inner rectangle
        {
            Envelope newAgg = StatCalculator.combine(agg, new Envelope(0.1, 0.5, 0.3, 0.8));
            assertEquals(new Envelope(0, 1, 0, 1), newAgg);
        }

        // Add disjoint rectangle
        {
            Envelope newAgg = StatCalculator.combine(agg, new Envelope(2, 2.5, 3, 8));
            assertEquals(new Envelope(0, 2.5, 0, 8), newAgg);
        }

        // Add a rectangle intersecting on the left side
        {
            Envelope newAgg = StatCalculator.combine(agg, new Envelope(-1, 0.5, 0.2, 0.4));
            assertEquals(new Envelope(-1, 1, 0, 1), newAgg);
        }

        // Add a rectangle intersecting on the right side
        {
            Envelope newAgg = StatCalculator.combine(agg, new Envelope(0.7, 3.4, 0.1, 0.3));
            assertEquals(new Envelope(0, 3.4, 0, 1), newAgg);
        }

        // Add a rectangle intersecting both top and bottom sides
        {
            Envelope newAgg = StatCalculator.combine(agg, new Envelope(0.1, 0.5, -1, 10));
            assertEquals(new Envelope(0, 1, -1, 10), newAgg);
        }

        // Add containing rectangle
        {
            Envelope newAgg = StatCalculator.combine(agg, new Envelope(-1, 2, -0.3, 5));
            assertEquals(new Envelope(-1, 2, -0.3, 5), newAgg);
        }
    }

    private Point makePoint(double x, double y)
    {
        return factory.createPoint(new Coordinate(x, y));
    }

    private Geometry parseWkt(String wkt)
            throws ParseException
    {
        return wktReader.read(wkt);
    }
}
