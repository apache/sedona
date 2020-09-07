/*
 * FILE: CircleTest
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
package org.datasyslab.geospark.geometryObjects;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

// TODO: Auto-generated Javadoc

/**
 * The Class CircleTest.
 */
public class CircleTest
{
    /**
     * The geom fact.
     */
    private static GeometryFactory geomFact = new GeometryFactory();
    private static WKTReader wktReader = new WKTReader();

    /**
     * Test get center.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetCenter()
            throws Exception
    {
        Circle circle = new Circle(makePoint(0.0, 0.0), 0.1);
        assertEquals(makePoint(0.0, 0.0).getCoordinate(), circle.getCenterPoint());
    }

    /**
     * Test get radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetRadius()
            throws Exception
    {
        Circle circle = new Circle(makePoint(0.0, 0.0), 0.1);
        assertEquals(0.1, circle.getRadius(), 0.01);
    }

    /**
     * Test set radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSetRadius()
            throws Exception
    {
        Circle circle = new Circle(makePoint(0.0, 0.0), 0.1);
        circle.setRadius(0.2);
        assertEquals(circle.getRadius(), 0.2, 0.01);
    }

    /**
     * Test get MBR.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetEnvelopeInternal()
            throws Exception
    {
        Circle circle = new Circle(makePoint(0.0, 0.0), 0.1);
        assertEquals(new Envelope(-0.1, 0.1, -0.1, 0.1), circle.getEnvelopeInternal());
    }

    /**
     * Test contains.
     *
     * @throws Exception the exception
     */
    @Test
    public void testCovers()
            throws Exception
    {
        Circle circle = new Circle(makePoint(0.0, 0.0), 0.5);

        assertTrue(circle.covers(makePoint(0.0, 0.0)));
        assertTrue(circle.covers(makePoint(0.1, 0.2)));
        assertFalse(circle.covers(makePoint(0.4, 0.4)));
        assertFalse(circle.covers(makePoint(-1, 0.4)));

        assertTrue(circle.covers(parseWkt("MULTIPOINT ((0.1 0.1), (0.2 0.4))")));
        assertFalse(circle.covers(parseWkt("MULTIPOINT ((0.1 0.1), (1.2 0.4))")));
        assertFalse(circle.covers(parseWkt("MULTIPOINT ((1.1 0.1), (0.2 1.4))")));

        assertTrue(circle.covers(parseWkt("POLYGON ((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1))")));
        assertTrue(circle.covers(parseWkt("POLYGON ((-0.5 0, 0 0.5, 0.5 0, -0.5 0))")));
        assertFalse(circle.covers(parseWkt("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")));
        assertFalse(circle.covers(parseWkt("POLYGON ((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4))")));

        assertTrue(circle.covers(parseWkt("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)), " +
                "((-0.5 0, 0 0.5, 0.5 0, -0.5 0)))")));
        assertFalse(circle.covers(parseWkt("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)), " +
                "((0 0, 0 1, 1 1, 1 0, 0 0)))")));
        assertFalse(circle.covers(parseWkt("MULTIPOLYGON (((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4)), " +
                "((0 0, 0 1, 1 1, 1 0, 0 0)))")));

        assertTrue(circle.covers(parseWkt("LINESTRING (-0.1 0, 0.2 0.3)")));
        assertTrue(circle.covers(parseWkt("LINESTRING (-0.5 0, 0 0.5, 0.5 0)")));
        assertFalse(circle.covers(parseWkt("LINESTRING (-0.1 0, 0 1)")));
        assertFalse(circle.covers(parseWkt("LINESTRING (0.4 0.4, 0.45 0.45)")));

        assertTrue(circle.covers(parseWkt("MULTILINESTRING ((-0.1 0, 0.2 0.3), (-0.5 0, 0 0.5, 0.5 0))")));
        assertFalse(circle.covers(parseWkt("MULTILINESTRING ((-0.1 0, 0.2 0.3), (-0.1 0, 0 1))")));
        assertFalse(circle.covers(parseWkt("MULTILINESTRING ((0.4 0.4, 0.45 0.45), (-0.1 0, 0 1))")));
    }

    /**
     * Test intersects.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIntersects()
            throws Exception
    {
        Circle circle = new Circle(makePoint(0.0, 0.0), 0.5);
        assertTrue(circle.intersects(makePoint(0, 0)));
        assertTrue(circle.intersects(makePoint(0.1, 0.2)));
        assertFalse(circle.intersects(makePoint(0.4, 0.4)));
        assertFalse(circle.intersects(makePoint(-1, 0.4)));

        assertTrue(circle.intersects(parseWkt("MULTIPOINT ((0.1 0.1), (0.2 0.4))")));
        assertTrue(circle.intersects(parseWkt("MULTIPOINT ((0.1 0.1), (1.2 0.4))")));
        assertFalse(circle.intersects(parseWkt("MULTIPOINT ((1.1 0.1), (0.2 1.4))")));

        // Polygon is fully contained within the circle
        assertTrue(circle.intersects(parseWkt("POLYGON ((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1))")));
        assertTrue(circle.intersects(parseWkt("POLYGON ((-0.5 0, 0 0.5, 0.5 0, -0.5 0))")));
        // Polygon boundary intersects the circle
        assertTrue(circle.intersects(parseWkt("POLYGON ((0 0, 1 1, 1 0, 0 0))")));
        // Polygon contains the circle
        assertTrue(circle.intersects(parseWkt("POLYGON ((-1 -1, -1 1, 1 1, 1.5 0.5, 1 -1, -1 -1))")));
        // Polygon with a hole intersects the circle, but doesn't contain circle center
        assertTrue(circle.intersects(parseWkt("POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1), " +
                "(-0.1 -0.1, 0.1 -0.1, 0.1 0.1, -0.1 0.1, -0.1 -0.1))")));

        // No intersection
        assertFalse(circle.intersects(parseWkt("POLYGON ((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4))")));
        assertFalse(circle.intersects(parseWkt("POLYGON ((-1 0, -1 1, 0 1, 0 2, -1 2, -1 0))")));
        assertFalse(circle.intersects(parseWkt("POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1), " +
                "(-0.6 -0.6, 0.6 -0.6, 0.6 0.6, -0.6 0.6, -0.6 -0.6))")));

        assertTrue(circle.intersects(parseWkt("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)), " +
                "((-0.5 0, 0 0.5, 0.5 0, -0.5 0)))")));
        assertTrue(circle.intersects(parseWkt("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)), " +
                "((-1 0, -1 1, 0 1, 0 2, -1 2, -1 0)))")));
        assertFalse(circle.intersects(parseWkt("MULTIPOLYGON (((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4)), " +
                "((-1 0, -1 1, 0 1, 0 2, -1 2, -1 0)))")));

        // Line intersects at 2 points
        assertTrue(circle.intersects(parseWkt("LINESTRING (-1 -1, 1 1)")));
        // Line intersects at one point
        assertTrue(circle.intersects(parseWkt("LINESTRING (-1 0.5, 1 0.5)")));
        // Line is fully within the circle
        assertTrue(circle.intersects(parseWkt("LINESTRING (0 0, 0.1 0.2)")));

        // No intersection
        assertFalse(circle.intersects(parseWkt("LINESTRING (0.4 0.4, 1 1)")));
        assertFalse(circle.intersects(parseWkt("LINESTRING (-0.4 -0.4, -2 -3.2)")));
        assertFalse(circle.intersects(parseWkt("LINESTRING (0.1 0.5, 1 0.5)")));

        assertTrue(circle.intersects(parseWkt("MULTILINESTRING ((-1 -1, 1 1), (-1 0.5, 1 0.5))")));
        assertTrue(circle.intersects(parseWkt("MULTILINESTRING ((-1 -1, 1 1), (0.4 0.4, 1 1))")));
        assertFalse(circle.intersects(parseWkt("MULTILINESTRING ((0.1 0.5, 1 0.5), (0.4 0.4, 1 1))")));
    }

    /**
     * Test equality.
     */
    @Test
    public void testEquality()
    {
        assertEquals(
                new Circle(makePoint(-112.574945, 45.987772), 0.01),
                new Circle(makePoint(-112.574945, 45.987772), 0.01));

        assertNotEquals(
                new Circle(makePoint(-112.574945, 45.987772), 0.01),
                new Circle(makePoint(-112.574942, 45.987772), 0.01));
    }

    private Point makePoint(double x, double y)
    {
        return geomFact.createPoint(new Coordinate(x, y));
    }

    private Geometry parseWkt(String wkt)
            throws ParseException
    {
        return wktReader.read(wkt);
    }
}