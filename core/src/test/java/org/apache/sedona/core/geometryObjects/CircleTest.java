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
package org.apache.sedona.core.geometryObjects;

import org.apache.sedona.core.utils.GeomUtils;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    private static final GeometryFactory geomFact = new GeometryFactory();
    private static final WKTReader wktReader = new WKTReader();

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
        assertTrue(GeomUtils.equalsExactGeom(
                new Circle(makePoint(-112.574945, 45.987772), 0.01),
                new Circle(makePoint(-112.574945, 45.987772), 0.01)));

        assertFalse(GeomUtils.equalsExactGeom(
                new Circle(makePoint(-112.574945, 45.987772), 0.01),
                new Circle(makePoint(-112.574942, 45.987772), 0.01)));
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