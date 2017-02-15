/**
 * FILE: CircleTest.java
 * PATH: org.datasyslab.geospark.geometryObjects.CircleTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

// TODO: Auto-generated Javadoc
/**
 * The Class CircleTest.
 */
public class CircleTest {

    /**
     * Test get center.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetCenter() throws Exception {
        Circle circle = new Circle(0.0, 0.0, 0.1);
        assertEquals(circle.getCenter().getX(), 0.0, 0.01);
    }

    /**
     * Test get radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetRadius() throws Exception {
        Circle circle = new Circle(0.0, 0.0, 0.1);
        assertEquals(circle.getRadius(), 0.1, 0.01);
    }

    /**
     * Test set radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSetRadius() throws Exception {
        Circle circle = new Circle(0.0, 0.0, 0.1);
        circle.setRadius(0.2);
        assertEquals(circle.getRadius(), 0.2, 0.01);
    }

    /**
     * Test get MBR.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetMBR() throws Exception {
        Circle circle = new Circle(0.0, 0.0, 0.1);

        assertEquals(circle.getMBR().getMinX(), circle.getCenter().getX() - circle.getRadius(), 0.01);
    }

    /**
     * Test MB rto circle.
     *
     * @throws Exception the exception
     */
    @Test
    public void testMBRtoCircle() throws Exception {
        Envelope e = new Envelope(-0.1, 0.1, -0.1, 0.1);
        assertEquals(Circle.MBRtoCircle(e).getCenter().getX(), 0.0, 0.01);
    }

    /**
     * Test contains.
     *
     * @throws Exception the exception
     */
    @Test
    public void testContains() throws Exception {
        Circle circle = new Circle(0.0, 0.0, 0.1);
        GeometryFactory geometryFactory = new GeometryFactory();
        assertEquals(true, circle.contains(geometryFactory.createPoint(new Coordinate(0.0, 0.0))));
    }



    /**
     * Test intersects.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIntersects() throws Exception {

    }

    /**
     * Test intersects 1.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIntersects1() throws Exception {
        Circle circle = new Circle(0.0, 0.0, 0.1);
        Envelope envelope = new Envelope(-0.1, 0.1, -0.1, 0.1);
        assertEquals(true, circle.intersects(envelope));

        circle = new Circle(-0.1, 0.0, 0.1);
        envelope = new Envelope(-0.1, 0.1, -0.1, 0.1);
        assertEquals(true, circle.intersects(envelope));

        circle = new Circle(-0.3, 0.0, 0.1);
        envelope = new Envelope(-0.1, 0.1, -0.1, 0.1);
        assertEquals(false, circle.intersects(envelope));
    }

    /**
     * Test intersects real data.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIntersectsRealData() throws Exception {
        Circle circle = new Circle(-112.574945, 45.987772, 0.01);
        Envelope envelope = new Envelope(-158.104182, -65.649956, 17.982169, 48.803593);
        assertEquals(true, circle.intersects(envelope));
    }
}