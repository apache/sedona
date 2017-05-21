/**
 * FILE: CircleTest.java
 * PATH: org.datasyslab.geospark.geometryObjects.CircleTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

// TODO: Auto-generated Javadoc
/**
 * The Class CircleTest.
 */
public class CircleTest {

   /** The geom fact. */
   public static GeometryFactory geomFact = new GeometryFactory();
	/**
     * Test get center.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetCenter() throws Exception {
    	
    	
        Circle circle = new Circle(geomFact.createPoint(new Coordinate(0.0,0.0)), 0.1);
        assertEquals(circle.getCenterPoint().x, 0.0, 0.01);
    }

    /**
     * Test get radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetRadius() throws Exception {
        Circle circle = new Circle(geomFact.createPoint(new Coordinate(0.0,0.0)), 0.1);
        assertEquals(circle.getRadius(), 0.1, 0.01);
    }

    /**
     * Test set radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSetRadius() throws Exception {
        Circle circle = new Circle(geomFact.createPoint(new Coordinate(0.0,0.0)), 0.1);
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
        Circle circle = new Circle(geomFact.createPoint(new Coordinate(0.0,0.0)), 0.1);

        assertEquals(circle.getEnvelopeInternal().getMinX(), circle.getCenterPoint().x - circle.getRadius(), 0.01);
    }

    /**
     * Test contains.
     *
     * @throws Exception the exception
     */
    @Test
    public void testContains() throws Exception {
        Circle circle = new Circle(geomFact.createPoint(new Coordinate(0.0,0.0)), 0.1);
        GeometryFactory geometryFactory = new GeometryFactory();
        assertEquals(true, circle.covers(geometryFactory.createPoint(new Coordinate(0.0, 0.0))));
    }

    /**
     * Test intersects.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIntersects() throws Exception {
        Circle circle = new Circle(geomFact.createPoint(new Coordinate(0.0,0.0)), 0.1);
        Envelope envelope = new Envelope(-0.1, 0.1, -0.1, 0.1);
        assertEquals(true, circle.getEnvelopeInternal().covers(envelope));

        circle = new Circle(geomFact.createPoint(new Coordinate(-0.1,0.0)), 0.1);
        envelope = new Envelope(-0.1, 0.1, -0.1, 0.1);
        assertEquals(true, circle.getEnvelopeInternal().intersects(envelope));

        circle = new Circle(geomFact.createPoint(new Coordinate(-0.3,0.0)), 0.1);
        envelope = new Envelope(-0.1, 0.1, -0.1, 0.1);
        assertEquals(false, circle.getEnvelopeInternal().intersects(envelope));
    }

    /**
     * Test intersects real data.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIntersectsRealData() throws Exception {
        Circle circle = new Circle(geomFact.createPoint(new Coordinate(-112.574945, 45.987772)), 0.01);
        Envelope envelope = new Envelope(-158.104182, -65.649956, 17.982169, 48.803593);
        assertEquals(true, circle.getEnvelopeInternal().intersects(envelope));
    }
    
    /**
     * Test equality.
     */
    @Test
    public void testEquality()
    {
        Circle circle1 = new Circle(geomFact.createPoint(new Coordinate(-112.574945, 45.987772)), 0.01);
        Circle circle2 = new Circle(geomFact.createPoint(new Coordinate(-112.574945, 45.987772)), 0.01);
        Circle circle3 = new Circle(geomFact.createPoint(new Coordinate(-112.574945, 45.987772)), 0.01);
        Circle circle4 = new Circle(geomFact.createPoint(new Coordinate(-112.574945, 45.987772)), 0.01);
        Circle circle5 = new Circle(geomFact.createPoint(new Coordinate(-112.574945, 45.987772)), 0.01);
        Circle circle6 = new Circle(geomFact.createPoint(new Coordinate(-112.574942, 45.987772)), 0.01);
        HashSet<Circle> result = new HashSet<Circle>();
        result.add(circle1);
        result.add(circle2);
        result.add(circle3);
        result.add(circle4);
        result.add(circle5);
        result.add(circle6);
        assert result.size()==2;
    }
}