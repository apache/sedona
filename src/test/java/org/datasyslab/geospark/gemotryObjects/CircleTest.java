package org.datasyslab.geospark.gemotryObjects;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jinxuanwu on 1/8/16.
 */
public class CircleTest {

    @Test
    public void testGetCenter() throws Exception {

    }

    @Test
    public void testGetRadius() throws Exception {

    }

    @Test
    public void testSetRadius() throws Exception {

    }

    @Test
    public void testGetMBR() throws Exception {

    }

    @Test
    public void testMBRtoCircle() throws Exception {

    }

    @Test
    public void testContains() throws Exception {
        Circle circle = new Circle(0.0, 0.0, 0.1);
        GeometryFactory geometryFactory = new GeometryFactory();
        assertEquals(true, circle.contains(geometryFactory.createPoint(new Coordinate(0.0, 0.0))));
    }



    @Test
    public void testIntersects() throws Exception {

    }

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

    @Test
    public void testIntersectsRealData() throws Exception {
        Circle circle = new Circle(-112.574945, 45.987772, 0.01);
        Envelope envelope = new Envelope(-158.104182, -65.649956, 17.982169, 48.803593);
        assertEquals(true, circle.intersects(envelope));
    }
}