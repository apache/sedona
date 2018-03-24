/**
 * FILE: BoundaryAndCountAggregationTest.java
 * PATH: org.datasyslab.geospark.spatialRDD.BoundaryAndCountAggregationTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geospark.spatialRddTool.StatCalculator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BoundaryAndCountAggregationTest
{

    private final GeometryFactory factory = new GeometryFactory();

    @Test
    public void testAdd()
            throws Exception
    {
        StatCalculator agg = null;

        agg = StatCalculator.add(agg, makePoint(0, 1));
        assertEquals(1, agg.getCount());
        assertEquals(new Envelope(0, 0, 1, 1), agg.getBoundary());

        agg = StatCalculator.add(agg, makePoint(0, 1));
        assertEquals(2, agg.getCount());
        assertEquals(new Envelope(0, 0, 1, 1), agg.getBoundary());

        agg = StatCalculator.add(agg, makePoint(1, 2));
        assertEquals(3, agg.getCount());
        assertEquals(new Envelope(0, 1, 1, 2), agg.getBoundary());

        agg = StatCalculator.add(agg, makePoint(.5, 1.2));
        assertEquals(4, agg.getCount());
        assertEquals(new Envelope(0, 1, 1, 2), agg.getBoundary());
    }

    @Test
    public void testCombine()
            throws Exception
    {
        StatCalculator agg = StatCalculator.combine(null, new StatCalculator(new Envelope(0, 1, 0, 1), 10));
        assertEquals(10, agg.getCount());
        assertEquals(new Envelope(0, 1, 0, 1), agg.getBoundary());

        {
            StatCalculator newAgg = StatCalculator.combine(agg, new StatCalculator(new Envelope(0.4, 1.2, 0.5, 1.7), 5));
            assertEquals(15, newAgg.getCount());
            assertEquals(new Envelope(0, 1.2, 0, 1.7), newAgg.getBoundary());
        }

        {
            StatCalculator newAgg = StatCalculator.combine(agg, new StatCalculator(new Envelope(0.1, 0.5, 0.2, 0.8), 3));
            assertEquals(13, newAgg.getCount());
            assertEquals(new Envelope(0, 1, 0, 1), newAgg.getBoundary());
        }
    }

    private Point makePoint(double x, double y)
    {
        return factory.createPoint(new Coordinate(x, y));
    }
}
