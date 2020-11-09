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
package org.apache.sedona.core.spatialRDD;

import org.apache.sedona.core.spatialRddTool.StatCalculator;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

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
