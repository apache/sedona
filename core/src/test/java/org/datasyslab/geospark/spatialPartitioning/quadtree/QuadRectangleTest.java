/**
 * FILE: QuadRectangleTest.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangleTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuadRectangleTest {

    @Test
    public void testContains() {
        QuadRectangle r1 = makeRect(0, 0, 10, 10);
        QuadRectangle r2 = makeRect(0, 0, 10, 10);

        // contains rectange
        assertTrue(r1.contains(r2));

        // contains point
        assertTrue(r1.contains(makeRect(5, 5, 0, 0)));

        // doesn't contain rectangle
        QuadRectangle r3 = makeRect(0, 0, 11, 10);
        assertFalse(r1.contains(r3));

        // doesn't contain point
        assertFalse(r1.contains(makeRect(5, 12, 0, 0)));
    }

    private QuadRectangle makeRect(double x, double y, double width, double height) {
        return new QuadRectangle(x, y, width, height);
    }
}