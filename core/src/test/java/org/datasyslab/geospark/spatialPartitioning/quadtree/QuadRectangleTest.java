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
        QuadRectangle r1 = new QuadRectangle(0, 0, 10, 10);
        QuadRectangle r2 = new QuadRectangle(0, 0, 10, 10);

        // contains
        assertTrue(r1.contains(r2));

        // dont contains
        r2.width = 11;
        assertFalse(r1.contains(r2));
    }
}