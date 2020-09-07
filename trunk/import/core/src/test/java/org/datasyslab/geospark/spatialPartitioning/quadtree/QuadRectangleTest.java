/*
 * FILE: QuadRectangleTest
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
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuadRectangleTest
{

    @Test
    public void testContains()
    {
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

    private QuadRectangle makeRect(double x, double y, double width, double height)
    {
        return new QuadRectangle(x, y, width, height);
    }
}