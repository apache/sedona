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
package org.apache.sedona.core.spatialPartitioning.quadtree;

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

        // contains rectangle
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