/*
 * FILE: HalfOpenRectangle
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
package org.datasyslab.geospark.utils;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;

public class HalfOpenCube
{
    private final Cube envelope;

    public HalfOpenCube(Cube envelope)
    {
        this.envelope = envelope;
    }

    public boolean contains(Point3D point)
    {
        return contains(point.getX(), point.getY(), point.getZ());
    }

    public boolean contains(double x, double y, double z)
    {
        return x >= envelope.getMinX() && x < envelope.getMaxX()
                && y >= envelope.getMinY() && y < envelope.getMaxY()
                && z >= envelope.getMinZ() && z < envelope.getMaxZ();
    }

    public Cube getEnvelope()
    {
        return envelope;
    }
}
