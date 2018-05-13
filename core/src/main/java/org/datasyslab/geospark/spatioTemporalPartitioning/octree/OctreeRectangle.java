/*
 * FILE: OctreeRectangle
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
package org.datasyslab.geospark.spatioTemporalPartitioning.octree;

import java.io.Serializable;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;

public class OctreeRectangle
        implements Serializable
{
    public final double x, y, width, length;
    public final double z, height;
    public Integer partitionId = -1;

    public OctreeRectangle(Cube cube)
    {
        this.x = cube.getMinX();
        this.y = cube.getMinY();
        this.z = cube.getMinZ();
        this.width = cube.getWidth();
        this.length = cube.getLength();
        this.height = cube.getHeight();
    }

    public OctreeRectangle(double x, double y, double z, double width, double length, double height)
    {
        if (width < 0) {
            throw new IllegalArgumentException("width must be >= 0");
        }

        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }

        if (height < 0) {
            throw new IllegalArgumentException("height must be >= 0");
        }

        this.x = x;
        this.y = y;
        this.z = z;
        this.width = width;
        this.length = length;
        this.height = height;
    }

    public boolean contains(double x, double y, double z)
    {
        return x >= this.x && x <= this.x + this.width
                && y >= this.y && y <= this.y + this.length
                && z >= this.z && z <= this.z + this.height;
    }

    public boolean contains(OctreeRectangle r)
    {
        return r.x >= this.x && r.x + r.width <= this.x + this.width
                && r.y >= this.y && r.y + r.length <= this.y + this.length
                && r.z >= this.z && r.z + r.height <= this.z + this.height;
    }

    public int getUniqueId()
    {
        return hashCode();
    }

    public Cube getSTEnvelope()
    {
        return new Cube(x, x + width, y, y + length, z, z + height);
    }

    @Override
    public String toString()
    {
        return "x: " + x + " y: " + y + " z: " + z + " w: " + width + " l: " + length + " h: " + height + " " + "PartitionId: " + partitionId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof OctreeRectangle)) {
            return false;
        }

        final OctreeRectangle other = (OctreeRectangle) o;
        return this.x == other.x && this.y == other.y
                && this.width == other.width && this.height == other.height
                && this.z == other.z && this.length == other.length
                && this.partitionId == other.partitionId;
    }

    @Override
    public int hashCode()
    {
        String stringId = "" + x + y + width + length + z + height;
        return stringId.hashCode();
    }
}
