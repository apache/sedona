/*
 * FILE: QuadRectangle
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

import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;

public class QuadRectangle
        implements Serializable
{
    public final double x, y, width, height;
    public Integer partitionId = -1;

    public QuadRectangle(Envelope envelope)
    {
        this.x = envelope.getMinX();
        this.y = envelope.getMinY();
        this.width = envelope.getWidth();
        this.height = envelope.getHeight();
    }

    public QuadRectangle(double x, double y, double width, double height)
    {
        if (width < 0) {
            throw new IllegalArgumentException("width must be >= 0");
        }

        if (height < 0) {
            throw new IllegalArgumentException("height must be >= 0");
        }

        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
    }

    public boolean contains(double x, double y)
    {
        return x >= this.x && x <= this.x + this.width
                && y >= this.y && y <= this.y + this.height;
    }

    public boolean contains(QuadRectangle r)
    {
        return r.x >= this.x && r.x + r.width <= this.x + this.width
                && r.y >= this.y && r.y + r.height <= this.y + this.height;
    }
    /*
    public boolean contains(int x, int y) {
        return this.width > 0 && this.height > 0
                && x >= this.x && x <= this.x + this.width
                && y >= this.y && y <= this.y + this.height;
    }
    */

    public int getUniqueId()
    {
        /*
        Long uniqueId = Long.valueOf(-1);
        try {
            uniqueId = Long.valueOf(RasterizationUtils.Encode2DTo1DId(resolutionX,resolutionY,(int)this.x,(int)this.y));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uniqueId;
        */
        return hashCode();
    }

    public Envelope getEnvelope()
    {
        return new Envelope(x, x + width, y, y + height);
    }

    @Override
    public String toString()
    {
        return "x: " + x + " y: " + y + " w: " + width + " h: " + height + " PartitionId: " + partitionId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof QuadRectangle)) {
            return false;
        }

        final QuadRectangle other = (QuadRectangle) o;
        return this.x == other.x && this.y == other.y
                && this.width == other.width && this.height == other.height
                && this.partitionId == other.partitionId;
    }

    @Override
    public int hashCode()
    {
        String stringId = "" + x + y + width + height;
        return stringId.hashCode();
    }
}
