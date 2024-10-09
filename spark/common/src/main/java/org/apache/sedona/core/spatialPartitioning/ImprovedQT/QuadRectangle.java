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

package org.apache.sedona.core.spatialPartitioning.ImprovedQT;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;

public class QuadRectangle
        implements Serializable
{
    public final double x, y, width, height;
    public double x1, y1;
    public Integer gridX, gridY, xStep,yStep;
    public Integer partitionId = null;
    public String lineage = null;

    public QuadRectangle(Envelope envelope)
    {
        this.x = Math.ceil(envelope.getMinX()*10000)/10000.0;
        this.y = Math.ceil(envelope.getMinY()*10000)/10000.0;
        this.x1=Math.ceil(envelope.getMaxX()*10000)/10000.0;
        this.y1=Math.ceil(envelope.getMaxY()*10000)/10000.0;
        this.width = Math.ceil(envelope.getWidth()*10000)/10000.0;
        this.height = Math.ceil(envelope.getHeight()*10000)/10000.0;
    }

    public QuadRectangle(Envelope envelope, Integer gridX, Integer gridY, Integer xStep,Integer yStep)
    {
        this.x = Math.ceil(envelope.getMinX()*10000)/10000.0;
        this.y = Math.ceil(envelope.getMinY()*10000)/10000.0;
        this.width = Math.ceil(envelope.getWidth()*10000)/10000.0;
        this.height = Math.ceil(envelope.getHeight()*10000)/10000.0;
        this.x1=Math.ceil(envelope.getMaxX()*10000)/10000.0;
        this.y1=Math.ceil(envelope.getMaxY()*10000)/10000.0;
        this.gridX=gridX;
        this.gridY=gridY;
        this.xStep=xStep;
        this.yStep=yStep;
//        if(envelope.getMaxY()==21.6) System.out.println("y: "+this.y+"width:"+this.height);

    }

    public QuadRectangle(double x, double y, double width, double height)
    {
        if (width < 0) {
            throw new IllegalArgumentException("width must be >= 0");
        }

        if (height < 0) {
            throw new IllegalArgumentException("height must be >= 0");
        }

        this.x = Math.ceil(x*10000)/10000.0;
        this.y = Math.ceil(y*10000)/10000.0;
        this.width = Math.ceil(width*10000)/10000.0;
        this.height = Math.ceil(height*10000)/10000.0;
        this.x1=Math.ceil((x+width)*10000)/10000.0;
        this.y1=Math.ceil((y+height)*10000)/10000.0;
    }

    public QuadRectangle(double x, double y, double width, double height, Integer gridX, Integer gridY, Integer xStep,Integer yStep)
    {
        if (width < 0) {
            throw new IllegalArgumentException("width must be >= 0");
        }

        if (height < 0) {
            throw new IllegalArgumentException("height must be >= 0");
        }

        this.x = Math.ceil(x*10000)/10000.0;
        this.y = Math.ceil(y*10000)/10000.0;
        this.width =Math.ceil(width*10000)/10000.0;
        this.height = Math.ceil(height*10000)/10000.0;
        this.x1=Math.ceil((x+width)*10000)/10000.0;
        this.y1=Math.ceil((y+height)*10000)/10000.0;
        this.gridX=gridX;
        this.gridY=gridY;
        this.xStep=xStep;
        this.yStep=yStep;
    }

    public boolean contains(double x, double y)
    {
        return x >= this.x && x <= this.x + this.width
                && y >= this.y && y <= this.y + this.height;
    }

    public boolean contains(QuadRectangle r)
    {
//        return r.x >= this.x && r.x + r.width <= this.x + this.width
//                && r.y >= this.y && r.y + r.height <= this.y + this.height;
        return r.x >= this.x && r.x1 <= this.x1
                && r.y >= this.y && r.y1 <= this.y1;
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
        return new Envelope(x, x1, y, y1);
    }

    @Override
    public String toString()
    {
        return "x: " + x + " y: " + y + " x1: " + x1 + " y1: " + y1 + " PartitionId: " + partitionId;
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
