/*
 * FILE: Pixel
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
package org.datasyslab.geosparkviz.utils;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Class Pixel.
 */
public class Pixel
        implements Serializable
{

    /**
     * The x.
     */
    private int x;

    /**
     * The y.
     */
    private int y;

    /**
     * The resolution X.
     */
    private int resolutionX;

    /**
     * The resolution Y.
     */
    private int resolutionY;

    /**
     * The is duplicate.
     */
    private boolean isDuplicate = false;

    /**
     * The current partition id.
     */
    private int currentPartitionId = -1;

    /**
     * Instantiates a new pixel.
     *
     * @param x the x
     * @param y the y
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param isDuplicate the is duplicate
     * @param currentPartitionId the current partition id
     */
    public Pixel(int x, int y, int resolutionX, int resolutionY, boolean isDuplicate, int currentPartitionId)
    {
        this.x = x;
        this.y = y;
        this.resolutionX = resolutionX;
        this.resolutionY = resolutionY;
        this.isDuplicate = isDuplicate;
        this.currentPartitionId = currentPartitionId;
    }

    /**
     * Instantiates a new pixel.
     *
     * @param x the x
     * @param y the y
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     */
    public Pixel(int x, int y, int resolutionX, int resolutionY)
    {
        this.x = x;
        this.y = y;
        this.resolutionX = resolutionX;
        this.resolutionY = resolutionY;
    }

    /**
     * Checks if is duplicate.
     *
     * @return true, if is duplicate
     */
    public boolean isDuplicate()
    {
        return isDuplicate;
    }

    /**
     * Sets the duplicate.
     *
     * @param duplicate the new duplicate
     */
    public void setDuplicate(boolean duplicate)
    {
        isDuplicate = duplicate;
    }

    /**
     * Gets the current partition id.
     *
     * @return the current partition id
     */
    public int getCurrentPartitionId()
    {
        return currentPartitionId;
    }

    /**
     * Sets the current partition id.
     *
     * @param currentPartitionId the new current partition id
     */
    public void setCurrentPartitionId(int currentPartitionId)
    {
        this.currentPartitionId = currentPartitionId;
    }

    /**
     * Gets the x.
     *
     * @return the x
     */
    public int getX()
    {
        return x;
    }

    /**
     * Gets the y.
     *
     * @return the y
     */
    public int getY()
    {
        return y;
    }

    public int getResolutionX()
    {
        return resolutionX;
    }

    public int getResolutionY()
    {
        return resolutionY;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o)
    {

        /*
        Pixel anotherObject = (Pixel) o;
        if(this.hashCode()==anotherObject.hashCode()&&this.getCurrentPartitionId()==anotherObject.getCurrentPartitionId()&&this.isDuplicate()==anotherObject.isDuplicate())
        {
            return true;
        }
        else return false;
        */
        return this.hashCode() == o.hashCode();
    }

    @Override
    public String toString()
    {
        return "Pixel(" +
                "x=" + x +
                ", y=" + y +
                ", width=" + resolutionX +
                ", height=" + resolutionY +
                ", isDuplicate=" + isDuplicate +
                ", tileId=" + currentPartitionId +
                ')';
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        /*
        int result = 17;
        result = 31 * result + this.getX();
        result = 31 * result + this.getY();
        //result = 31 * result + this.getCurrentPartitionId();
        return result;
        */
        int id = -1;
        try {
            id = RasterizationUtils.Encode2DTo1DId(resolutionX, resolutionY, x, y);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }
}
