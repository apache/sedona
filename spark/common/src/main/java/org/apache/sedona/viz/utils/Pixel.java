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
package org.apache.sedona.viz.utils;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;

// TODO: Auto-generated Javadoc

/**
 * The Class Pixel.
 */
public class Pixel
        extends Point
{

    /**
     * The resolution X.
     */
    private final int resolutionX;

    /**
     * The resolution Y.
     */
    private final int resolutionY;

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
    public Pixel(double x, double y, int resolutionX, int resolutionY, boolean isDuplicate, int currentPartitionId)
    {
        super(new Coordinate(x, y), new PrecisionModel(), Integer.parseInt("0"));
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
    public Pixel(double x, double y, int resolutionX, int resolutionY)
    {
        super(new Coordinate(x, y), new PrecisionModel(), Integer.parseInt("0"));
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
        if (o == null || !(o instanceof Pixel)) { return false; }
        return this.hashCode() == o.hashCode();
    }

    @Override
    public String toString()
    {
        return "Pixel(" +
                "x=" + getX() +
                ", y=" + getY() +
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
        int id = -1;
        try {
            id = RasterizationUtils.Encode2DTo1DId(resolutionX, resolutionY, (int) getX(), (int) getY());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }
}
