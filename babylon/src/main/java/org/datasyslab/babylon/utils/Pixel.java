/**
 * FILE: Pixel.java
 * PATH: org.datasyslab.babylon.utils.Pixel.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.utils;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class Pixel.
 */
public class Pixel implements Serializable{
    
    /** The x. */
    private int x;
    
    /** The y. */
    private int y;
    
    /** The resolution X. */
    private int resolutionX;
    
    /** The resolution Y. */
    private int resolutionY;
    
    /** The is duplicate. */
    private boolean isDuplicate = false;
    
    /** The current partition id. */
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
        this.resolutionX=resolutionX;
        this.resolutionY=resolutionY;
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
        this.resolutionX=resolutionX;
        this.resolutionY=resolutionY;
    }
    
    /**
     * Checks if is duplicate.
     *
     * @return true, if is duplicate
     */
    public boolean isDuplicate() {
        return isDuplicate;
    }

    /**
     * Sets the duplicate.
     *
     * @param duplicate the new duplicate
     */
    public void setDuplicate(boolean duplicate) {
        isDuplicate = duplicate;
    }

    /**
     * Gets the current partition id.
     *
     * @return the current partition id
     */
    public int getCurrentPartitionId() {
        return currentPartitionId;
    }

    /**
     * Sets the current partition id.
     *
     * @param currentPartitionId the new current partition id
     */
    public void setCurrentPartitionId(int currentPartitionId) {
        this.currentPartitionId = currentPartitionId;
    }

    /**
     * Gets the x.
     *
     * @return the x
     */
    public int getX() {
        return x;
    }

    /**
     * Gets the y.
     *
     * @return the y
     */
    public int getY() {
        return y;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        /*
        if (o == this) return true;
        if (!(o instanceof Pixel)) {
            return false;
        }
        Pixel pixel = (Pixel) o;
        return this.getX() == pixel.getX() && this.getY() == pixel.getY() && this.getCurrentPartitionId() == pixel.getCurrentPartitionId()
                && this.isDuplicate() == pixel.isDuplicate();
                */
        return this.hashCode() == o.hashCode();

    }



    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        /*
        int result = 17;
        result = 31 * result + this.getX();
        result = 31 * result + this.getY();
        //result = 31 * result + this.getCurrentPartitionId();
        return result;
        */
        int id = -1;
        try {
            id = RasterizationUtils.Encode2DTo1DId(resolutionX,resolutionY,x,y);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

}
