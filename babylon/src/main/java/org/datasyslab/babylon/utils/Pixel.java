package org.datasyslab.babylon.utils;

import java.io.Serializable;

public class Pixel implements Serializable{
    private int x;
    private int y;
    private int resolutionX;
    private int resolutionY;
    private boolean isDuplicate = false;
    private int currentPartitionId = -1;

    public Pixel(int x, int y, int resolutionX, int resolutionY, boolean isDuplicate, int currentPartitionId)
    {
        this.x = x;
        this.y = y;
        this.resolutionX=resolutionX;
        this.resolutionY=resolutionY;
        this.isDuplicate = isDuplicate;
        this.currentPartitionId = currentPartitionId;
    }

    public Pixel(int x, int y, int resolutionX, int resolutionY)
    {
        this.x = x;
        this.y = y;
        this.resolutionX=resolutionX;
        this.resolutionY=resolutionY;
    }
    
    public boolean isDuplicate() {
        return isDuplicate;
    }

    public void setDuplicate(boolean duplicate) {
        isDuplicate = duplicate;
    }

    public int getCurrentPartitionId() {
        return currentPartitionId;
    }

    public void setCurrentPartitionId(int currentPartitionId) {
        this.currentPartitionId = currentPartitionId;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }


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
