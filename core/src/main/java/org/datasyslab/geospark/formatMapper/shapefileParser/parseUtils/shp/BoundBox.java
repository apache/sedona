package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import java.io.Serializable;

/**
 * Created by zongsizhang on 7/6/17.
 */
public class BoundBox implements Serializable {
    private double[] bounds;

    public BoundBox(double[] bounds) {
        this.bounds = bounds;
    }

    public BoundBox(){
        bounds = new double[8];
    }

    public void set(int i, double value){
        bounds[i] = value;
    }

    public double getXMin(){
        return bounds[0];
    }

    public double getYMin(){
        return bounds[1];
    }

    public double getXMax(){
        return bounds[2];
    }

    public double getYMax(){
        return bounds[3];
    }

    public double getZMin(){
        return bounds[4];
    }

    public double getZMax(){
        return bounds[5];
    }

    public double getMMin(){
        return bounds[6];
    }

    public double getMMax(){
        return bounds[7];
    }

    public String toString(){
        return java.util.Arrays.toString(bounds);
    }

}
