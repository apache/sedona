package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import java.io.Serializable;

/**
 * Created by zongsizhang on 7/6/17.
 */
public class BoundBox implements Serializable{

    /** bounds of 8 numbers. Xmin, Ymin, Xmax, Ymax, Zmin, Zmax, Mmin, Mmax */
    double[] bounds = null;

    /**
     * construct bounds with an array
     * @param bounds
     */
    public BoundBox(double[] bounds) {
        this.bounds = bounds;
    }

    /**
     * construct a initial boundBox with all value 0
     */
    public BoundBox() {
        bounds = new double[8];
    }

    /**
     * set value at i with value
     * @param i
     * @param value
     */
    public void set(int i, double value){
        bounds[i] = value;
    }

    public double getXMin(){
        return bounds[0];
    }

    public double getXMax(){
        return bounds[2];
    }

    public double getYMin(){
        return bounds[1];
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
}
