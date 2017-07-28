/**
 * FILE: BoundBox.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.BoundBox.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * Created by zongsizhang on 7/6/17.
 */
public class BoundBox implements Serializable{

    /** bounds of 8 numbers. Xmin, Ymin, Xmax, Ymax, Zmin, Zmax, Mmin, Mmax */
    double[] bounds = null;

    /**
     * construct bounds with an array.
     *
     * @param bounds the bounds
     */
    public BoundBox(double[] bounds) {
        this.bounds = bounds;
    }

    /**
     * construct a initial boundBox with all value 0.
     */
    public BoundBox() {
        bounds = new double[8];
    }

    /**
     * set value at i with value.
     *
     * @param i the i
     * @param value the value
     */
    public void set(int i, double value){
        bounds[i] = value;
    }

    /**
     * Gets the x min.
     *
     * @return the x min
     */
    public double getXMin(){
        return bounds[0];
    }

    /**
     * Gets the x max.
     *
     * @return the x max
     */
    public double getXMax(){
        return bounds[2];
    }

    /**
     * Gets the y min.
     *
     * @return the y min
     */
    public double getYMin(){
        return bounds[1];
    }

    /**
     * Gets the y max.
     *
     * @return the y max
     */
    public double getYMax(){
        return bounds[3];
    }

    /**
     * Gets the z min.
     *
     * @return the z min
     */
    public double getZMin(){
        return bounds[4];
    }

    /**
     * Gets the z max.
     *
     * @return the z max
     */
    public double getZMax(){
        return bounds[5];
    }

    /**
     * Gets the m min.
     *
     * @return the m min
     */
    public double getMMin(){
        return bounds[6];
    }

    /**
     * Gets the m max.
     *
     * @return the m max
     */
    public double getMMax(){
        return bounds[7];
    }
}
