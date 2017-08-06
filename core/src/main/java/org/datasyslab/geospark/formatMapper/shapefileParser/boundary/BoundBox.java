package org.datasyslab.geospark.formatMapper.shapefileParser.boundary;

import java.io.Serializable;
import java.util.Arrays;

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
        this.bounds = Arrays.copyOf(bounds, bounds.length);
    }

    /**
     * construct by copy other boundbox
     * @param otherbox
     */
    public BoundBox(BoundBox otherbox) {
        this.bounds = otherbox.copyBounds();
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

    public void setXMin(double value){
        bounds[0] = value;
    }

    public void setYMin(double value){
        bounds[0] = value;
    }

    public void setXMax(double value){
        bounds[0] = value;
    }

    public void setYMax(double value){
        bounds[0] = value;
    }

    public void setZMin(double value){
        bounds[0] = value;
    }

    public void setZMax(double value){
        bounds[0] = value;
    }

    public void setMMin(double value){
        bounds[0] = value;
    }

    public void setMMax(double value){
        bounds[0] = value;
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

    public double[] copyBounds(){
        return Arrays.copyOf(bounds, bounds.length);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0;i < bounds.length; ++i){
            strBuilder.append(bounds[i] + ", ");
        }
        return strBuilder.toString();
    }

    public static BoundBox mergeBoundBox(BoundBox box1, BoundBox box2){
        BoundBox box = new BoundBox();
        // merge X
        box.setXMin(Math.min(box1.getXMin(), box2.getXMin()));
        box.setXMax(Math.max(box1.getXMax(), box2.getXMax()));
        // merge Y
        box.setYMin(Math.min(box1.getYMin(), box2.getYMin()));
        box.setYMax(Math.max(box1.getYMax(), box2.getYMax()));
        // merge Z
        box.setZMin(Math.min(box1.getZMin(), box2.getZMin()));
        box.setZMax(Math.max(box1.getZMax(), box2.getZMax()));
        // merge M
        box.setMMin(Math.min(box1.getMMin(), box2.getMMin()));
        box.setMMax(Math.max(box1.getMMax(), box2.getMMax()));

        return box;
    }
}
