package org.apache.sedona.common.subDivide;

import org.locationtech.jts.geom.Envelope;

public class SubDivideExtent {
    private double xMin;
    private double xMax;
    private double yMin;
    private double yMax;

    public SubDivideExtent(double xMin, double xMax, double yMin, double yMax) {
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
    }

    public SubDivideExtent(Envelope clip) {
        this.xMin = clip.getMinX();
        this.xMax = clip.getMaxX();
        this.yMin = clip.getMinY();
        this.yMax = clip.getMaxY();
    }

    public SubDivideExtent copy() {
        return new SubDivideExtent(this.xMin, this.xMax, this.yMin, this.yMax);
    }

    public double getxMin() {
        return xMin;
    }

    public SubDivideExtent setxMin(double xMin) {
        this.xMin = xMin;
        return this;
    }

    public double getxMax() {
        return xMax;
    }

    public SubDivideExtent setxMax(double xMax) {
        this.xMax = xMax;
        return this;
    }

    public double getyMin() {
        return yMin;
    }

    public SubDivideExtent setyMin(double yMin) {
        this.yMin = yMin;
        return this;
    }

    public double getyMax() {
        return yMax;
    }

    public SubDivideExtent setyMax(double yMax) {
        this.yMax = yMax;
        return this;
    }
}
