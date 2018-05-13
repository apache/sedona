package org.datasyslab.geospark.SpatioTemporalObjects;

import com.vividsolutions.jts.geom.Envelope;

/**
 * The Class Cube.
 */
public class Cube implements java.io.Serializable{

    public Envelope envelope;

    public double minz;

    public double maxz;


    /**
     * Instantiates a new Cube.
     *
     * @param envelope the spatial envelop
     * @param beginTime the temporal begin time
     * @param endTime the temporal end time
     */
    public Cube(Envelope envelope, double beginTime, double endTime) {
        this.envelope = envelope;
        this.minz = beginTime;
        this.maxz = endTime;
    }

    /**
     * Instantiates a new Cube.
     *
     * @param minx the minx
     * @param max the max
     * @param miny the minx
     * @param maxy the maxy
     * @param miniz the minz
     * @param maxz the maxz
     */
    public Cube(double minx, double maxx, double miny, double maxy, double minz, double maxz) {
        this.envelope = new Envelope(minx, maxx, miny, maxy);
        this.minz = minz;
        this.maxz = maxz;
    }

    /**
     * Gets the minx.
     *
     * @return the minx
     */
    public double getMinX() {
        return envelope.getMinX();
    }

    /**
     * Gets the maxx.
     *
     * @return the maxx
     */
    public double getMaxX() {
        return envelope.getMaxX();
    }

    /**
     * Gets the minY.
     *
     * @return the minY
     */
    public double getMinY() {
        return envelope.getMinY();
    }

    /**
     * Gets the maxY.
     *
     * @return the maxY
     */
    public double getMaxY() {
        return envelope.getMaxY();
    }

    /**
     * Gets the minz.
     *
     * @return the minz
     */
    public double getMinZ() {
        return minz;
    }

    /**
     * Gets the maxz.
     *
     * @return the maxz
     */
    public double getMaxZ() {
        return maxz;
    }

    /**
     * Gets the width.
     *
     * @return the width
     */
    public double getWidth() {
        return getMaxX() - getMinX();
    }

    /**
     * Gets the length.
     *
     * @return the length
     */
    public double getLength() {
        return getMaxY() - getMinY();
    }

    /**
     * Gets the height.
     *
     * @return the height
     */
    public double getHeight() {
        return maxz - minz;
    }

    public boolean intersects(Cube other) {

        if (this.envelope.covers(other.envelope)) {
            if ((other.maxz >= this.maxz && other.minz <= this.maxz)
                    || (other.minz<=this.minz && other.maxz>=this.minz)) {
                return true;
            }
        }

        if (other.envelope.covers(this.envelope)) {
            if ((this.maxz >= other.maxz && this.minz <= other.maxz)
                    || (this.minz <= other.minz && this.maxz >= other.minz)) {
                return true;
            }
        }

        if (this.envelope.intersects(other.envelope)) {
            if ((this.minz <= other.minz && this.maxz >= other.minz)
                    || (this.minz <= other.maxz && this.maxz >= other.maxz)
                    || (this.minz <= other.minz && this.maxz >= other.maxz)
                    || (this.minz >= other.minz && this.maxz <= other.maxz)) {
                return true;
            }
        }
        return false;
    }

    public boolean covers(Cube other) {
        if (this.envelope.covers(other.envelope)) {
            if (this.minz <= other.minz && this.maxz >= other.maxz) {
                return true;
            }
        }
        return false;
    }

    public boolean equals(Object o) {
        if (o instanceof Cube) {
            Cube cube = (Cube) o;
            if (this.envelope.equals(((Cube) o).envelope)
                    && this.minz == ((Cube) o).minz && this.maxz == ((Cube) o).maxz) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

}
