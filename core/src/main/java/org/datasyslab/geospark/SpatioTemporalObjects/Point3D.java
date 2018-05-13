package org.datasyslab.geospark.SpatioTemporalObjects;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Point;

/**
 * The Class Point3D.
 */
public class Point3D extends SpatioTemporalObject
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(Point3D.class);

    public Point point;

    public double z;

    public Point3D(Point point, double z) {
        super(point, z, z);
        this.point = point;
        this.z = z;
    }

    public double getX() {
        return point.getX();
    }

    public double getY() {
        return point.getY();
    }

    public double getZ() {
        return z;
    }

}
