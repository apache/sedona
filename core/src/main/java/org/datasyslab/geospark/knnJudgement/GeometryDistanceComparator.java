/**
 * FILE: GeometryDistanceComparator.java
 * PATH: org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Comparator;

// TODO: Auto-generated Javadoc

/**
 * The Class GeometryDistanceComparator.
 */
public class GeometryDistanceComparator<T extends Geometry>
        implements Comparator<T>, Serializable
{

    /**
     * The query center.
     */
    T queryCenter;

    /**
     * The normal order.
     */
    boolean normalOrder;

    /**
     * Instantiates a new geometry distance comparator.
     *
     * @param queryCenter the query center
     * @param normalOrder the normal order
     */
    public GeometryDistanceComparator(T queryCenter, boolean normalOrder)
    {
        this.queryCenter = queryCenter;
        this.normalOrder = normalOrder;
    }

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(T p1, T p2)
    {
        double distance1 = (p1).distance(queryCenter);
        double distance2 = (p2).distance(queryCenter);
        if (this.normalOrder) {
            if (distance1 > distance2) {
                return 1;
            }
            else if (distance1 == distance2) {
                return 0;
            }
            return -1;
        }
        else {
            if (distance1 > distance2) {
                return -1;
            }
            else if (distance1 == distance2) {
                return 0;
            }
            return 1;
        }
    }
}
