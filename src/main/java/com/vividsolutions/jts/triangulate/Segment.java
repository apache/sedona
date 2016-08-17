/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */

package com.vividsolutions.jts.triangulate;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineSegment;

/**
 * Models a constraint segment in a triangulation.
 * A constraint segment is an oriented straight line segment between a start point
 * and an end point.
 * 
 * @author David Skea
 * @author Martin Davis
 */
public class Segment 
{
    private LineSegment ls;
    private Object data = null;

    /** 
     * Creates a new instance for the given ordinates.
     */
    public Segment(double x1, double y1, double z1, double x2, double y2, double z2) {
      this(new Coordinate(x1, y1, z1), new Coordinate(x2, y2, z2));
    }

    /** 
     * Creates a new instance for the given ordinates,  with associated external data. 
     */
    public Segment(double x1, double y1, double z1, double x2, double y2, double z2, Object data) {
      this(new Coordinate(x1, y1, z1), new Coordinate(x2, y2, z2), data);
    }

    /** 
     * Creates a new instance for the given points, with associated external data.
     * 
     * @param p0 the start point
     * @param p1 the end point
     * @param data an external data object
     */
    public Segment(Coordinate p0, Coordinate p1, Object data) {
        ls = new LineSegment(p0, p1);
        this.data = data;
    }

    /** 
     * Creates a new instance for the given points.
     * 
     * @param p0 the start point
     * @param p1 the end point
     */
    public Segment(Coordinate p0, Coordinate p1) {
        ls = new LineSegment(p0, p1);
    }

    /**
     * Gets the start coordinate of the segment
     * 
     * @return a Coordinate
     */
    public Coordinate getStart() {
        return ls.getCoordinate(0);
    }

    /**
     * Gets the end coordinate of the segment
     * 
     * @return a Coordinate
     */
    public Coordinate getEnd() {
        return ls.getCoordinate(1);
    }

    /**
     * Gets the start X ordinate of the segment
     * 
     * @return the X ordinate value
     */
    public double getStartX() {
        Coordinate p = ls.getCoordinate(0);
        return p.x;
    }

    /**
     * Gets the start Y ordinate of the segment
     * 
     * @return the Y ordinate value
     */
    public double getStartY() {
        Coordinate p = ls.getCoordinate(0);
        return p.y;
    }

    /**
     * Gets the start Z ordinate of the segment
     * 
     * @return the Z ordinate value
     */
    public double getStartZ() {
        Coordinate p = ls.getCoordinate(0);
        return p.z;
    }

    /**
     * Gets the end X ordinate of the segment
     * 
     * @return the X ordinate value
     */
    public double getEndX() {
        Coordinate p = ls.getCoordinate(1);
        return p.x;
    }

    /**
     * Gets the end Y ordinate of the segment
     * 
     * @return the Y ordinate value
     */
    public double getEndY() {
        Coordinate p = ls.getCoordinate(1);
        return p.y;
    }

    /**
     * Gets the end Z ordinate of the segment
     * 
     * @return the Z ordinate value
     */
    public double getEndZ() {
        Coordinate p = ls.getCoordinate(1);
        return p.z;
    }

    /**
     * Gets a <tt>LineSegment</tt> modelling this segment.
     * 
     * @return a LineSegment
     */
    public LineSegment getLineSegment() {
        return ls;
    }

    /**
     * Gets the external data associated with this segment
     * 
     * @return a data object
     */
    public Object getData() {
        return data;
    }
    
    /**
     * Sets the external data to be associated with this segment
     * 
     * @param data a data object
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * Determines whether two segments are topologically equal.
     * I.e. equal up to orientation.
     * 
     * @param s a segment
     * @return true if the segments are topologically equal
     */
    public boolean equalsTopo(Segment s) {
        return ls.equalsTopo(s.getLineSegment());
    }

    /**
     * Computes the intersection point between this segment and another one.
     * 
     * @param s a segment
     * @return the intersection point, or <code>null</code> if there is none
     */
    public Coordinate intersection(Segment s) {
        return ls.intersection(s.getLineSegment());
    }

    /**
     * Computes a string representation of this segment.
     * 
     * @return a string
     */
    public String toString() {
        return ls.toString();
    }
}
