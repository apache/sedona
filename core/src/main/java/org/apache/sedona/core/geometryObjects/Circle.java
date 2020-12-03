/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.geometryObjects;

import org.apache.sedona.core.utils.GeomUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateFilter;
import org.locationtech.jts.geom.CoordinateSequenceComparator;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.GeometryFilter;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

// TODO: Auto-generated Javadoc

/**
 * The Class Circle.
 */
public class Circle
        extends Geometry
{

    /**
     * The center.
     */
    private final Geometry centerGeometry;
    /**
     * The center point.
     */
    private final Coordinate centerPoint;
    /**
     * The radius.
     */
    private Double radius;
    /**
     * The mbr.
     */
    private Envelope MBR;

    /**
     * Instantiates a new circle.
     *
     * @param centerGeometry the center geometry
     * @param givenRadius the given radius
     */
    public Circle(Geometry centerGeometry, Double givenRadius)
    {
        super(new GeometryFactory(centerGeometry.getPrecisionModel()));
        this.centerGeometry = centerGeometry;
        Envelope centerGeometryMBR = this.centerGeometry.getEnvelopeInternal();
        this.centerPoint = new Coordinate((centerGeometryMBR.getMinX() + centerGeometryMBR.getMaxX()) / 2.0,
                (centerGeometryMBR.getMinY() + centerGeometryMBR.getMaxY()) / 2.0);
        // Get the internal radius of the object. We need to make sure that the circle at least should be the minimum circumscribed circle
        double width = centerGeometryMBR.getMaxX() - centerGeometryMBR.getMinX();
        double length = centerGeometryMBR.getMaxY() - centerGeometryMBR.getMinY();
        double centerGeometryInternalRadius = Math.sqrt(width * width + length * length) / 2;
        this.radius = givenRadius > centerGeometryInternalRadius ? givenRadius : centerGeometryInternalRadius;
        this.MBR = new Envelope(this.centerPoint.x - this.radius, this.centerPoint.x + this.radius, this.centerPoint.y - this.radius, this.centerPoint.y + this.radius);
        this.setUserData(centerGeometry.getUserData());
    }

    /**
     * Gets the center geometry.
     *
     * @return the center geometry
     */
    public Geometry getCenterGeometry()
    {
        return centerGeometry;
    }

    /**
     * Gets the center point.
     *
     * @return the center point
     */
    public Coordinate getCenterPoint()
    {
        return this.centerPoint;
    }

    /**
     * Gets the radius.
     *
     * @return the radius
     */
    public Double getRadius()
    {
        return radius;
    }

    /**
     * Sets the radius.
     *
     * @param givenRadius the new radius
     */
    public void setRadius(Double givenRadius)
    {
        // Get the internal radius of the object. We need to make sure that the circle at least should be the minimum circumscribed circle
        Envelope centerGeometryMBR = this.centerGeometry.getEnvelopeInternal();
        double width = centerGeometryMBR.getMaxX() - centerGeometryMBR.getMinX();
        double length = centerGeometryMBR.getMaxY() - centerGeometryMBR.getMinY();
        double centerGeometryInternalRadius = Math.sqrt(width * width + length * length) / 2;
        this.radius = givenRadius > centerGeometryInternalRadius ? givenRadius : centerGeometryInternalRadius;
        this.MBR = new Envelope(this.centerPoint.x - this.radius, this.centerPoint.x + this.radius, this.centerPoint.y - this.radius, this.centerPoint.y + this.radius);
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#covers(org.locationtech.jts.geom.Geometry)
     */
    @Override
    public boolean covers(Geometry other)
    {
        // short-circuit test
        Envelope otherEnvelope = other.getEnvelopeInternal();
        if (!getEnvelopeInternal().covers(otherEnvelope)) {
            return false;
        }

        if (other instanceof Point) {
            return covers((Point) other);
        }

        if (other instanceof LineString) {
            return covers((LineString) other);
        }

        if (other instanceof Polygon) {
            return covers(((Polygon) other).getExteriorRing());
        }

        if (other instanceof GeometryCollection) {
            GeometryCollection collection = (GeometryCollection) other;
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                if (!covers(collection.getGeometryN(i))) {
                    return false;
                }
            }
            return true;
        }

        throw new IllegalArgumentException("Circle.covers() doesn't support geometry type " +
                other.getGeometryType());
    }

    private boolean covers(LineString lineString)
    {
        for (int i = 0; i < lineString.getNumPoints(); i++) {
            if (!covers(lineString.getPointN(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean covers(Point point)
    {
        double deltaX = point.getX() - centerPoint.x;
        double deltaY = point.getY() - centerPoint.y;

        return (deltaX * deltaX + deltaY * deltaY) <= radius * radius;
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#intersects(org.locationtech.jts.geom.Geometry)
     */
    @Override
    public boolean intersects(Geometry other)
    {
        // short-circuit test
        Envelope otherEnvelope = other.getEnvelopeInternal();
        if (!getEnvelopeInternal().intersects(otherEnvelope)) {
            return false;
        }

        if (other instanceof Point) {
            return covers((Point) other);
        }

        if (other instanceof LineString) {
            return intersects((LineString) other);
        }

        if (other instanceof Polygon) {
            return intersects((Polygon) other);
        }

        if (other instanceof GeometryCollection) {
            GeometryCollection collection = (GeometryCollection) other;
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                if (intersects(collection.getGeometryN(i))) {
                    return true;
                }
            }
            return false;
        }

        throw new IllegalArgumentException("Circle.intersects() doesn't support geometry type " +
                other.getGeometryType());
    }

    private boolean intersects(Polygon polygon)
    {
        if (intersects(polygon.getExteriorRing())) {
            return true;
        }

        if (polygon.contains(factory.createPoint(centerPoint))) {
            return true;
        }

        if (polygon.getNumInteriorRing() == 0) {
            return false;
        }

        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            if (intersects(polygon.getInteriorRingN(i))) {
                return true;
            }
        }

        return false;
    }

    private boolean intersects(LineString lineString)
    {
        for (int i = 0; i < lineString.getNumPoints() - 1; i++) {
            if (intersects(lineString.getPointN(i), lineString.getPointN(i + 1))) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if a line from `start` to `end` intersects this circle
     */
    private boolean intersects(Point start, Point end)
    {
        double deltaX = end.getX() - start.getX();
        double deltaY = end.getY() - start.getY();

        double centerDeltaX = start.getX() - centerPoint.x;
        double centerDeltaY = start.getY() - centerPoint.y;

        // Building and solving quadractic equation: ax2 + bx + c = 0
        double a = deltaX * deltaX + deltaY * deltaY;
        double b = 2 * (deltaX * centerDeltaX + deltaY * centerDeltaY);
        double c = centerDeltaX * centerDeltaX + centerDeltaY * centerDeltaY - radius * radius;

        double discriminant = b * b - 4 * a * c;

        if (discriminant < 0) {
            return false;
        }

        double t1 = (-b + Math.sqrt(discriminant)) / (2 * a);
        if (t1 >= 0 && t1 <= 1) {
            return true;
        }

        double t2 = (-b - Math.sqrt(discriminant)) / (2 * a);
        if (t2 >= 0 && t2 <= 1) {
            return true;
        }

        return (Math.signum(t1) != Math.signum(t2));
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#getGeometryType()
     */
    @Override
    public String getGeometryType()
    {
        return "Circle";
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#getCoordinate()
     */
    @Override
    public Coordinate getCoordinate()
    {
        return this.centerGeometry.getCoordinate();
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#getCoordinates()
     */
    @Override
    public Coordinate[] getCoordinates()
    {
        return this.centerGeometry.getCoordinates();
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#getNumPoints()
     */
    @Override
    public int getNumPoints()
    {
        return this.centerGeometry.getNumPoints();
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#isEmpty()
     */
    @Override
    public boolean isEmpty()
    {
        return this.centerGeometry.isEmpty();
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#getDimension()
     */
    @Override
    public int getDimension()
    {
        return 0;
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#getBoundary()
     */
    @Override
    public Geometry getBoundary()
    {
        return getFactory().createGeometryCollection(null);
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#getBoundaryDimension()
     */
    @Override
    public int getBoundaryDimension()
    {
        return 0;
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#reverse()
     */
    @Override
    public Geometry reverse()
    {
        Geometry g = this.centerGeometry.reverse();
        Circle newCircle = new Circle(g, this.radius);
        return newCircle;
    }

    @Override
    protected Geometry reverseInternal()
    {
        Geometry g = this.centerGeometry.reverse();
        Circle newCircle = new Circle(g, this.radius);
        return newCircle;
    }

    public Geometry copy()
    {
        Circle cloneCircle = new Circle(this.centerGeometry.copy(), this.radius);
        return cloneCircle;// return the clone
    }

    @Override
    protected Geometry copyInternal()
    {
        Circle cloneCircle = new Circle(this.centerGeometry.copy(), this.radius);
        return cloneCircle;
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#equalsExact(org.locationtech.jts.geom.Geometry, double)
     */
    @Override
    public boolean equalsExact(Geometry g, double tolerance)
    {
        String type1 = this.getGeometryType();
        String type2 = g.getGeometryType();
        double radius1 = this.radius;
        double radius2 = ((Circle) g).radius;

        if (type1 != type2) { return false; }
        if (radius1 != radius2) { return false; }
        return GeomUtils.equalsTopoGeom(this.centerGeometry, ((Circle) g).centerGeometry);
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#apply(org.locationtech.jts.geom.CoordinateFilter)
     */
    @Override
    public void apply(CoordinateFilter filter)
    {
        // Do nothing. This circle is not expected to be a complete geometry.
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#apply(org.locationtech.jts.geom.CoordinateSequenceFilter)
     */
    @Override
    public void apply(CoordinateSequenceFilter filter)
    {
        // Do nothing. This circle is not expected to be a complete geometry.
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#apply(org.locationtech.jts.geom.GeometryFilter)
     */
    @Override
    public void apply(GeometryFilter filter)
    {
        // Do nothing. This circle is not expected to be a complete geometry.
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#apply(org.locationtech.jts.geom.GeometryComponentFilter)
     */
    @Override
    public void apply(GeometryComponentFilter filter)
    {
        // Do nothing. This circle is not expected to be a complete geometry.
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#normalize()
     */
    @Override
    public void normalize()
    {
        // Do nothing. This circle is not expected to be a complete geometry.
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#computeEnvelopeInternal()
     */
    @Override
    protected Envelope computeEnvelopeInternal()
    {
        if (isEmpty()) {
            return new Envelope();
        }
        return this.MBR;
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#compareToSameClass(java.lang.Object)
     */
    @Override
    protected int compareToSameClass(Object other)
    {
        Envelope env = (Envelope) other;
        Envelope mbr = this.MBR;
        // compare based on numerical ordering of ordinates
        if (mbr.getMinX() < env.getMinX()) { return -1; }
        if (mbr.getMinX() > env.getMinX()) { return 1; }
        if (mbr.getMinY() < env.getMinY()) { return -1; }
        if (mbr.getMinY() > env.getMinY()) { return 1; }
        if (mbr.getMaxX() < env.getMaxX()) { return -1; }
        if (mbr.getMaxX() > env.getMaxX()) { return 1; }
        if (mbr.getMaxY() < env.getMaxY()) { return -1; }
        if (mbr.getMaxY() > env.getMaxY()) { return 1; }
        return 0;
    }

    /* (non-Javadoc)
     * @see org.locationtech.jts.geom.Geometry#compareToSameClass(java.lang.Object, org.locationtech.jts.geom.CoordinateSequenceComparator)
     */
    @Override
    protected int compareToSameClass(Object other, CoordinateSequenceComparator comp)
    {
        Envelope env = (Envelope) other;
        Envelope mbr = this.MBR;
        // compare based on numerical ordering of ordinates
        if (mbr.getMinX() < env.getMinX()) { return -1; }
        if (mbr.getMinX() > env.getMinX()) { return 1; }
        if (mbr.getMinY() < env.getMinY()) { return -1; }
        if (mbr.getMinY() > env.getMinY()) { return 1; }
        if (mbr.getMaxX() < env.getMaxX()) { return -1; }
        if (mbr.getMaxX() > env.getMaxX()) { return 1; }
        if (mbr.getMaxY() < env.getMaxY()) { return -1; }
        if (mbr.getMaxY() > env.getMaxY()) { return 1; }
        return 0;
    }

    @Override
    protected int getTypeCode()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return "Circle of radius " + radius + " around " + centerGeometry;
    }
}
