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

package com.vividsolutions.jts.awt;

import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.PathIterator;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A {@link Shape} which contains a heterogeneous collection of other shapes
 * representing JTS {@link Geometry}s.
 * 
 * @author Martin Davis
 *
 */
public class GeometryCollectionShape implements Shape {
    private ArrayList shapes = new ArrayList();

    public GeometryCollectionShape() {
    }

    public void add(Shape shape) {
        shapes.add(shape);
    }

    public Rectangle getBounds() {
        /**@todo Implement this java.awt.Shape method*/
        throw new java.lang.UnsupportedOperationException(
            "Method getBounds() not yet implemented.");
    }

    public Rectangle2D getBounds2D() {
        Rectangle2D rectangle = null;

        for (Iterator i = shapes.iterator(); i.hasNext();) {
            Shape shape = (Shape) i.next();

            if (rectangle == null) {
                rectangle = shape.getBounds2D();
            } else {
                rectangle.add(shape.getBounds2D());
            }
        }

        return rectangle;
    }

    public boolean contains(double x, double y) {
        /**@todo Implement this java.awt.Shape method*/
        throw new java.lang.UnsupportedOperationException(
            "Method contains() not yet implemented.");
    }

    public boolean contains(Point2D p) {
        /**@todo Implement this java.awt.Shape method*/
        throw new java.lang.UnsupportedOperationException(
            "Method contains() not yet implemented.");
    }

    public boolean intersects(double x, double y, double w, double h) {
        /**@todo Implement this java.awt.Shape method*/
        throw new java.lang.UnsupportedOperationException(
            "Method intersects() not yet implemented.");
    }

    public boolean intersects(Rectangle2D r) {
        /**@todo Implement this java.awt.Shape method*/
        throw new java.lang.UnsupportedOperationException(
            "Method intersects() not yet implemented.");
    }

    public boolean contains(double x, double y, double w, double h) {
        /**@todo Implement this java.awt.Shape method*/
        throw new java.lang.UnsupportedOperationException(
            "Method contains() not yet implemented.");
    }

    public boolean contains(Rectangle2D r) {
        /**@todo Implement this java.awt.Shape method*/
        throw new java.lang.UnsupportedOperationException(
            "Method contains() not yet implemented.");
    }

    public PathIterator getPathIterator(AffineTransform at) {
        return new ShapeCollectionPathIterator(shapes, at);
    }

    public PathIterator getPathIterator(AffineTransform at, double flatness) {
        // since Geometry is linear, can simply delegate to the simple method
        return getPathIterator(at);
    }
}
