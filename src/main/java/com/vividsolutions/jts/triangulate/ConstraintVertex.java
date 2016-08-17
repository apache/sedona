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
import com.vividsolutions.jts.triangulate.quadedge.Vertex;

/**
 * A vertex in a Constrained Delaunay Triangulation.
 * The vertex may or may not lie on a constraint.
 * If it does it may carry extra information about the original constraint.
 * 
 * @author Martin Davis
 */
public class ConstraintVertex extends Vertex {
    private boolean isOnConstraint;
    private Object  constraint = null;

    /**
     * Creates a new constraint vertex
     * 
     * @param p the location of the vertex
     */
    public ConstraintVertex(Coordinate p) {
        super(p);
    }

    /**
     * Sets whether this vertex lies on a constraint.
     * 
     * @param isOnConstraint true if this vertex lies on a constraint
     */
    public void setOnConstraint(boolean isOnConstraint) {
        this.isOnConstraint = isOnConstraint;
    }

    /**
     * Tests whether this vertex lies on a constraint.
     * 
     * @return true if the vertex lies on a constraint
     */
    public boolean isOnConstraint() {
        return isOnConstraint;
    }

    /**
     * Sets the external constraint information
     * 
     * @param constraint an object which carries information about the constraint this vertex lies on
     */
    public void setConstraint(Object constraint) {
        isOnConstraint = true;
        this.constraint = constraint;
    }

    /**
     * Gets the external constraint object
     * 
     * @return the external constraint object
     */
    public Object getConstraint() {
        return constraint;
    }

    /**
     * Merges the constraint data in the vertex <tt>other</tt> into this vertex. 
     * This method is called when an inserted vertex is
     * very close to an existing vertex in the triangulation.
     * 
     * @param other the constraint vertex to merge
     */
    protected void merge(ConstraintVertex other) {
        if (other.isOnConstraint) {
            isOnConstraint = true;
            constraint = other.constraint;
        }
    }
}