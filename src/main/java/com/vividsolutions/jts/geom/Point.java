

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
package com.vividsolutions.jts.geom;

import com.vividsolutions.jts.util.Assert;
import com.vividsolutions.jts.operation.valid.*;

/**
 * Represents a single point.
 *
 * A <code>Point</code> is topologically valid if and only if:
 * <ul>
 * <li>the coordinate which defines it (if any) is a valid coordinate 
 * (i.e does not have an <code>NaN</code> X or Y ordinate)
 * </ul>
 * 
 *@version 1.7
 */
public class Point 
	extends Geometry
	implements Puntal
{
  private static final long serialVersionUID = 4902022702746614570L;
  /**
   *  The <code>Coordinate</code> wrapped by this <code>Point</code>.
   */
  private CoordinateSequence coordinates;

  /**
   *  Constructs a <code>Point</code> with the given coordinate.
   *
   *@param  coordinate      the coordinate on which to base this <code>Point</code>
   *      , or <code>null</code> to create the empty geometry.
   *@param  precisionModel  the specification of the grid of allowable points
   *      for this <code>Point</code>
   *@param  SRID            the ID of the Spatial Reference System used by this
   *      <code>Point</code>
   * @deprecated Use GeometryFactory instead
   */
  public Point(Coordinate coordinate, PrecisionModel precisionModel, int SRID) {
    super(new GeometryFactory(precisionModel, SRID));
    init(getFactory().getCoordinateSequenceFactory().create(
          coordinate != null ? new Coordinate[]{coordinate} : new Coordinate[]{}));
  }

  /**
   *@param  coordinates      contains the single coordinate on which to base this <code>Point</code>
   *      , or <code>null</code> to create the empty geometry.
   */
  public Point(CoordinateSequence coordinates, GeometryFactory factory) {
    super(factory);
    init(coordinates);
  }

  private void init(CoordinateSequence coordinates)
  {
    if (coordinates == null) {
      coordinates = getFactory().getCoordinateSequenceFactory().create(new Coordinate[]{});
    }
    Assert.isTrue(coordinates.size() <= 1);
    this.coordinates = coordinates;
  }

  public Coordinate[] getCoordinates() {
    return isEmpty() ? new Coordinate[]{} : new Coordinate[]{
        getCoordinate()
        };
  }

  public int getNumPoints() {
    return isEmpty() ? 0 : 1;
  }

  public boolean isEmpty() {
    return coordinates.size() == 0;
  }

  public boolean isSimple() {
    return true;
  }

  public int getDimension() {
    return 0;
  }

  public int getBoundaryDimension() {
    return Dimension.FALSE;
  }

  public double getX() {
    if (getCoordinate() == null) {
      throw new IllegalStateException("getX called on empty Point");
    }
    return getCoordinate().x;
  }

  public double getY() {
    if (getCoordinate() == null) {
      throw new IllegalStateException("getY called on empty Point");
    }
    return getCoordinate().y;
  }

  public Coordinate getCoordinate() {
    return coordinates.size() != 0 ? coordinates.getCoordinate(0): null;
  }

  public String getGeometryType() {
    return "Point";
  }

  /**
   * Gets the boundary of this geometry.
   * Zero-dimensional geometries have no boundary by definition,
   * so an empty GeometryCollection is returned.
   *
   * @return an empty GeometryCollection
   * @see Geometry#getBoundary
   */
  public Geometry getBoundary() {
    return getFactory().createGeometryCollection(null);
  }

  protected Envelope computeEnvelopeInternal() {
    if (isEmpty()) {
      return new Envelope();
    }
    Envelope env = new Envelope();
    env.expandToInclude(coordinates.getX(0), coordinates.getY(0));
    return env;
  }

  public boolean equalsExact(Geometry other, double tolerance) {
    if (!isEquivalentClass(other)) {
      return false;
    }
    if (isEmpty() && other.isEmpty()) {
      return true;
    }
    if (isEmpty() != other.isEmpty()) {
      return false;
    }
    return equal(((Point) other).getCoordinate(), this.getCoordinate(), tolerance);
  }

  public void apply(CoordinateFilter filter) {
	    if (isEmpty()) { return; }
	    filter.filter(getCoordinate());
	  }

  public void apply(CoordinateSequenceFilter filter) 
  {
	    if (isEmpty())
        return;
	    filter.filter(coordinates, 0);
      if (filter.isGeometryChanged())
        geometryChanged();
	  }

  public void apply(GeometryFilter filter) {
    filter.filter(this);
  }

  public void apply(GeometryComponentFilter filter) {
    filter.filter(this);
  }

  /**
   * Creates and returns a full copy of this {@link Point} object.
   * (including all coordinates contained by it).
   *
   * @return a clone of this instance
   */
  public Object clone() {
    Point p = (Point) super.clone();
    p.coordinates = (CoordinateSequence) coordinates.clone();
    return p;// return the clone
  }

  public Geometry reverse()
  {
    return (Geometry) clone();
  }
  
  public void normalize() 
  { 
    // a Point is always in normalized form 
  }

  protected int compareToSameClass(Object other) {
    Point point = (Point) other;
    return getCoordinate().compareTo(point.getCoordinate());
  }

  protected int compareToSameClass(Object other, CoordinateSequenceComparator comp)
  {
    Point point = (Point) other;
    return comp.compare(this.coordinates, point.coordinates);
  }

  public CoordinateSequence getCoordinateSequence() {
    return coordinates;
  }
}

