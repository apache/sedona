

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

/**
 * Models a collection of {@link Point}s.
 * <p>
 * Any collection of Points is a valid MultiPoint.
 *
 *@version 1.7
 */
public class MultiPoint
  extends GeometryCollection
  implements Puntal
{

  private static final long serialVersionUID = -8048474874175355449L;

  /**
   *  Constructs a <code>MultiPoint</code>.
   *
   *@param  points          the <code>Point</code>s for this <code>MultiPoint</code>
   *      , or <code>null</code> or an empty array to create the empty geometry.
   *      Elements may be empty <code>Point</code>s, but not <code>null</code>s.
   *@param  precisionModel  the specification of the grid of allowable points
   *      for this <code>MultiPoint</code>
   *@param  SRID            the ID of the Spatial Reference System used by this
   *      <code>MultiPoint</code>
   * @deprecated Use GeometryFactory instead
   */
  public MultiPoint(Point[] points, PrecisionModel precisionModel, int SRID) {
    super(points, new GeometryFactory(precisionModel, SRID));
  }

  /**
   *@param  points          the <code>Point</code>s for this <code>MultiPoint</code>
   *      , or <code>null</code> or an empty array to create the empty geometry.
   *      Elements may be empty <code>Point</code>s, but not <code>null</code>s.
   */
  public MultiPoint(Point[] points, GeometryFactory factory) {
    super(points, factory);
  }

  public int getDimension() {
    return 0;
  }

  public int getBoundaryDimension() {
    return Dimension.FALSE;
  }

  public String getGeometryType() {
    return "MultiPoint";
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

  public boolean isValid() {
    return true;
  }

  public boolean equalsExact(Geometry other, double tolerance) {
    if (!isEquivalentClass(other)) {
      return false;
    }
    return super.equalsExact(other, tolerance);
  }

  /**
   *  Returns the <code>Coordinate</code> at the given position.
   *
   *@param  n  the index of the <code>Coordinate</code> to retrieve, beginning
   *      at 0
   *@return    the <code>n</code>th <code>Coordinate</code>
   */
  protected Coordinate getCoordinate(int n) {
    return ((Point) geometries[n]).getCoordinate();
  }

}

