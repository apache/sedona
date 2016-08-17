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

import com.vividsolutions.jts.operation.BoundaryOp;

/**
 * Models a collection of (@link LineString}s.
 * <p>
 * Any collection of LineStrings is a valid MultiLineString.
 *
 *@version 1.7
 */
public class MultiLineString 
	extends GeometryCollection
	implements Lineal
	{
  private static final long serialVersionUID = 8166665132445433741L;
  /**
   *  Constructs a <code>MultiLineString</code>.
   *
   *@param  lineStrings     the <code>LineString</code>s for this <code>MultiLineString</code>
   *      , or <code>null</code> or an empty array to create the empty geometry.
   *      Elements may be empty <code>LineString</code>s, but not <code>null</code>
   *      s.
   *@param  precisionModel  the specification of the grid of allowable points
   *      for this <code>MultiLineString</code>
   *@param  SRID            the ID of the Spatial Reference System used by this
   *      <code>MultiLineString</code>
   * @deprecated Use GeometryFactory instead
   */
  public MultiLineString(LineString[] lineStrings, PrecisionModel precisionModel, int SRID) {
    super(lineStrings, new GeometryFactory(precisionModel, SRID));
  }



  /**
   * @param lineStrings
   *            the <code>LineString</code>s for this <code>MultiLineString</code>,
   *            or <code>null</code> or an empty array to create the empty
   *            geometry. Elements may be empty <code>LineString</code>s,
   *            but not <code>null</code>s.
   */
  public MultiLineString(LineString[] lineStrings, GeometryFactory factory) {
    super(lineStrings, factory);
  }

  public int getDimension() {
    return 1;
  }

  public int getBoundaryDimension() {
    if (isClosed()) {
      return Dimension.FALSE;
    }
    return 0;
  }

  public String getGeometryType() {
    return "MultiLineString";
  }

  public boolean isClosed() {
    if (isEmpty()) {
      return false;
    }
    for (int i = 0; i < geometries.length; i++) {
      if (!((LineString) geometries[i]).isClosed()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets the boundary of this geometry.
   * The boundary of a lineal geometry is always a zero-dimensional geometry (which may be empty).
   *
   * @return the boundary geometry
   * @see Geometry#getBoundary
   */
  public Geometry getBoundary()
  {
    return (new BoundaryOp(this)).getBoundary();
  }

  /**
   * Creates a {@link MultiLineString} in the reverse
   * order to this object.
   * Both the order of the component LineStrings
   * and the order of their coordinate sequences
   * are reversed.
   *
   * @return a {@link MultiLineString} in the reverse order
   */
  public Geometry reverse()
  {
    int nLines = geometries.length;
    LineString[] revLines = new LineString[nLines];
    for (int i = 0; i < geometries.length; i++) {
      revLines[nLines - 1 - i] = (LineString)geometries[i].reverse();
    }
    return getFactory().createMultiLineString(revLines);
  }

  public boolean equalsExact(Geometry other, double tolerance) {
    if (!isEquivalentClass(other)) {
      return false;
    }
    return super.equalsExact(other, tolerance);
  }

}

