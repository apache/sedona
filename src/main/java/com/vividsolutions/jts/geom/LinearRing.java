

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
 * Models an OGC SFS <code>LinearRing</code>.
 * A <code>LinearRing</code> is a {@link LineString} which is both closed and simple.
 * In other words,
 * the first and last coordinate in the ring must be equal,
 * and the interior of the ring must not self-intersect.
 * Either orientation of the ring is allowed.
 * <p>
 * A ring must have either 0 or 4 or more points.  
 * The first and last points must be equal (in 2D).
 * If these conditions are not met, the constructors throw 
 * an {@link IllegalArgumentException}
 *
 * @version 1.7
 */
public class LinearRing extends LineString
{
  /**
   * The minimum number of vertices allowed in a valid non-empty ring (= 4).
   * Empty rings with 0 vertices are also valid.
   */
  public static final int MINIMUM_VALID_SIZE = 4;
  
  private static final long serialVersionUID = -4261142084085851829L;

  /**
   * Constructs a <code>LinearRing</code> with the given points.
   *
   *@param  points          points forming a closed and simple linestring, or
   *      <code>null</code> or an empty array to create the empty geometry.
   *      This array must not contain <code>null</code> elements.
   *
   *@param  precisionModel  the specification of the grid of allowable points
   *      for this <code>LinearRing</code>
   *@param  SRID            the ID of the Spatial Reference System used by this
   *      <code>LinearRing</code>
   * @throws IllegalArgumentException if the ring is not closed, or has too few points
   * 
   * @deprecated Use GeometryFactory instead
   */
  public LinearRing(Coordinate points[], PrecisionModel precisionModel,
                    int SRID) {
    this(points, new GeometryFactory(precisionModel, SRID));
    validateConstruction();
  }

  /**
   * This method is ONLY used to avoid deprecation warnings.
   * @param points
   * @param factory
   * @throws IllegalArgumentException if the ring is not closed, or has too few points
   */
  private LinearRing(Coordinate points[], GeometryFactory factory) {
    this(factory.getCoordinateSequenceFactory().create(points), factory);
  }


  /**
   * Constructs a <code>LinearRing</code> with the vertices
   * specifed by the given {@link CoordinateSequence}.
   *
   *@param  points  a sequence points forming a closed and simple linestring, or
   *      <code>null</code> to create the empty geometry.
   *      
   * @throws IllegalArgumentException if the ring is not closed, or has too few points
   *
   */
  public LinearRing(CoordinateSequence points, GeometryFactory factory) {
    super(points, factory);
    validateConstruction();
  }

  private void validateConstruction() {
    if (!isEmpty() && ! super.isClosed()) {
      throw new IllegalArgumentException("Points of LinearRing do not form a closed linestring");
    }
    if (getCoordinateSequence().size() >= 1 && getCoordinateSequence().size() < MINIMUM_VALID_SIZE) {
      throw new IllegalArgumentException("Invalid number of points in LinearRing (found " 
      		+ getCoordinateSequence().size() + " - must be 0 or >= 4)");
    }
  }

  /**
   * Returns <code>Dimension.FALSE</code>, since by definition LinearRings do
   * not have a boundary.
   *
   * @return Dimension.FALSE
   */
  public int getBoundaryDimension() {
    return Dimension.FALSE;
  }

  /**
   * Tests whether this ring is closed.
   * Empty rings are closed by definition.
   * 
   * @return true if this ring is closed
   */
  public boolean isClosed() {
    if (isEmpty()) {
    	// empty LinearRings are closed by definition
      return true;
    }
    return super.isClosed();
  }


  public String getGeometryType() {
    return "LinearRing";
  }

  public Geometry reverse()
  {
    CoordinateSequence seq = (CoordinateSequence) points.clone();
    CoordinateSequences.reverse(seq);
    LinearRing rev = getFactory().createLinearRing(seq);
    return rev;
  }
}
