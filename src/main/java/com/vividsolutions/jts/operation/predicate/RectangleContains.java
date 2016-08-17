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

package com.vividsolutions.jts.operation.predicate;

import com.vividsolutions.jts.geom.*;

/**
 * Optimized implementation of the <tt>contains</tt> spatial predicate 
 * for cases where the first {@link Geometry} is a rectangle.
 * This class works for all input geometries, including
 * {@link GeometryCollection}s.
 * <p>
 * As a further optimization,
 * this class can be used to test 
 * many geometries against a single
 * rectangle in a slightly more efficient way.
 *
 * @version 1.7
 */
public class RectangleContains {

  /**
   * Tests whether a rectangle contains a given geometry.
   * 
   * @param rectangle a rectangular Polygon
   * @param b a Geometry of any type
   * @return true if the geometries intersect
   */
  public static boolean contains(Polygon rectangle, Geometry b)
  {
    RectangleContains rc = new RectangleContains(rectangle);
    return rc.contains(b);
  }

  private Envelope rectEnv;

  /**
   * Create a new contains computer for two geometries.
   *
   * @param rectangle a rectangular geometry
   */
  public RectangleContains(Polygon rectangle) {
    rectEnv = rectangle.getEnvelopeInternal();
  }

  public boolean contains(Geometry geom)
  {
    // the test geometry must be wholly contained in the rectangle envelope
    if (! rectEnv.contains(geom.getEnvelopeInternal()))
      return false;
    
    /**
     * Check that geom is not contained entirely in the rectangle boundary.
     * According to the somewhat odd spec of the SFS, if this
     * is the case the geometry is NOT contained.
     */
    if (isContainedInBoundary(geom))
      return false;
    return true;
  }

  private boolean isContainedInBoundary(Geometry geom)
  {
    // polygons can never be wholely contained in the boundary
    if (geom instanceof Polygon) return false;
    if (geom instanceof Point) return isPointContainedInBoundary((Point) geom);
    if (geom instanceof LineString) return isLineStringContainedInBoundary((LineString) geom);

    for (int i = 0; i < geom.getNumGeometries(); i++) {
      Geometry comp = geom.getGeometryN(i);
      if (! isContainedInBoundary(comp))
        return false;
    }
    return true;
  }

  private boolean isPointContainedInBoundary(Point point)
  {
    return isPointContainedInBoundary(point.getCoordinate());
  }

  /**
   * Tests if a point is contained in the boundary of the target rectangle.
   * 
   * @param pt the point to test
   * @return true if the point is contained in the boundary
   */
  private boolean isPointContainedInBoundary(Coordinate pt)
  {
    /**
     * contains = false iff the point is properly contained in the rectangle.
     * 
     * This code assumes that the point lies in the rectangle envelope
     */ 
    return pt.x == rectEnv.getMinX() 
    		|| pt.x == rectEnv.getMaxX()
    		|| pt.y == rectEnv.getMinY()
    		|| pt.y == rectEnv.getMaxY();
  }

  /**
   * Tests if a linestring is completely contained in the boundary of the target rectangle.
   * @param line the linestring to test
   * @return true if the linestring is contained in the boundary
   */
  private boolean isLineStringContainedInBoundary(LineString line)
  {
    CoordinateSequence seq = line.getCoordinateSequence();
    Coordinate p0 = new Coordinate();
    Coordinate p1 = new Coordinate();
    for (int i = 0; i < seq.size() - 1; i++) {
      seq.getCoordinate(i, p0);
      seq.getCoordinate(i + 1, p1);

      if (! isLineSegmentContainedInBoundary(p0, p1))
        return false;
    }
    return true;
  }

  /**
   * Tests if a line segment is contained in the boundary of the target rectangle.
   * @param p0 an endpoint of the segment
   * @param p1 an endpoint of the segment
   * @return true if the line segment is contained in the boundary
   */
  private boolean isLineSegmentContainedInBoundary(Coordinate p0, Coordinate p1)
  {
    if (p0.equals(p1))
      return isPointContainedInBoundary(p0);

    // we already know that the segment is contained in the rectangle envelope
    if (p0.x == p1.x) {
      if (p0.x == rectEnv.getMinX() ||
          p0.x == rectEnv.getMaxX() )
        return true;
    }
    else if (p0.y == p1.y) {
      if (p0.y == rectEnv.getMinY() ||
          p0.y == rectEnv.getMaxY() )
        return true;
    }
    /**
     * Either
     *   both x and y values are different
     * or
     *   one of x and y are the same, but the other ordinate is not the same as a boundary ordinate
     *
     * In either case, the segment is not wholely in the boundary
     */
    return false;
  }

}
