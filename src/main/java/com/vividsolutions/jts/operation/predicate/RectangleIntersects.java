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

import java.util.*;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.algorithm.locate.SimplePointInAreaLocator;
import com.vividsolutions.jts.geom.util.*;

/**
 * Implementation of the <tt>intersects</tt> spatial predicate
 * optimized for the case where one {@link Geometry} is a rectangle. 
 * This class works for all
 * input geometries, including {@link GeometryCollection}s.
 * <p>
 * As a further optimization, 
 * this class can be used in batch style
 * to test many geometries
 * against a single rectangle.
 * 
 * @version 1.7
 */
public class RectangleIntersects
{
  /**
   * Tests whether a rectangle intersects a given geometry.
   * 
   * @param rectangle
   *          a rectangular Polygon
   * @param b
   *          a Geometry of any type
   * @return true if the geometries intersect
   */
  public static boolean intersects(Polygon rectangle, Geometry b)
  {
    RectangleIntersects rp = new RectangleIntersects(rectangle);
    return rp.intersects(b);
  }

  private Polygon rectangle;

  private Envelope rectEnv;

  /**
   * Create a new intersects computer for a rectangle.
   * 
   * @param rectangle
   *          a rectangular Polygon
   */
  public RectangleIntersects(Polygon rectangle)
  {
    this.rectangle = rectangle;
    rectEnv = rectangle.getEnvelopeInternal();
  }

  /**
   * Tests whether the given Geometry intersects
   * the query rectangle.
   * 
   * @param geom the Geometry to test (may be of any type)
   * @return true if the geometry intersects the query rectangle
   */
  public boolean intersects(Geometry geom)
  {
    if (!rectEnv.intersects(geom.getEnvelopeInternal()))
      return false;

    /**
     * Test if rectangle envelope intersects any component envelope.
     * This handles Point components as well
     */
    EnvelopeIntersectsVisitor visitor = new EnvelopeIntersectsVisitor(rectEnv);
    visitor.applyTo(geom);
    if (visitor.intersects())
      return true;

    /**
     * Test if any rectangle vertex is contained in the target geometry
     */
    GeometryContainsPointVisitor ecpVisitor = new GeometryContainsPointVisitor(rectangle);
    ecpVisitor.applyTo(geom);
    if (ecpVisitor.containsPoint())
      return true;

    /**
     * Test if any target geometry line segment intersects the rectangle
     */
    RectangleIntersectsSegmentVisitor riVisitor = new RectangleIntersectsSegmentVisitor(rectangle);
    riVisitor.applyTo(geom);
    if (riVisitor.intersects())
      return true;

    return false;
  }
}

/**
 * Tests whether it can be concluded that a rectangle intersects a geometry,
 * based on the relationship of the envelope(s) of the geometry.
 * 
 * @author Martin Davis
 * @version 1.7
 */
class EnvelopeIntersectsVisitor extends ShortCircuitedGeometryVisitor
{
  private Envelope rectEnv;

  private boolean intersects = false;

  public EnvelopeIntersectsVisitor(Envelope rectEnv)
  {
    this.rectEnv = rectEnv;
  }

  /**
   * Reports whether it can be concluded that an intersection occurs, 
   * or whether further testing is required.
   * 
   * @return true if an intersection must occur 
   * or false if no conclusion about intersection can be made
   */
  public boolean intersects()
  {
    return intersects;
  }

  protected void visit(Geometry element)
  {
    Envelope elementEnv = element.getEnvelopeInternal();

    // disjoint => no intersection
    if (!rectEnv.intersects(elementEnv)) {
      return;
    }
    // rectangle contains target env => must intersect
    if (rectEnv.contains(elementEnv)) {
      intersects = true;
      return;
    }
    /**
     * Since the envelopes intersect and the test element is connected, if the
     * test envelope is completely bisected by an edge of the rectangle the
     * element and the rectangle must touch (This is basically an application of
     * the Jordan Curve Theorem). The alternative situation is that the test
     * envelope is "on a corner" of the rectangle envelope, i.e. is not
     * completely bisected. In this case it is not possible to make a conclusion
     * about the presence of an intersection.
     */
    if (elementEnv.getMinX() >= rectEnv.getMinX()
        && elementEnv.getMaxX() <= rectEnv.getMaxX()) {
      intersects = true;
      return;
    }
    if (elementEnv.getMinY() >= rectEnv.getMinY()
        && elementEnv.getMaxY() <= rectEnv.getMaxY()) {
      intersects = true;
      return;
    }
  }

  protected boolean isDone()
  {
    return intersects == true;
  }
}

/**
 * A visitor which tests whether it can be 
 * concluded that a geometry contains a vertex of
 * a query geometry.
 * 
 * @author Martin Davis
 * @version 1.7
 */
class GeometryContainsPointVisitor extends ShortCircuitedGeometryVisitor
{
  private CoordinateSequence rectSeq;

  private Envelope rectEnv;

  private boolean containsPoint = false;

  public GeometryContainsPointVisitor(Polygon rectangle)
  {
    this.rectSeq = rectangle.getExteriorRing().getCoordinateSequence();
    rectEnv = rectangle.getEnvelopeInternal();
  }

  /**
   * Reports whether it can be concluded that a corner point of the rectangle is
   * contained in the geometry, or whether further testing is required.
   * 
   * @return true if a corner point is contained 
   * or false if no conclusion about intersection can be made
   */
  public boolean containsPoint()
  {
    return containsPoint;
  }

  protected void visit(Geometry geom)
  {
    // if test geometry is not polygonal this check is not needed
    if (!(geom instanceof Polygon))
      return;

    // skip if envelopes do not intersect
    Envelope elementEnv = geom.getEnvelopeInternal();
    if (!rectEnv.intersects(elementEnv))
      return;

    // test each corner of rectangle for inclusion
    Coordinate rectPt = new Coordinate();
    for (int i = 0; i < 4; i++) {
      rectSeq.getCoordinate(i, rectPt);
      if (!elementEnv.contains(rectPt))
        continue;
      // check rect point in poly (rect is known not to touch polygon at this
      // point)
      if (SimplePointInAreaLocator.containsPointInPolygon(rectPt,
          (Polygon) geom)) {
        containsPoint = true;
        return;
      }
    }
  }

  protected boolean isDone()
  {
    return containsPoint == true;
  }
}


/**
 * A visitor to test for intersection between the query
 * rectangle and the line segments of the geometry.
 * 
 * @author Martin Davis
 *
 */
class RectangleIntersectsSegmentVisitor extends ShortCircuitedGeometryVisitor
{
  private Envelope rectEnv;
  private RectangleLineIntersector rectIntersector;

  private boolean hasIntersection = false;
  private Coordinate p0 = new Coordinate();
  private Coordinate p1 = new Coordinate();

  /**
   * Creates a visitor for checking rectangle intersection
   * with segments
   * 
   * @param rectangle the query rectangle 
   */
  public RectangleIntersectsSegmentVisitor(Polygon rectangle)
  {
    rectEnv = rectangle.getEnvelopeInternal();
    rectIntersector = new RectangleLineIntersector(rectEnv);
  }

  /**
   * Reports whether any segment intersection exists.
   * 
   * @return true if a segment intersection exists
   * or false if no segment intersection exists
   */
  public boolean intersects()
  {
    return hasIntersection;
  }

  protected void visit(Geometry geom)
  {
    /**
     * It may be the case that the rectangle and the 
     * envelope of the geometry component are disjoint,
     * so it is worth checking this simple condition.
     */
    Envelope elementEnv = geom.getEnvelopeInternal();
    if (!rectEnv.intersects(elementEnv))
      return;
    
    // check segment intersections
    // get all lines from geometry component
    // (there may be more than one if it's a multi-ring polygon)
    List lines = LinearComponentExtracter.getLines(geom);
    checkIntersectionWithLineStrings(lines);
  }

  private void checkIntersectionWithLineStrings(List lines)
  {
    for (Iterator i = lines.iterator(); i.hasNext(); ) {
      LineString testLine = (LineString) i.next();
      checkIntersectionWithSegments(testLine);
      if (hasIntersection)
        return;
    }
  }

  private void checkIntersectionWithSegments(LineString testLine)
  {
    CoordinateSequence seq1 = testLine.getCoordinateSequence();
    for (int j = 1; j < seq1.size(); j++) {
      seq1.getCoordinate(j - 1, p0);
      seq1.getCoordinate(j,     p1);

      if (rectIntersector.intersects(p0, p1)) {
        hasIntersection = true;
        return;
      }
    }
  }

  protected boolean isDone()
  {
    return hasIntersection == true;
  }
}
