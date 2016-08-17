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
package com.vividsolutions.jts.noding;

import com.vividsolutions.jts.algorithm.LineIntersector;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Computes the possible intersections between two line segments in {@link NodedSegmentString}s
 * and adds them to each string 
 * using {@link NodedSegmentString#addIntersection(LineIntersector, int, int, int)}.
 *
 * @version 1.7
 */
public class IntersectionAdder
    implements SegmentIntersector
{
  public static boolean isAdjacentSegments(int i1, int i2)
  {
    return Math.abs(i1 - i2) == 1;
  }

  /**
   * These variables keep track of what types of intersections were
   * found during ALL edges that have been intersected.
   */
  private boolean hasIntersection = false;
  private boolean hasProper = false;
  private boolean hasProperInterior = false;
  private boolean hasInterior = false;

  // the proper intersection point found
  private Coordinate properIntersectionPoint = null;

  private LineIntersector li;
  private boolean isSelfIntersection;
  //private boolean intersectionFound;
  public int numIntersections = 0;
  public int numInteriorIntersections = 0;
  public int numProperIntersections = 0;

  // testing only
  public int numTests = 0;

  public IntersectionAdder(LineIntersector li)
  {
    this.li = li;
  }

  public LineIntersector getLineIntersector() { return li; }

  /**
   * @return the proper intersection point, or <code>null</code> if none was found
   */
  public Coordinate getProperIntersectionPoint()  {    return properIntersectionPoint;  }

  public boolean hasIntersection() { return hasIntersection; }
  /**
   * A proper intersection is an intersection which is interior to at least two
   * line segments.  Note that a proper intersection is not necessarily
   * in the interior of the entire Geometry, since another edge may have
   * an endpoint equal to the intersection, which according to SFS semantics
   * can result in the point being on the Boundary of the Geometry.
   */
  public boolean hasProperIntersection() { return hasProper; }
  /**
   * A proper interior intersection is a proper intersection which is <b>not</b>
   * contained in the set of boundary nodes set for this SegmentIntersector.
   */
  public boolean hasProperInteriorIntersection() { return hasProperInterior; }
  /**
   * An interior intersection is an intersection which is
   * in the interior of some segment.
   */
  public boolean hasInteriorIntersection() { return hasInterior; }

  /**
   * A trivial intersection is an apparent self-intersection which in fact
   * is simply the point shared by adjacent line segments.
   * Note that closed edges require a special check for the point shared by the beginning
   * and end segments.
   */
  private boolean isTrivialIntersection(SegmentString e0, int segIndex0, SegmentString e1, int segIndex1)
  {
    if (e0 == e1) {
      if (li.getIntersectionNum() == 1) {
        if (isAdjacentSegments(segIndex0, segIndex1))
          return true;
        if (e0.isClosed()) {
          int maxSegIndex = e0.size() - 1;
          if (    (segIndex0 == 0 && segIndex1 == maxSegIndex)
              ||  (segIndex1 == 0 && segIndex0 == maxSegIndex) ) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * This method is called by clients
   * of the {@link SegmentIntersector} class to process
   * intersections for two segments of the {@link SegmentString}s being intersected.
   * Note that some clients (such as <code>MonotoneChain</code>s) may optimize away
   * this call for segment pairs which they have determined do not intersect
   * (e.g. by an disjoint envelope test).
   */
  public void processIntersections(
    SegmentString e0,  int segIndex0,
    SegmentString e1,  int segIndex1
     )
  {
    if (e0 == e1 && segIndex0 == segIndex1) return;
numTests++;
    Coordinate p00 = e0.getCoordinates()[segIndex0];
    Coordinate p01 = e0.getCoordinates()[segIndex0 + 1];
    Coordinate p10 = e1.getCoordinates()[segIndex1];
    Coordinate p11 = e1.getCoordinates()[segIndex1 + 1];

    li.computeIntersection(p00, p01, p10, p11);
//if (li.hasIntersection() && li.isProper()) Debug.println(li);
    if (li.hasIntersection()) {
      //intersectionFound = true;
      numIntersections++;
      if (li.isInteriorIntersection()) {
        numInteriorIntersections++;
        hasInterior = true;
//System.out.println(li);
      }
      // if the segments are adjacent they have at least one trivial intersection,
      // the shared endpoint.  Don't bother adding it if it is the
      // only intersection.
      if (! isTrivialIntersection(e0, segIndex0, e1, segIndex1)) {
        hasIntersection = true;
        ((NodedSegmentString) e0).addIntersections(li, segIndex0, 0);
        ((NodedSegmentString) e1).addIntersections(li, segIndex1, 1);
        if (li.isProper()) {
          numProperIntersections++;
//Debug.println(li.toString());  Debug.println(li.getIntersection(0));
          //properIntersectionPoint = (Coordinate) li.getIntersection(0).clone();
          hasProper = true;
          hasProperInterior = true;
        }
      }
    }
  }
  
  /**
   * Always process all intersections
   * 
   * @return false always
   */
  public boolean isDone() { return false; }
}
