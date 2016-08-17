
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

import java.util.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.*;

/**
 * Validates that a collection of {@link SegmentString}s is correctly noded.
 * Throws an appropriate exception if an noding error is found.
 *
 * @version 1.7
 */
public class NodingValidator {

  private LineIntersector li = new RobustLineIntersector();

  private Collection segStrings;

  public NodingValidator(Collection segStrings)
  {
    this.segStrings = segStrings;
  }

  public void checkValid()
  {
  	// MD - is this call required?  Or could it be done in the Interior Intersection code?
    checkEndPtVertexIntersections();
    checkInteriorIntersections();
    checkCollapses();
  }

  /**
   * Checks if a segment string contains a segment pattern a-b-a (which implies a self-intersection)
   */
  private void checkCollapses()
  {
    for (Iterator i = segStrings.iterator(); i.hasNext(); ) {
      SegmentString ss = (SegmentString) i.next();
      checkCollapses(ss);
    }
  }

  private void checkCollapses(SegmentString ss)
  {
    Coordinate[] pts = ss.getCoordinates();
    for (int i = 0; i < pts.length - 2; i++) {
      checkCollapse(pts[i], pts[i + 1], pts[i + 2]);
    }
  }

  private void checkCollapse(Coordinate p0, Coordinate p1, Coordinate p2)
  {
    if (p0.equals(p2))
      throw new RuntimeException("found non-noded collapse at "
                                 + Debug.toLine(p0, p1, p2));
  }

  /**
   * Checks all pairs of segments for intersections at an interior point of a segment
   */
  private void checkInteriorIntersections()
  {
    for (Iterator i = segStrings.iterator(); i.hasNext(); ) {
      SegmentString ss0 = (SegmentString) i.next();
      for (Iterator j = segStrings.iterator(); j.hasNext(); ) {
        SegmentString ss1 = (SegmentString) j.next();

          checkInteriorIntersections(ss0, ss1);
      }
    }
  }

  private void checkInteriorIntersections(SegmentString ss0, SegmentString ss1)
  {
    Coordinate[] pts0 = ss0.getCoordinates();
    Coordinate[] pts1 = ss1.getCoordinates();
    for (int i0 = 0; i0 < pts0.length - 1; i0++) {
      for (int i1 = 0; i1 < pts1.length - 1; i1++) {
        checkInteriorIntersections(ss0, i0, ss1, i1);
      }
    }
  }

  private void checkInteriorIntersections(SegmentString e0, int segIndex0, SegmentString e1, int segIndex1)
  {
    if (e0 == e1 && segIndex0 == segIndex1) return;
//numTests++;
    Coordinate p00 = e0.getCoordinates()[segIndex0];
    Coordinate p01 = e0.getCoordinates()[segIndex0 + 1];
    Coordinate p10 = e1.getCoordinates()[segIndex1];
    Coordinate p11 = e1.getCoordinates()[segIndex1 + 1];

    li.computeIntersection(p00, p01, p10, p11);
    if (li.hasIntersection()) {

      if (li.isProper()
          || hasInteriorIntersection(li, p00, p01)
          || hasInteriorIntersection(li, p10, p11)) {
        throw new RuntimeException("found non-noded intersection at "
                                   + p00 + "-" + p01
                                   + " and "
                                   + p10 + "-" + p11);
      }
    }
  }
  /**
   *@return true if there is an intersection point which is not an endpoint of the segment p0-p1
   */
  private boolean hasInteriorIntersection(LineIntersector li, Coordinate p0, Coordinate p1)
  {
    for (int i = 0; i < li.getIntersectionNum(); i++) {
      Coordinate intPt = li.getIntersection(i);
      if (! (intPt.equals(p0) || intPt.equals(p1)))
          return true;
    }
    return false;
  }

  /**
   * Checks for intersections between an endpoint of a segment string
   * and an interior vertex of another segment string
   */
  private void checkEndPtVertexIntersections()
  {
    for (Iterator i = segStrings.iterator(); i.hasNext(); ) {
      SegmentString ss = (SegmentString) i.next();
      Coordinate[] pts = ss.getCoordinates();
      checkEndPtVertexIntersections(pts[0], segStrings);
      checkEndPtVertexIntersections(pts[pts.length - 1], segStrings);
    }
  }

  private void checkEndPtVertexIntersections(Coordinate testPt, Collection segStrings)
  {
    for (Iterator i = segStrings.iterator(); i.hasNext(); ) {
      SegmentString ss = (SegmentString) i.next();
      Coordinate[] pts = ss.getCoordinates();
      for (int j = 1; j < pts.length - 1; j++) {
        if (pts[j].equals(testPt))
          throw new RuntimeException("found endpt/interior pt intersection at index " + j + " :pt " + testPt);
      }
    }
  }


}
