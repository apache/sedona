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
import com.vividsolutions.jts.geom.*;

/**
 * Allows comparing {@link Coordinate} arrays
 * in an orientation-independent way.
 *
 * @author Martin Davis
 * @version 1.7
 */
public class OrientedCoordinateArray
    implements Comparable
{
  private Coordinate[] pts;
  private boolean orientation;

  /**
   * Creates a new {@link OrientedCoordinateArray}
   * for the given {@link Coordinate} array.
   *
   * @param pts the coordinates to orient
   */
  public OrientedCoordinateArray(Coordinate[] pts)
  {
    this.pts = pts;
    orientation = orientation(pts);
  }

  /**
   * Computes the canonical orientation for a coordinate array.
   *
   * @param pts the array to test
   * @return <code>true</code> if the points are oriented forwards
   * or <code>false</code if the points are oriented in reverse
   */
  private static boolean orientation(Coordinate[] pts)
  {
    return CoordinateArrays.increasingDirection(pts) == 1;
  }

  /**
   * Compares two {@link OrientedCoordinateArray}s for their relative order
   *
   * @return -1 this one is smaller;
   * 0 the two objects are equal;
   * 1 this one is greater
   */

  public int compareTo(Object o1) {
    OrientedCoordinateArray oca = (OrientedCoordinateArray) o1;
    int comp = compareOriented(pts, orientation,
                               oca.pts, oca.orientation);
/*
    // MD - testing only
    int oldComp = SegmentStringDissolver.ptsComp.compare(pts, oca.pts);
    if ((oldComp == 0 || comp == 0) && oldComp != comp) {
      System.out.println("bidir mismatch");

      boolean orient1 = orientation(pts);
      boolean orient2 = orientation(oca.pts);
      int comp2 = compareOriented(pts, orientation,
                               oca.pts, oca.orientation);
      int oldComp2 = SegmentStringDissolver.ptsComp.compare(pts, oca.pts);
    }
    */
    return comp;
  }

  private static int compareOriented(Coordinate[] pts1,
                                     boolean orientation1,
                                     Coordinate[] pts2,
                                     boolean orientation2)
  {
    int dir1 = orientation1 ? 1 : -1;
    int dir2 = orientation2 ? 1 : -1;
    int limit1 = orientation1 ? pts1.length : -1;
    int limit2 = orientation2 ? pts2.length : -1;

    int i1 = orientation1 ? 0 : pts1.length - 1;
    int i2 = orientation2 ? 0 : pts2.length - 1;
    int comp = 0;
    while (true) {
      int compPt = pts1[i1].compareTo(pts2[i2]);
      if (compPt != 0)
        return compPt;
      i1 += dir1;
      i2 += dir2;
      boolean done1 = i1 == limit1;
      boolean done2 = i2 == limit2;
      if (done1 && ! done2) return -1;
      if (! done1 && done2) return 1;
      if (done1 && done2) return 0;
    }
  }


}
