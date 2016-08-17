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
package com.vividsolutions.jts.algorithm;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Location;
import com.vividsolutions.jts.math.MathUtil;

/**
 * Specifies and implements various fundamental Computational Geometric
 * algorithms. The algorithms supplied in this class are robust for
 * double-precision floating point.
 * 
 * @version 1.7
 */
public class CGAlgorithms
{

  /**
   * A value that indicates an orientation of clockwise, or a right turn.
   */
  public static final int CLOCKWISE = -1;

  /**
   * A value that indicates an orientation of clockwise, or a right turn.
   */
  public static final int RIGHT = CLOCKWISE;

  /**
   * A value that indicates an orientation of counterclockwise, or a left turn.
   */
  public static final int COUNTERCLOCKWISE = 1;

  /**
   * A value that indicates an orientation of counterclockwise, or a left turn.
   */
  public static final int LEFT = COUNTERCLOCKWISE;

  /**
   * A value that indicates an orientation of collinear, or no turn (straight).
   */
  public static final int COLLINEAR = 0;

  /**
   * A value that indicates an orientation of collinear, or no turn (straight).
   */
  public static final int STRAIGHT = COLLINEAR;

  /**
   * Returns the index of the direction of the point <code>q</code> relative to
   * a vector specified by <code>p1-p2</code>.
   * 
   * @param p1 the origin point of the vector
   * @param p2 the final point of the vector
   * @param q the point to compute the direction to
   * 
   * @return 1 if q is counter-clockwise (left) from p1-p2
   * @return -1 if q is clockwise (right) from p1-p2
   * @return 0 if q is collinear with p1-p2
   */
  public static int orientationIndex(Coordinate p1, Coordinate p2, Coordinate q)
  {
    /**
     * MD - 9 Aug 2010 It seems that the basic algorithm is slightly orientation
     * dependent, when computing the orientation of a point very close to a
     * line. This is possibly due to the arithmetic in the translation to the
     * origin.
     * 
     * For instance, the following situation produces identical results in spite
     * of the inverse orientation of the line segment:
     * 
     * Coordinate p0 = new Coordinate(219.3649559090992, 140.84159161824724);
     * Coordinate p1 = new Coordinate(168.9018919682399, -5.713787599646864);
     * 
     * Coordinate p = new Coordinate(186.80814046338352, 46.28973405831556); int
     * orient = orientationIndex(p0, p1, p); int orientInv =
     * orientationIndex(p1, p0, p);
     * 
     * A way to force consistent results is to normalize the orientation of the
     * vector using the following code. However, this may make the results of
     * orientationIndex inconsistent through the triangle of points, so it's not
     * clear this is an appropriate patch.
     * 
     */
    return CGAlgorithmsDD.orientationIndex(p1, p2, q);
    // testing only
    //return ShewchuksDeterminant.orientationIndex(p1, p2, q);
    // previous implementation - not quite fully robust
    //return RobustDeterminant.orientationIndex(p1, p2, q);
    
  }

  public CGAlgorithms()
  {
  }

  /**
   * Tests whether a point lies inside or on a ring. The ring may be oriented in
   * either direction. A point lying exactly on the ring boundary is considered
   * to be inside the ring.
   * <p>
   * This method does <i>not</i> first check the point against the envelope of
   * the ring.
   * 
   * @param p
   *          point to check for ring inclusion
   * @param ring
   *          an array of coordinates representing the ring (which must have
   *          first point identical to last point)
   * @return true if p is inside ring
   * 
   * @see locatePointInRing
   */
  public static boolean isPointInRing(Coordinate p, Coordinate[] ring)
  {
    return locatePointInRing(p, ring) != Location.EXTERIOR;
  }

  /**
   * Determines whether a point lies in the interior, on the boundary, or in the
   * exterior of a ring. The ring may be oriented in either direction.
   * <p>
   * This method does <i>not</i> first check the point against the envelope of
   * the ring.
   * 
   * @param p
   *          point to check for ring inclusion
   * @param ring
   *          an array of coordinates representing the ring (which must have
   *          first point identical to last point)
   * @return the {@link Location} of p relative to the ring
   */
  public static int locatePointInRing(Coordinate p, Coordinate[] ring)
  {
    return RayCrossingCounter.locatePointInRing(p, ring);
  }

  /**
   * Tests whether a point lies on the line segments defined by a list of
   * coordinates.
   * 
   * @return true if the point is a vertex of the line or lies in the interior
   *         of a line segment in the linestring
   */
  public static boolean isOnLine(Coordinate p, Coordinate[] pt)
  {
    LineIntersector lineIntersector = new RobustLineIntersector();
    for (int i = 1; i < pt.length; i++) {
      Coordinate p0 = pt[i - 1];
      Coordinate p1 = pt[i];
      lineIntersector.computeIntersection(p, p0, p1);
      if (lineIntersector.hasIntersection()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Computes whether a ring defined by an array of {@link Coordinate}s is
   * oriented counter-clockwise.
   * <ul>
   * <li>The list of points is assumed to have the first and last points equal.
   * <li>This will handle coordinate lists which contain repeated points.
   * </ul>
   * This algorithm is <b>only</b> guaranteed to work with valid rings. If the
   * ring is invalid (e.g. self-crosses or touches), the computed result may not
   * be correct.
   * 
   * @param ring
   *          an array of Coordinates forming a ring
   * @return true if the ring is oriented counter-clockwise.
   * @throws IllegalArgumentException
   *           if there are too few points to determine orientation (< 4)
   */
  public static boolean isCCW(Coordinate[] ring)
  {
    // # of points without closing endpoint
    int nPts = ring.length - 1;
    // sanity check
    if (nPts < 3)
      throw new IllegalArgumentException(
          "Ring has fewer than 4 points, so orientation cannot be determined");

    // find highest point
    Coordinate hiPt = ring[0];
    int hiIndex = 0;
    for (int i = 1; i <= nPts; i++) {
      Coordinate p = ring[i];
      if (p.y > hiPt.y) {
        hiPt = p;
        hiIndex = i;
      }
    }

    // find distinct point before highest point
    int iPrev = hiIndex;
    do {
      iPrev = iPrev - 1;
      if (iPrev < 0)
        iPrev = nPts;
    } while (ring[iPrev].equals2D(hiPt) && iPrev != hiIndex);

    // find distinct point after highest point
    int iNext = hiIndex;
    do {
      iNext = (iNext + 1) % nPts;
    } while (ring[iNext].equals2D(hiPt) && iNext != hiIndex);

    Coordinate prev = ring[iPrev];
    Coordinate next = ring[iNext];

    /**
     * This check catches cases where the ring contains an A-B-A configuration
     * of points. This can happen if the ring does not contain 3 distinct points
     * (including the case where the input array has fewer than 4 elements), or
     * it contains coincident line segments.
     */
    if (prev.equals2D(hiPt) || next.equals2D(hiPt) || prev.equals2D(next))
      return false;

    int disc = computeOrientation(prev, hiPt, next);

    /**
     * If disc is exactly 0, lines are collinear. There are two possible cases:
     * (1) the lines lie along the x axis in opposite directions (2) the lines
     * lie on top of one another
     * 
     * (1) is handled by checking if next is left of prev ==> CCW (2) will never
     * happen if the ring is valid, so don't check for it (Might want to assert
     * this)
     */
    boolean isCCW = false;
    if (disc == 0) {
      // poly is CCW if prev x is right of next x
      isCCW = (prev.x > next.x);
    }
    else {
      // if area is positive, points are ordered CCW
      isCCW = (disc > 0);
    }
    return isCCW;
  }

  /**
   * Computes the orientation of a point q to the directed line segment p1-p2.
   * The orientation of a point relative to a directed line segment indicates
   * which way you turn to get to q after travelling from p1 to p2.
   * 
   * @param p1 the first vertex of the line segment
   * @param p2 the second vertex of the line segment
   * @param q the point to compute the relative orientation of
   * @return 1 if q is counter-clockwise from p1-p2,
   * or -1 if q is clockwise from p1-p2,
   * or 0 if q is collinear with p1-p2
   */
  public static int computeOrientation(Coordinate p1, Coordinate p2,
      Coordinate q)
  {
    return orientationIndex(p1, p2, q);
  }

  /**
   * Computes the distance from a point p to a line segment AB
   * 
   * Note: NON-ROBUST!
   * 
   * @param p
   *          the point to compute the distance for
   * @param A
   *          one point of the line
   * @param B
   *          another point of the line (must be different to A)
   * @return the distance from p to line segment AB
   */
  public static double distancePointLine(Coordinate p, Coordinate A,
      Coordinate B)
  {
    // if start = end, then just compute distance to one of the endpoints
    if (A.x == B.x && A.y == B.y)
      return p.distance(A);

    // otherwise use comp.graphics.algorithms Frequently Asked Questions method
    /*
     * (1) r = AC dot AB 
     *         --------- 
     *         ||AB||^2 
     *         
     * r has the following meaning: 
     *   r=0 P = A 
     *   r=1 P = B 
     *   r<0 P is on the backward extension of AB 
     *   r>1 P is on the forward extension of AB 
     *   0<r<1 P is interior to AB
     */

    double len2 = (B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y);
    double r = ((p.x - A.x) * (B.x - A.x) + (p.y - A.y) * (B.y - A.y))
        / len2;

    if (r <= 0.0)
      return p.distance(A);
    if (r >= 1.0)
      return p.distance(B);

    /*
     * (2) s = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay) 
     *         ----------------------------- 
     *                    L^2
     * 
     * Then the distance from C to P = |s|*L.
     * 
     * This is the same calculation as {@link #distancePointLinePerpendicular}.
     * Unrolled here for performance.
     */
    double s = ((A.y - p.y) * (B.x - A.x) - (A.x - p.x) * (B.y - A.y))
        / len2;
    return Math.abs(s) * Math.sqrt(len2);
  }

  /**
   * Computes the perpendicular distance from a point p to the (infinite) line
   * containing the points AB
   * 
   * @param p
   *          the point to compute the distance for
   * @param A
   *          one point of the line
   * @param B
   *          another point of the line (must be different to A)
   * @return the distance from p to line AB
   */
  public static double distancePointLinePerpendicular(Coordinate p,
      Coordinate A, Coordinate B)
  {
    // use comp.graphics.algorithms Frequently Asked Questions method
    /*
     * (2) s = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay) 
     *         ----------------------------- 
     *                    L^2
     * 
     * Then the distance from C to P = |s|*L.
     */
    double len2 = (B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y);
    double s = ((A.y - p.y) * (B.x - A.x) - (A.x - p.x) * (B.y - A.y))
        / len2;

    return Math.abs(s) * Math.sqrt(len2);
  }

  /**
   * Computes the distance from a point to a sequence of line segments.
   * 
   * @param p
   *          a point
   * @param line
   *          a sequence of contiguous line segments defined by their vertices
   * @return the minimum distance between the point and the line segments
   */
  public static double distancePointLine(Coordinate p, Coordinate[] line)
  {
    if (line.length == 0)
      throw new IllegalArgumentException(
          "Line array must contain at least one vertex");
    // this handles the case of length = 1
    double minDistance = p.distance(line[0]);
    for (int i = 0; i < line.length - 1; i++) {
      double dist = CGAlgorithms.distancePointLine(p, line[i], line[i + 1]);
      if (dist < minDistance) {
        minDistance = dist;
      }
    }
    return minDistance;
  }

  /**
   * Computes the distance from a line segment AB to a line segment CD
   * 
   * Note: NON-ROBUST!
   * 
   * @param A
   *          a point of one line
   * @param B
   *          the second point of (must be different to A)
   * @param C
   *          one point of the line
   * @param D
   *          another point of the line (must be different to A)
   */
  public static double distanceLineLine(Coordinate A, Coordinate B,
      Coordinate C, Coordinate D)
  {
    // check for zero-length segments
    if (A.equals(B))
      return distancePointLine(A, C, D);
    if (C.equals(D))
      return distancePointLine(D, A, B);

    // AB and CD are line segments
    /*
     * from comp.graphics.algo
     * 
     * Solving the above for r and s yields 
     * 
     *     (Ay-Cy)(Dx-Cx)-(Ax-Cx)(Dy-Cy) 
     * r = ----------------------------- (eqn 1) 
     *     (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx)
     * 
     *     (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)  
     * s = ----------------------------- (eqn 2)
     *     (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx) 
     *     
     * Let P be the position vector of the
     * intersection point, then 
     *   P=A+r(B-A) or 
     *   Px=Ax+r(Bx-Ax) 
     *   Py=Ay+r(By-Ay) 
     * By examining the values of r & s, you can also determine some other limiting
     * conditions: 
     *   If 0<=r<=1 & 0<=s<=1, intersection exists 
     *      r<0 or r>1 or s<0 or s>1 line segments do not intersect 
     *   If the denominator in eqn 1 is zero, AB & CD are parallel 
     *   If the numerator in eqn 1 is also zero, AB & CD are collinear.
     */

    boolean noIntersection = false;
    if (! Envelope.intersects(A, B, C, D)) {
      noIntersection = true;
    }
    else {
      double denom = (B.x - A.x) * (D.y - C.y) - (B.y - A.y) * (D.x - C.x);
      
      if (denom == 0) {
        noIntersection = true;
      }
      else {
        double r_num = (A.y - C.y) * (D.x - C.x) - (A.x - C.x) * (D.y - C.y);
        double s_num = (A.y - C.y) * (B.x - A.x) - (A.x - C.x) * (B.y - A.y);
        
        double s = s_num / denom;
        double r = r_num / denom;
  
        if ((r < 0) || (r > 1) || (s < 0) || (s > 1)) {
          noIntersection = true;
        }
      }
    }
    if (noIntersection) {
      return MathUtil.min(
            distancePointLine(A, C, D),
            distancePointLine(B, C, D),
            distancePointLine(C, A, B),
            distancePointLine(D, A, B));
    }
    // segments intersect
    return 0.0; 
  }

  /**
   * Computes the signed area for a ring. The signed area is positive if the
   * ring is oriented CW, negative if the ring is oriented CCW, and zero if the
   * ring is degenerate or flat.
   * 
   * @param ring
   *          the coordinates forming the ring
   * @return the signed area of the ring
   */
  public static double signedArea(Coordinate[] ring)
  {
    if (ring.length < 3)
      return 0.0;
    double sum = 0.0;
    /**
     * Based on the Shoelace formula.
     * http://en.wikipedia.org/wiki/Shoelace_formula
     */
    double x0 = ring[0].x;
    for (int i = 1; i < ring.length - 1; i++) {
      double x = ring[i].x - x0;
      double y1 = ring[i + 1].y;
      double y2 = ring[i - 1].y;
      sum += x * (y2 - y1);
    }
    return sum / 2.0;
  }

  /**
   * Computes the signed area for a ring. The signed area is:
   * <ul>
   * <li>positive if the ring is oriented CW
   * <li>negative if the ring is oriented CCW
   * <li>zero if the ring is degenerate or flat
   * </ul>
   * 
   * @param ring
   *          the coordinates forming the ring
   * @return the signed area of the ring
   */
  public static double signedArea(CoordinateSequence ring)
  {
    int n = ring.size();
    if (n < 3)
      return 0.0;
    /**
     * Based on the Shoelace formula.
     * http://en.wikipedia.org/wiki/Shoelace_formula
     */
    Coordinate p0 = new Coordinate();
    Coordinate p1 = new Coordinate();
    Coordinate p2 = new Coordinate();
    ring.getCoordinate(0, p1);
    ring.getCoordinate(1, p2);
    double x0 = p1.x;
    p2.x -= x0;
    double sum = 0.0;
    for (int i = 1; i < n - 1; i++) {
      p0.y = p1.y;
      p1.x = p2.x;
      p1.y = p2.y;
      ring.getCoordinate(i + 1, p2);
      p2.x -= x0;
      sum += p1.x * (p0.y - p2.y);
    }
    return sum / 2.0;
  }

  /**
   * Computes the length of a linestring specified by a sequence of points.
   * 
   * @param pts
   *          the points specifying the linestring
   * @return the length of the linestring
   */
  public static double length(CoordinateSequence pts)
  {
    // optimized for processing CoordinateSequences
    int n = pts.size();
    if (n <= 1)
      return 0.0;

    double len = 0.0;

    Coordinate p = new Coordinate();
    pts.getCoordinate(0, p);
    double x0 = p.x;
    double y0 = p.y;

    for (int i = 1; i < n; i++) {
      pts.getCoordinate(i, p);
      double x1 = p.x;
      double y1 = p.y;
      double dx = x1 - x0;
      double dy = y1 - y0;

      len += Math.sqrt(dx * dx + dy * dy);

      x0 = x1;
      y0 = y1;
    }
    return len;
  }

}
