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
import com.vividsolutions.jts.math.DD;

/**
 * Implements basic computational geometry algorithms using {@link DD} arithmetic.
 * 
 * @author Martin Davis
 *
 */
public class CGAlgorithmsDD
{
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
    // fast filter for orientation index
    // avoids use of slow extended-precision arithmetic in many cases
    int index = orientationIndexFilter(p1, p2, q);
    if (index <= 1) return index;
    
    // normalize coordinates
    DD dx1 = DD.valueOf(p2.x).selfAdd(-p1.x);
    DD dy1 = DD.valueOf(p2.y).selfAdd(-p1.y);
    DD dx2 = DD.valueOf(q.x).selfAdd(-p2.x);
    DD dy2 = DD.valueOf(q.y).selfAdd(-p2.y);

    // sign of determinant - unrolled for performance
    return dx1.selfMultiply(dy2).selfSubtract(dy1.selfMultiply(dx2)).signum();
  }
  
  /**
   * Computes the sign of the determinant of the 2x2 matrix
   * with the given entries.
   * 
   * @return -1 if the determinant is negative,
   * @return  1 if the determinant is positive,
   * @return  0 if the determinant is 0.
   */
  public static int signOfDet2x2(DD x1, DD y1, DD x2, DD y2)
  {
    DD det = x1.multiply(y2).selfSubtract(y1.multiply(x2));
    return det.signum();
  }

  /**
   * A value which is safely greater than the
   * relative round-off error in double-precision numbers
   */
  private static final double DP_SAFE_EPSILON = 1e-15;

  /**
   * A filter for computing the orientation index of three coordinates.
   * <p>
   * If the orientation can be computed safely using standard DP
   * arithmetic, this routine returns the orientation index.
   * Otherwise, a value i > 1 is returned.
   * In this case the orientation index must 
   * be computed using some other more robust method.
   * The filter is fast to compute, so can be used to 
   * avoid the use of slower robust methods except when they are really needed,
   * thus providing better average performance.
   * <p>
   * Uses an approach due to Jonathan Shewchuk, which is in the public domain.
   * 
   * @param pa a coordinate
   * @param pb a coordinate
   * @param pc a coordinate
   * @return the orientation index if it can be computed safely
   * @return i > 1 if the orientation index cannot be computed safely
   */
  private static int orientationIndexFilter(Coordinate pa, Coordinate pb, Coordinate pc)
  {
    double detsum;

    double detleft = (pa.x - pc.x) * (pb.y - pc.y);
    double detright = (pa.y - pc.y) * (pb.x - pc.x);
    double det = detleft - detright;

    if (detleft > 0.0) {
      if (detright <= 0.0) {
        return signum(det);
      }
      else {
        detsum = detleft + detright;
      }
    }
    else if (detleft < 0.0) {
      if (detright >= 0.0) {
        return signum(det);
      }
      else {
        detsum = -detleft - detright;
      }
    }
    else {
      return signum(det);
    }

    double errbound = DP_SAFE_EPSILON * detsum;
    if ((det >= errbound) || (-det >= errbound)) {
      return signum(det);
    }

    return 2;
  }

  private static int signum(double x)
  {
    if (x > 0) return 1;
    if (x < 0) return -1;
    return 0;
  }

  /**
   * Computes an intersection point between two lines
   * using DD arithmetic.
   * Currently does not handle case of parallel lines.
   * 
   * @param p1
   * @param p2
   * @param q1
   * @param q2
   * @return
   */
  public static Coordinate intersection(
      Coordinate p1, Coordinate p2,
      Coordinate q1, Coordinate q2)
  {
    DD denom1 = DD.valueOf(q2.y).selfSubtract(q1.y)
    .selfMultiply(DD.valueOf(p2.x).selfSubtract(p1.x));
    DD denom2 = DD.valueOf(q2.x).selfSubtract(q1.x)
    .selfMultiply(DD.valueOf(p2.y).selfSubtract(p1.y));
    DD denom = denom1.subtract(denom2);
    
    /**
     * Cases:
     * - denom is 0 if lines are parallel
     * - intersection point lies within line segment p if fracP is between 0 and 1
     * - intersection point lies within line segment q if fracQ is between 0 and 1
     */
    
    DD numx1 = DD.valueOf(q2.x).selfSubtract(q1.x)
    .selfMultiply(DD.valueOf(p1.y).selfSubtract(q1.y));
    DD numx2 = DD.valueOf(q2.y).selfSubtract(q1.y)
    .selfMultiply(DD.valueOf(p1.x).selfSubtract(q1.x));
    DD numx = numx1.subtract(numx2);
    double fracP = numx.selfDivide(denom).doubleValue();
    
    double x = DD.valueOf(p1.x).selfAdd(DD.valueOf(p2.x).selfSubtract(p1.x).selfMultiply(fracP)).doubleValue();
    
    DD numy1 = DD.valueOf(p2.x).selfSubtract(p1.x)
    .selfMultiply(DD.valueOf(p1.y).selfSubtract(q1.y));
    DD numy2 = DD.valueOf(p2.y).selfSubtract(p1.y)
    .selfMultiply(DD.valueOf(p1.x).selfSubtract(q1.x));
    DD numy = numy1.subtract(numy2);
    double fracQ = numy.selfDivide(denom).doubleValue();
    
    double y = DD.valueOf(q1.y).selfAdd(DD.valueOf(q2.y).selfSubtract(q1.y).selfMultiply(fracQ)).doubleValue();

    return new Coordinate(x,y);
  }
}
