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

import com.vividsolutions.jts.geom.Coordinate;
/**
 * Methods for computing and working with octants of the Cartesian plane
 * Octants are numbered as follows:
 * <pre>
 *  \2|1/
 * 3 \|/ 0
 * ---+--
 * 4 /|\ 7
 *  /5|6\
 * <pre>
 * If line segments lie along a coordinate axis, the octant is the lower of the two
 * possible values.
 *
 * @version 1.7
 */
public class Octant {

  /**
   * Returns the octant of a directed line segment (specified as x and y
   * displacements, which cannot both be 0).
   */
  public static int octant(double dx, double dy)
  {
    if (dx == 0.0 && dy == 0.0)
      throw new IllegalArgumentException("Cannot compute the octant for point ( "+ dx + ", " + dy + " )" );

    double adx = Math.abs(dx);
    double ady = Math.abs(dy);

    if (dx >= 0) {
      if (dy >= 0) {
        if (adx >= ady)
          return 0;
        else
          return 1;
      }
      else { // dy < 0
        if (adx >= ady)
          return 7;
        else
          return 6;
      }
    }
    else { // dx < 0
      if (dy >= 0) {
        if (adx >= ady)
          return 3;
        else
          return 2;
      }
      else { // dy < 0
        if (adx >= ady)
          return 4;
        else
          return 5;
      }
    }
  }

  /**
   * Returns the octant of a directed line segment from p0 to p1.
   */
  public static int octant(Coordinate p0, Coordinate p1)
  {
    double dx = p1.x - p0.x;
    double dy = p1.y - p0.y;
    if (dx == 0.0 && dy == 0.0)
      throw new IllegalArgumentException("Cannot compute the octant for two identical points " + p0);
    return octant(dx, dy);
  }

  private Octant() {
  }
}
