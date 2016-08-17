

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
package com.vividsolutions.jts.util;

import com.vividsolutions.jts.geom.*;

/**
 *  A {@link CoordinateFilter} that creates an array containing every
 *  coordinate in a {@link Geometry}.
 *
 *@version 1.7
 */
public class CoordinateArrayFilter implements CoordinateFilter {
  Coordinate[] pts = null;
  int n = 0;

  /**
   *  Constructs a <code>CoordinateArrayFilter</code>.
   *
   *@param  size  the number of points that the <code>CoordinateArrayFilter</code>
   *      will collect
   */
  public CoordinateArrayFilter(int size) {
    pts = new Coordinate[size];
  }

  /**
   *  Returns the gathered <code>Coordinate</code>s.
   *
   *@return    the <code>Coordinate</code>s collected by this <code>CoordinateArrayFilter</code>
   */
  public Coordinate[] getCoordinates() {
    return pts;
  }

  public void filter(Coordinate coord) {
    pts[n++] = coord;
  }
}

