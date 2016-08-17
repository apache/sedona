

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

import java.util.ArrayList;
import java.util.TreeSet;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;

/**
 *  A {@link CoordinateFilter} that builds a set of <code>Coordinate</code>s.
 *  The set of coordinates contains no duplicate points.
 *  It preserves the order of the input points.
 *
 *@version 1.7
 */
public class UniqueCoordinateArrayFilter implements CoordinateFilter 
{
  /**
   * Convenience method which allows running the filter over an array of {@link Coordinate}s.
   * 
   * @param coords an array of coordinates
   * @return an array of the unique coordinates
   */
  public static Coordinate[] filterCoordinates(Coordinate[] coords)
  {
    UniqueCoordinateArrayFilter filter = new UniqueCoordinateArrayFilter();
    for (int i = 0; i < coords.length; i++) {
      filter.filter(coords[i]);
    }
    return filter.getCoordinates();
  }
  
  TreeSet treeSet = new TreeSet();
  ArrayList list = new ArrayList();

  public UniqueCoordinateArrayFilter() { }

  /**
   *  Returns the gathered <code>Coordinate</code>s.
   *
   *@return    the <code>Coordinate</code>s collected by this <code>CoordinateArrayFilter</code>
   */
  public Coordinate[] getCoordinates() {
    Coordinate[] coordinates = new Coordinate[list.size()];
    return (Coordinate[]) list.toArray(coordinates);
  }

  public void filter(Coordinate coord) {
    if (!treeSet.contains(coord)) {
      list.add(coord);
      treeSet.add(coord);
    }
  }
}

