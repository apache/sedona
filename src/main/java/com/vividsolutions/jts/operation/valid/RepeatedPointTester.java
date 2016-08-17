

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
package com.vividsolutions.jts.operation.valid;

import com.vividsolutions.jts.geom.*;

/**
 * Implements the appropriate checks for repeated points
 * (consecutive identical coordinates) as defined in the
 * JTS spec.
 *
 * @version 1.7
 */
public class RepeatedPointTester {

  // save the repeated coord found (if any)
  private Coordinate repeatedCoord;

  public RepeatedPointTester() {
  }

  public Coordinate getCoordinate() { return repeatedCoord; }

  public boolean hasRepeatedPoint(Geometry g)
  {
    if (g.isEmpty()) return false;
    if (g instanceof Point)                   return false;
    else if (g instanceof MultiPoint)         return false;
                        // LineString also handles LinearRings
    else if (g instanceof LineString)         return hasRepeatedPoint(((LineString) g).getCoordinates());
    else if (g instanceof Polygon)            return hasRepeatedPoint((Polygon) g);
    else if (g instanceof GeometryCollection) return hasRepeatedPoint((GeometryCollection) g);
    else  throw new UnsupportedOperationException(g.getClass().getName());
  }

  public boolean hasRepeatedPoint(Coordinate[] coord)
  {
    for (int i = 1; i < coord.length; i++) {
      if (coord[i - 1].equals(coord[i]) ) {
        repeatedCoord = coord[i];
        return true;
      }
    }
    return false;
  }
  private boolean hasRepeatedPoint(Polygon p)
  {
    if (hasRepeatedPoint(p.getExteriorRing().getCoordinates())) return true;
    for (int i = 0; i < p.getNumInteriorRing(); i++) {
      if (hasRepeatedPoint(p.getInteriorRingN(i).getCoordinates())) return true;
    }
    return false;
  }
  private boolean hasRepeatedPoint(GeometryCollection gc)
  {
    for (int i = 0; i < gc.getNumGeometries(); i++) {
      Geometry g = gc.getGeometryN(i);
      if (hasRepeatedPoint(g)) return true;
    }
    return false;
  }


}
