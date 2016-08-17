
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

import com.vividsolutions.jts.geom.*;

/**
 * Computes the centroid of a point geometry.
 * <h2>Algorithm</h2>
 * Compute the average of all points.
 *
 * @version 1.7
 * @deprecated use Centroid instead
 */
public class CentroidPoint
{
  private int ptCount = 0;
  private Coordinate centSum = new Coordinate();

  public CentroidPoint()
  {
  }

  /**
   * Adds the point(s) defined by a Geometry to the centroid total.
   * If the geometry is not of dimension 0 it does not contribute to the centroid.
   * @param geom the geometry to add
   */
  public void add(Geometry geom)
  {
    if (geom instanceof Point) {
      add(geom.getCoordinate());
    }
    else if (geom instanceof GeometryCollection) {
      GeometryCollection gc = (GeometryCollection) geom;
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        add(gc.getGeometryN(i));
      }
    }
  }

  /**
   * Adds the length defined by an array of coordinates.
   * @param pts an array of {@link Coordinate}s
   */
  public void add(Coordinate pt)
  {
    ptCount += 1;
    centSum.x += pt.x;
    centSum.y += pt.y;
  }

  public Coordinate getCentroid()
  {
    Coordinate cent = new Coordinate();
    cent.x = centSum.x / ptCount;
    cent.y = centSum.y / ptCount;
    return cent;
  }

}
