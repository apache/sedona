
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
 * Computes a point in the interior of an linear geometry.
 * <h2>Algorithm</h2>
 * <ul>
 * <li>Find an interior vertex which is closest to
 * the centroid of the linestring.
 * <li>If there is no interior vertex, find the endpoint which is
 * closest to the centroid.
 * </ul>
 *
 * @version 1.7
 */
public class InteriorPointLine {

  private Coordinate centroid;
  private double minDistance = Double.MAX_VALUE;

  private Coordinate interiorPoint = null;

  public InteriorPointLine(Geometry g)
  {
    centroid = g.getCentroid().getCoordinate();
    addInterior(g);
    if (interiorPoint == null)
      addEndpoints(g);
  }

  public Coordinate getInteriorPoint()
  {
    return interiorPoint;
  }

  /**
   * Tests the interior vertices (if any)
   * defined by a linear Geometry for the best inside point.
   * If a Geometry is not of dimension 1 it is not tested.
   * @param geom the geometry to add
   */
  private void addInterior(Geometry geom)
  {
    if (geom instanceof LineString) {
      addInterior(geom.getCoordinates());
    }
    else if (geom instanceof GeometryCollection) {
      GeometryCollection gc = (GeometryCollection) geom;
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        addInterior(gc.getGeometryN(i));
      }
    }
  }
  private void addInterior(Coordinate[] pts)
  {
    for (int i = 1; i < pts.length - 1; i++) {
      add(pts[i]);
    }
  }
  /**
   * Tests the endpoint vertices
   * defined by a linear Geometry for the best inside point.
   * If a Geometry is not of dimension 1 it is not tested.
   * @param geom the geometry to add
   */
  private void addEndpoints(Geometry geom)
  {
    if (geom instanceof LineString) {
      addEndpoints(geom.getCoordinates());
    }
    else if (geom instanceof GeometryCollection) {
      GeometryCollection gc = (GeometryCollection) geom;
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        addEndpoints(gc.getGeometryN(i));
      }
    }
  }
  private void addEndpoints(Coordinate[] pts)
  {
    add(pts[0]);
    add(pts[pts.length - 1]);
  }

  private void add(Coordinate point)
  {
    double dist = point.distance(centroid);
    if (dist < minDistance) {
      interiorPoint = new Coordinate(point);
      minDistance = dist;
    }
  }

}
