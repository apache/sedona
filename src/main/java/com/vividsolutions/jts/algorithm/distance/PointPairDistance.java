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

package com.vividsolutions.jts.algorithm.distance;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * Contains a pair of points and the distance between them.
 * Provides methods to update with a new point pair with
 * either maximum or minimum distance.
 */
public class PointPairDistance {

  private Coordinate[] pt = { new Coordinate(), new Coordinate() };
  private double distance = Double.NaN;
  private boolean isNull = true;

  public PointPairDistance()
  {
  }

  public void initialize() { isNull = true; }

  public void initialize(Coordinate p0, Coordinate p1)
  {
    pt[0].setCoordinate(p0);
    pt[1].setCoordinate(p1);
    distance = p0.distance(p1);
    isNull = false;
  }

  /**
   * Initializes the points, avoiding recomputing the distance.
   * @param p0
   * @param p1
   * @param distance the distance between p0 and p1
   */
  private void initialize(Coordinate p0, Coordinate p1, double distance)
  {
    pt[0].setCoordinate(p0);
    pt[1].setCoordinate(p1);
    this.distance = distance;
    isNull = false;
  }

  public double getDistance() { return distance; }

  public Coordinate[] getCoordinates() { return pt; }

  public Coordinate getCoordinate(int i) { return pt[i]; }

  public void setMaximum(PointPairDistance ptDist)
  {
    setMaximum(ptDist.pt[0], ptDist.pt[1]);
  }

  public void setMaximum(Coordinate p0, Coordinate p1)
  {
    if (isNull) {
      initialize(p0, p1);
      return;
    }
    double dist = p0.distance(p1);
    if (dist > distance)
      initialize(p0, p1, dist);
  }

  public void setMinimum(PointPairDistance ptDist)
  {
    setMinimum(ptDist.pt[0], ptDist.pt[1]);
  }

  public void setMinimum(Coordinate p0, Coordinate p1)
  {
    if (isNull) {
      initialize(p0, p1);
      return;
    }
    double dist = p0.distance(p1);
    if (dist < distance)
      initialize(p0, p1, dist);
  }
  
  public String toString()
  {
  	return WKTWriter.toLineString(pt[0], pt[1]);
  }
}
