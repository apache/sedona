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
 * Represents a homogeneous coordinate in a 2-D coordinate space.
 * In JTS {@link HCoordinate}s are used as a clean way
 * of computing intersections between line segments.
 *
 * @author David Skea
 * @version 1.7
 */
public class HCoordinate
{

  /**
   * Computes the (approximate) intersection point between two line segments
   * using homogeneous coordinates.
   * <p>
   * Note that this algorithm is
   * not numerically stable; i.e. it can produce intersection points which
   * lie outside the envelope of the line segments themselves.  In order
   * to increase the precision of the calculation input points should be normalized
   * before passing them to this routine.
   */
  public static Coordinate intersection(
      Coordinate p1, Coordinate p2,
      Coordinate q1, Coordinate q2)
      throws NotRepresentableException
  {
  	// unrolled computation
    double px = p1.y - p2.y;
    double py = p2.x - p1.x;
    double pw = p1.x * p2.y - p2.x * p1.y;
    
    double qx = q1.y - q2.y;
    double qy = q2.x - q1.x;
    double qw = q1.x * q2.y - q2.x * q1.y;
    
    double x = py * qw - qy * pw;
    double y = qx * pw - px * qw;
    double w = px * qy - qx * py;
    
    double xInt = x/w;
    double yInt = y/w;
    
    if ((Double.isNaN(xInt)) || (Double.isInfinite(xInt)
    		|| Double.isNaN(yInt)) || (Double.isInfinite(yInt))) {
      throw new NotRepresentableException();
    }
    
    return new Coordinate(xInt, yInt);
  }

  /*
  public static Coordinate OLDintersection(
      Coordinate p1, Coordinate p2,
      Coordinate q1, Coordinate q2)
      throws NotRepresentableException
  {
    HCoordinate l1 = new HCoordinate(p1, p2);
    HCoordinate l2 = new HCoordinate(q1, q2);
    HCoordinate intHCoord = new HCoordinate(l1, l2);
    Coordinate intPt = intHCoord.getCoordinate();
    return intPt;
  }
  */

  public double x,y,w;

  public HCoordinate() {
    x = 0.0;
    y = 0.0;
    w = 1.0;
  }

  public HCoordinate(double _x, double _y, double _w) {
    x = _x;
    y = _y;
    w = _w;
  }

  public HCoordinate(double _x, double _y) {
    x = _x;
    y = _y;
    w = 1.0;
  }

  public HCoordinate(Coordinate p) {
    x = p.x;
    y = p.y;
    w = 1.0;
  }

  public HCoordinate(HCoordinate p1, HCoordinate p2) 
  {
    x = p1.y * p2.w - p2.y * p1.w;
    y = p2.x * p1.w - p1.x * p2.w;
    w = p1.x * p2.y - p2.x * p1.y;
  }

  /**
   * Constructs a homogeneous coordinate which is the intersection of the lines
   * define by the homogenous coordinates represented by two
   * {@link Coordinate}s.
   * 
   * @param p1
   * @param p2
   */
  public HCoordinate(Coordinate p1, Coordinate p2) 
  {
  	// optimization when it is known that w = 1
    x = p1.y - p2.y;
    y = p2.x - p1.x;
    w = p1.x * p2.y - p2.x * p1.y;
  }
  
  public HCoordinate(Coordinate p1, Coordinate p2, Coordinate q1, Coordinate q2) 
  {
  	// unrolled computation
    double px = p1.y - p2.y;
    double py = p2.x - p1.x;
    double pw = p1.x * p2.y - p2.x * p1.y;
    
    double qx = q1.y - q2.y;
    double qy = q2.x - q1.x;
    double qw = q1.x * q2.y - q2.x * q1.y;
    
    x = py * qw - qy * pw;
    y = qx * pw - px * qw;
    w = px * qy - qx * py;
  }
  
  public double getX() throws NotRepresentableException {
    double a = x/w;
    if ((Double.isNaN(a)) || (Double.isInfinite(a))) {
      throw new NotRepresentableException();
    }
    return a;
  }

  public double getY() throws NotRepresentableException {
    double a = y/w;
    if  ((Double.isNaN(a)) || (Double.isInfinite(a))) {
      throw new NotRepresentableException();
    }
    return a;
  }

  public Coordinate getCoordinate() throws NotRepresentableException {
    Coordinate p = new Coordinate();
    p.x = getX();
    p.y = getY();
    return p;
  }
}