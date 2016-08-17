
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
 * Computes the centroid of an area geometry.
 * <h2>Algorithm</h2>
 * Based on the usual algorithm for calculating
 * the centroid as a weighted sum of the centroids
 * of a decomposition of the area into (possibly overlapping) triangles.
 * The algorithm has been extended to handle holes and multi-polygons.
 * See <code>http://www.faqs.org/faqs/graphics/algorithms-faq/</code>
 * for further details of the basic approach.
 * The code has also be extended to handle degenerate (zero-area) polygons.
 * In this case, the centroid of the line segments in the polygon 
 * will be returned.
 *
 * @version 1.7
 * @deprecated use Centroid instead
 */
public class CentroidArea
{
  private Coordinate basePt = null;// the point all triangles are based at
  private Coordinate triangleCent3 = new Coordinate();// temporary variable to hold centroid of triangle
  private double  areasum2 = 0;        /* Partial area sum */
  private Coordinate cg3 = new Coordinate(); // partial centroid sum
  
  // data for linear centroid computation, if needed
  private Coordinate centSum = new Coordinate();
  private double totalLength = 0.0;

  public CentroidArea()
  {
    basePt = null;
  }

  /**
   * Adds the area defined by a Geometry to the centroid total.
   * If the geometry has no area it does not contribute to the centroid.
   *
   * @param geom the geometry to add
   */
  public void add(Geometry geom)
  {
    if (geom instanceof Polygon) {
      Polygon poly = (Polygon) geom;
      setBasePoint(poly.getExteriorRing().getCoordinateN(0));
      add(poly);
    }
    else if (geom instanceof GeometryCollection) {
      GeometryCollection gc = (GeometryCollection) geom;
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        add(gc.getGeometryN(i));
      }
    }
  }

  /**
   * Adds the area defined by an array of
   * coordinates.  The array must be a ring;
   * i.e. end with the same coordinate as it starts with.
   * @param ring an array of {@link Coordinate}s
   */
  public void add(Coordinate[] ring)
  {
    setBasePoint(ring[0]);
    addShell(ring);
  }

  public Coordinate getCentroid()
  {
    Coordinate cent = new Coordinate();
    if (Math.abs(areasum2) > 0.0) {
    	cent.x = cg3.x / 3 / areasum2;
    	cent.y = cg3.y / 3 / areasum2;
    }
    else {
    	// if polygon was degenerate, compute linear centroid instead
      cent.x = centSum.x / totalLength;
      cent.y = centSum.y / totalLength;   	
    }
    return cent;
  }

  private void setBasePoint(Coordinate basePt)
  {
    if (this.basePt == null)
      this.basePt = basePt;
  }
  
  private void add(Polygon poly)
  {
    addShell(poly.getExteriorRing().getCoordinates());
    for (int i = 0; i < poly.getNumInteriorRing(); i++) {
      addHole(poly.getInteriorRingN(i).getCoordinates());
    }
  }

  private void addShell(Coordinate[] pts)
  {
    boolean isPositiveArea = ! CGAlgorithms.isCCW(pts);
    for (int i = 0; i < pts.length - 1; i++) {
      addTriangle(basePt, pts[i], pts[i+1], isPositiveArea);
    }
    addLinearSegments(pts);
  }
  private void addHole(Coordinate[] pts)
  {
    boolean isPositiveArea = CGAlgorithms.isCCW(pts);
    for (int i = 0; i < pts.length - 1; i++) {
      addTriangle(basePt, pts[i], pts[i+1], isPositiveArea);
    }
    addLinearSegments(pts);
  }
  private void addTriangle(Coordinate p0, Coordinate p1, Coordinate p2, boolean isPositiveArea)
  {
    double sign = (isPositiveArea) ? 1.0 : -1.0;
    centroid3( p0, p1, p2, triangleCent3 );
    double area2 =  area2( p0, p1, p2 );
    cg3.x += sign * area2 * triangleCent3.x;
    cg3.y += sign * area2 * triangleCent3.y;
    areasum2 += sign * area2;
  }
  /**
   * Returns three times the centroid of the triangle p1-p2-p3.
   * The factor of 3 is
   * left in to permit division to be avoided until later.
   */
  private static void centroid3( Coordinate p1, Coordinate p2, Coordinate p3, Coordinate c )
  {
    c.x = p1.x + p2.x + p3.x;
    c.y = p1.y + p2.y + p3.y;
    return;
  }

  /**
   * Returns twice the signed area of the triangle p1-p2-p3,
   * positive if a,b,c are oriented ccw, and negative if cw.
   */
  private static double area2( Coordinate p1, Coordinate p2, Coordinate p3 )
  {
    return
    (p2.x - p1.x) * (p3.y - p1.y) -
        (p3.x - p1.x) * (p2.y - p1.y);
  }

  /**
   * Adds the linear segments defined by an array of coordinates
   * to the linear centroid accumulators.
   * This is done in case the polygon(s) have zero-area, 
   * in which case the linear centroid is computed instead.
   * 
   * @param pts an array of {@link Coordinate}s
   */
  private void addLinearSegments(Coordinate[] pts)
  {
    for (int i = 0; i < pts.length - 1; i++) {
      double segmentLen = pts[i].distance(pts[i + 1]);
      totalLength += segmentLen;

      double midx = (pts[i].x + pts[i + 1].x) / 2;
      centSum.x += segmentLen * midx;
      double midy = (pts[i].y + pts[i + 1].y) / 2;
      centSum.y += segmentLen * midy;
    }
  }


}
