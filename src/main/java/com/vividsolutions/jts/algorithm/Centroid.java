
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
 * Computes the centroid of a {@link Geometry} of any dimension.
 * If the geometry is nominally of higher dimension, 
 * but has lower <i>effective</i> dimension 
 * (i.e. contains only components
 * having zero length or area), 
 * the centroid will be computed as for the equivalent lower-dimension geometry.
 * If the input geometry is empty, a
 * <code>null</code> Coordinate is returned.
 * 
 * <h2>Algorithm</h2>
 * <ul>
 * <li><b>Dimension 2</b> - the centroid is computed 
 * as the weighted sum of the centroids
 * of a decomposition of the area into (possibly overlapping) triangles.
 * Holes and multipolygons are handled correctly.
 * See <code>http://www.faqs.org/faqs/graphics/algorithms-faq/</code>
 * for further details of the basic approach.
 * 
 * <li><b>Dimension 1</b> - Computes the average of the midpoints
 * of all line segments weighted by the segment length.
 * Zero-length lines are treated as points.
 * 
 * <li><b>Dimension 0</b> - Compute the average coordinate for all points.
 * Repeated points are all included in the average.
 * </ul>
 * 
 * @version 1.7
 */
public class Centroid
{
  /**
   * Computes the centroid point of a geometry.
   * 
   * @param geom the geometry to use
   * @return the centroid point, or null if the geometry is empty
   */
  public static Coordinate getCentroid(Geometry geom)
  {
    Centroid cent = new Centroid(geom);
    return cent.getCentroid();
  }
  
  private Coordinate areaBasePt = null;// the point all triangles are based at
  private Coordinate triangleCent3 = new Coordinate();// temporary variable to hold centroid of triangle
  private double  areasum2 = 0;        /* Partial area sum */
  private Coordinate cg3 = new Coordinate(); // partial centroid sum
  
  // data for linear centroid computation, if needed
  private Coordinate lineCentSum = new Coordinate();
  private double totalLength = 0.0;

  private int ptCount = 0;
  private Coordinate ptCentSum = new Coordinate();

  /**
   * Creates a new instance for computing the centroid of a geometry
   */
  public Centroid(Geometry geom)
  {
    areaBasePt = null;
    add(geom);
  }

  /**
   * Adds a Geometry to the centroid total.
   *
   * @param geom the geometry to add
   */
  private void add(Geometry geom)
  {
    if (geom.isEmpty())
      return;
    if (geom instanceof Point) {
      addPoint(geom.getCoordinate());
    }
    else if (geom instanceof LineString) {
      addLineSegments(geom.getCoordinates());
    }
    else if (geom instanceof Polygon) {
      Polygon poly = (Polygon) geom;
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
   * Gets the computed centroid.
   * 
   * @return the computed centroid, or null if the input is empty
   */
  public Coordinate getCentroid()
  {
    /**
     * The centroid is computed from the highest dimension components present in the input.
     * I.e. areas dominate lineal geometry, which dominates points.
     * Degenerate geometry are computed using their effective dimension
     * (e.g. areas may degenerate to lines or points)
     */
    Coordinate cent = new Coordinate();
    if (Math.abs(areasum2) > 0.0) {
      /**
       * Input contains areal geometry
       */
    	cent.x = cg3.x / 3 / areasum2;
    	cent.y = cg3.y / 3 / areasum2;
    }
    else if (totalLength > 0.0) {
      /**
       * Input contains lineal geometry
       */
      cent.x = lineCentSum.x / totalLength;
      cent.y = lineCentSum.y / totalLength;   	
    }
    else if (ptCount > 0){
      /**
       * Input contains puntal geometry only
       */
      cent.x = ptCentSum.x / ptCount;
      cent.y = ptCentSum.y / ptCount;
    }
    else {
      return null;
    }
    return cent;
  }

  private void setBasePoint(Coordinate basePt)
  {
    if (this.areaBasePt == null)
      this.areaBasePt = basePt;
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
    if (pts.length > 0) 
      setBasePoint(pts[0]);
    boolean isPositiveArea = ! CGAlgorithms.isCCW(pts);
    for (int i = 0; i < pts.length - 1; i++) {
      addTriangle(areaBasePt, pts[i], pts[i+1], isPositiveArea);
    }
    addLineSegments(pts);
  }
  
  private void addHole(Coordinate[] pts)
  {
    boolean isPositiveArea = CGAlgorithms.isCCW(pts);
    for (int i = 0; i < pts.length - 1; i++) {
      addTriangle(areaBasePt, pts[i], pts[i+1], isPositiveArea);
    }
    addLineSegments(pts);
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
   * Computes three times the centroid of the triangle p1-p2-p3.
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
   * Returns twice the signed area of the triangle p1-p2-p3.
   * The area is positive if the triangle is oriented CCW, and negative if CW.
   */
  private static double area2( Coordinate p1, Coordinate p2, Coordinate p3 )
  {
    return
    (p2.x - p1.x) * (p3.y - p1.y) -
        (p3.x - p1.x) * (p2.y - p1.y);
  }

  /**
   * Adds the line segments defined by an array of coordinates
   * to the linear centroid accumulators.
   * 
   * @param pts an array of {@link Coordinate}s
   */
  private void addLineSegments(Coordinate[] pts)
  {
    double lineLen = 0.0;
    for (int i = 0; i < pts.length - 1; i++) {
      double segmentLen = pts[i].distance(pts[i + 1]);
      if (segmentLen == 0.0)
        continue;
      
      lineLen += segmentLen;

      double midx = (pts[i].x + pts[i + 1].x) / 2;
      lineCentSum.x += segmentLen * midx;
      double midy = (pts[i].y + pts[i + 1].y) / 2;
      lineCentSum.y += segmentLen * midy;
    }
    totalLength += lineLen;
    if (lineLen == 0.0 && pts.length > 0)
      addPoint(pts[0]);
  }

  /**
   * Adds a point to the point centroid accumulator.
   * @param pt a {@link Coordinate}
   */
  private void addPoint(Coordinate pt)
  {
    ptCount += 1;
    ptCentSum.x += pt.x;
    ptCentSum.y += pt.y;
  }


}
