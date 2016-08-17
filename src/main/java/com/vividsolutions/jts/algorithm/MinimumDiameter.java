
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
 * Computes the minimum diameter of a {@link Geometry}.
 * The minimum diameter is defined to be the
 * width of the smallest band that
 * contains the geometry,
 * where a band is a strip of the plane defined
 * by two parallel lines.
 * This can be thought of as the smallest hole that the geometry can be
 * moved through, with a single rotation.
 * <p>
 * The first step in the algorithm is computing the convex hull of the Geometry.
 * If the input Geometry is known to be convex, a hint can be supplied to
 * avoid this computation.
 * <p>
 * This class can also be used to compute a line segment representing 
 * the minimum diameter, the supporting line segment of the minimum diameter,
 * and a minimum rectangle enclosing the input geometry.
 * This rectangle will
 * have width equal to the minimum diameter, and have one side
 * parallel to the supporting segment.
 *
 * @see ConvexHull
 *
 * @version 1.7
 */
public class MinimumDiameter
{
  /**
   * Gets the minimum rectangle enclosing a geometry.
   * 
   * @param geom the geometry
   * @return the minimum rectangle enclosing the geometry
   */
  public static Geometry getMinimumRectangle(Geometry geom) {
    return (new MinimumDiameter(geom)).getMinimumRectangle();
  }
  
  /**
   * Gets the length of the minimum diameter enclosing a geometry
   * @param geom the geometry
   * @return the length of the minimum diameter of the geometry
   */
  public static Geometry getMinimumDiameter(Geometry geom) {
    return (new MinimumDiameter(geom)).getDiameter();
  }
  
  private final Geometry inputGeom;
  private final boolean isConvex;

  private Coordinate[] convexHullPts = null;
  private LineSegment minBaseSeg = new LineSegment();
  private Coordinate minWidthPt = null;
  private int minPtIndex;
  private double minWidth = 0.0;

  /**
   * Compute a minimum diameter for a given {@link Geometry}.
   *
   * @param geom a Geometry
   */
  public MinimumDiameter(Geometry inputGeom)
  {
    this(inputGeom, false);
  }

  /**
   * Compute a minimum diameter for a giver {@link Geometry},
   * with a hint if
   * the Geometry is convex
   * (e.g. a convex Polygon or LinearRing,
   * or a two-point LineString, or a Point).
   *
   * @param geom a Geometry which is convex
   * @param isConvex <code>true</code> if the input geometry is convex
   */
  public MinimumDiameter(Geometry inputGeom, boolean isConvex)
  {
    this.inputGeom = inputGeom;
    this.isConvex = isConvex;
  }

  /**
   * Gets the length of the minimum diameter of the input Geometry
   *
   * @return the length of the minimum diameter
   */
  public double getLength()
  {
    computeMinimumDiameter();
    return minWidth;
  }

  /**
   * Gets the {@link Coordinate} forming one end of the minimum diameter
   *
   * @return a coordinate forming one end of the minimum diameter
   */
  public Coordinate getWidthCoordinate()
  {
    computeMinimumDiameter();
    return minWidthPt;
  }

  /**
   * Gets the segment forming the base of the minimum diameter
   *
   * @return the segment forming the base of the minimum diameter
   */
  public LineString getSupportingSegment()
  {
    computeMinimumDiameter();
    return inputGeom.getFactory().createLineString(new Coordinate[] { minBaseSeg.p0, minBaseSeg.p1 } );
  }

  /**
   * Gets a {@link LineString} which is a minimum diameter
   *
   * @return a {@link LineString} which is a minimum diameter
   */
  public LineString getDiameter()
  {
    computeMinimumDiameter();

    // return empty linestring if no minimum width calculated
    if (minWidthPt == null)
      return inputGeom.getFactory().createLineString((Coordinate[])null);

    Coordinate basePt = minBaseSeg.project(minWidthPt);
    return inputGeom.getFactory().createLineString(new Coordinate[] { basePt, minWidthPt } );
  }

  private void computeMinimumDiameter()
  {
    // check if computation is cached
    if (minWidthPt != null)
      return;

    if (isConvex)
      computeWidthConvex(inputGeom);
    else {
      Geometry convexGeom = (new ConvexHull(inputGeom)).getConvexHull();
      computeWidthConvex(convexGeom);
    }
  }

  private void computeWidthConvex(Geometry convexGeom)
  {
//System.out.println("Input = " + geom);
    if (convexGeom instanceof Polygon)
      convexHullPts = ((Polygon) convexGeom).getExteriorRing().getCoordinates();
    else
      convexHullPts = convexGeom.getCoordinates();

    // special cases for lines or points or degenerate rings
    if (convexHullPts.length == 0) {
      minWidth = 0.0;
      minWidthPt = null;
      minBaseSeg = null;
    }
    else if (convexHullPts.length == 1) {
      minWidth = 0.0;
      minWidthPt = convexHullPts[0];
      minBaseSeg.p0 = convexHullPts[0];
      minBaseSeg.p1 = convexHullPts[0];
    }
    else if (convexHullPts.length == 2 || convexHullPts.length == 3) {
      minWidth = 0.0;
      minWidthPt = convexHullPts[0];
      minBaseSeg.p0 = convexHullPts[0];
      minBaseSeg.p1 = convexHullPts[1];
    }
    else
      computeConvexRingMinDiameter(convexHullPts);
  }

  /**
   * Compute the width information for a ring of {@link Coordinate}s.
   * Leaves the width information in the instance variables.
   *
   * @param pts
   */
  private void computeConvexRingMinDiameter(Coordinate[] pts)
  {
    // for each segment in the ring
    minWidth = Double.MAX_VALUE;
    int currMaxIndex = 1;

    LineSegment seg = new LineSegment();
    // compute the max distance for all segments in the ring, and pick the minimum
    for (int i = 0; i < pts.length - 1; i++) {
      seg.p0 = pts[i];
      seg.p1 = pts[i + 1];
      currMaxIndex = findMaxPerpDistance(pts, seg, currMaxIndex);
    }
  }

  private int findMaxPerpDistance(Coordinate[] pts, LineSegment seg, int startIndex)
  {
    double maxPerpDistance = seg.distancePerpendicular(pts[startIndex]);
    double nextPerpDistance = maxPerpDistance;
    int maxIndex = startIndex;
    int nextIndex = maxIndex;
    while (nextPerpDistance >= maxPerpDistance) {
      maxPerpDistance = nextPerpDistance;
      maxIndex = nextIndex;

      nextIndex = nextIndex(pts, maxIndex);
      nextPerpDistance = seg.distancePerpendicular(pts[nextIndex]);
    }
    // found maximum width for this segment - update global min dist if appropriate
    if (maxPerpDistance < minWidth) {
      minPtIndex = maxIndex;
      minWidth = maxPerpDistance;
      minWidthPt = pts[minPtIndex];
      minBaseSeg = new LineSegment(seg);
//      System.out.println(minBaseSeg);
//      System.out.println(minWidth);
    }
    return maxIndex;
  }

  private static int nextIndex(Coordinate[] pts, int index)
  {
    index++;
    if (index >= pts.length) index = 0;
    return index;
  }
  
  /**
   * Gets the minimum rectangular {@link Polygon} which encloses the input geometry.
   * The rectangle has width equal to the minimum diameter, 
   * and a longer length.
   * If the convex hull of the input is degenerate (a line or point)
   * a {@link LineString} or {@link Point} is returned.
   * <p>
   * The minimum rectangle can be used as an extremely generalized representation
   * for the given geometry.
   * 
   * @return the minimum rectangle enclosing the input (or a line or point if degenerate)
   */
  public Geometry getMinimumRectangle()
  {
    computeMinimumDiameter();
  
    // check if minimum rectangle is degenerate (a point or line segment)
    if (minWidth == 0.0) {
      if (minBaseSeg.p0.equals2D(minBaseSeg.p1)) {
        return inputGeom.getFactory().createPoint(minBaseSeg.p0);
      }
      return minBaseSeg.toGeometry(inputGeom.getFactory());
    }
    
    // deltas for the base segment of the minimum diameter
    double dx = minBaseSeg.p1.x - minBaseSeg.p0.x;
    double dy = minBaseSeg.p1.y - minBaseSeg.p0.y;
    
    /*
    double c0 = computeC(dx, dy, minBaseSeg.p0);
    double c1 = computeC(dx, dy, minBaseSeg.p1);
    */
    
    double minPara = Double.MAX_VALUE;
    double maxPara = -Double.MAX_VALUE;
    double minPerp = Double.MAX_VALUE;
    double maxPerp = -Double.MAX_VALUE;
    
    // compute maxima and minima of lines parallel and perpendicular to base segment
    for (int i = 0; i < convexHullPts.length; i++) {
      
      double paraC = computeC(dx, dy, convexHullPts[i]);
      if (paraC > maxPara) maxPara = paraC;
      if (paraC < minPara) minPara = paraC;
      
      double perpC = computeC(-dy, dx, convexHullPts[i]);
      if (perpC > maxPerp) maxPerp = perpC;
      if (perpC < minPerp) minPerp = perpC;
    }
    
    // compute lines along edges of minimum rectangle
    LineSegment maxPerpLine = computeSegmentForLine(-dx, -dy, maxPerp);
    LineSegment minPerpLine = computeSegmentForLine(-dx, -dy, minPerp);
    LineSegment maxParaLine = computeSegmentForLine(-dy, dx, maxPara);
    LineSegment minParaLine = computeSegmentForLine(-dy, dx, minPara);
    
    // compute vertices of rectangle (where the para/perp max & min lines intersect)
    Coordinate p0 = maxParaLine.lineIntersection(maxPerpLine);
    Coordinate p1 = minParaLine.lineIntersection(maxPerpLine);
    Coordinate p2 = minParaLine.lineIntersection(minPerpLine);
    Coordinate p3 = maxParaLine.lineIntersection(minPerpLine);
    
    LinearRing shell = inputGeom.getFactory().createLinearRing(
        new Coordinate[] { p0, p1, p2, p3, p0 });
    return inputGeom.getFactory().createPolygon(shell, null);

  }
  
  private static double computeC(double a, double b, Coordinate p)
  {
    return a * p.y - b * p.x;
  }
  
  private static LineSegment computeSegmentForLine(double a, double b, double c)
  {
    Coordinate p0;
    Coordinate p1;
    /*
    * Line eqn is ax + by = c
    * Slope is a/b.
    * If slope is steep, use y values as the inputs
    */
    if (Math.abs(b) > Math.abs(a)) {
      p0 = new Coordinate(0.0, c/b);
      p1 = new Coordinate(1.0, c/b - a/b);
    }
    else {
      p0 = new Coordinate(c/a, 0.0);
      p1 = new Coordinate(c/a - b/a, 1.0);
    }
    return new LineSegment(p0, p1);
  }
}
