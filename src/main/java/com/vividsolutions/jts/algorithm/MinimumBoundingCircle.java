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
import com.vividsolutions.jts.util.*;

/**
 * Computes the <b>Minimum Bounding Circle</b> (MBC)
 * for the points in a {@link Geometry}.
 * The MBC is the smallest circle which <tt>cover</tt>s
 * all the input points 
 * (this is also known as the <b>Smallest Enclosing Circle</b>).
 * This is equivalent to computing the Maximum Diameter 
 * of the input point set.
 * <p>
 * The computed circle can be specified in two equivalent ways,
 * both of which are provide as output by this class:
 * <ul>
 * <li>As a centre point and a radius
 * <li>By the set of points defining the circle.
 * Depending on the number of points in the input
 * and their relative positions, this
 * will be specified by anywhere from 0 to 3 points. 
 * <ul>
 * <li>0 or 1 points indicate an empty or trivial input point arrangement.
 * <li>2 or 3 points define a circle which contains 
 * all the input points.
 * </ul>
 * </ul>
 * The class can also output a {@link Geometry} which approximates the
 * shape of the MBC (although as an approximation 
 * it is <b>not</b> guaranteed to <tt>cover</tt> all the input points.)
 * 
 * @author Martin Davis
 * 
 * @see MinimumDiameter
 *
 */
public class MinimumBoundingCircle 
{
  /*
   * The algorithm used is based on the one by Jon Rokne in 
   * the article "An Easy Bounding Circle" in <i>Graphic Gems II</i>.
   */
	
	private Geometry input;
	private Coordinate[] extremalPts = null;
	private Coordinate centre = null;
	private double radius = 0.0;
	
	/**
	 * Creates a new object for computing the minimum bounding circle for the
	 * point set defined by the vertices of the given geometry.
	 * 
	 * @param geom the geometry to use to obtain the point set 
	 */
	public MinimumBoundingCircle(Geometry geom)
	{
		this.input = geom;
	}
	
	/**
	 * Gets a geometry which represents the Minimum Bounding Circle.
	 * If the input is degenerate (empty or a single unique point),
	 * this method will return an empty geometry or a single Point geometry.
	 * Otherwise, a Polygon will be returned which approximates the 
	 * Minimum Bounding Circle. 
	 * (Note that because the computed polygon is only an approximation, 
	 * it may not precisely contain all the input points.)
	 * 
	 * @return a Geometry representing the Minimum Bounding Circle.
	 */
	public Geometry getCircle()
	{
		//TODO: ensure the output circle contains the extermal points.
		//TODO: or maybe even ensure that the returned geometry contains ALL the input points?
		
		compute();
		if (centre == null)
			return input.getFactory().createPolygon(null, null);
		Point centrePoint = input.getFactory().createPoint(centre);
		if (radius == 0.0)
			return centrePoint;
		return centrePoint.buffer(radius);
	}

  /**
   * Gets a geometry representing a line between the two farthest points
   * in the input.
   * These points will be two of the extremal points of the Minimum Bounding Circle.
   * They also lie on the convex hull of the input.
   * 
   * @return a LineString between the two farthest points of the input
   * @return a empty LineString if the input is empty
   * @return a Point if the input is a point
   */
  public Geometry getFarthestPoints() {
    compute();
    switch (extremalPts.length) {
    case 0:
      return input.getFactory().createLineString((CoordinateSequence) null);
    case 1:
      return input.getFactory().createPoint(centre);
    }
    Coordinate p0 = extremalPts[0];
    Coordinate p1 = extremalPts[extremalPts.length - 1];
    return input.getFactory().createLineString(new Coordinate[] { p0, p1 });
  }

  /**
   * Gets a geometry representing the diameter of the computed Minimum Bounding
   * Circle.
   * 
   * @return the diameter LineString of the Minimum Bounding Circle
   * @return a empty LineString if the input is empty
   * @return a Point if the input is a point
   */
  public Geometry getDiameter() {
    compute();
    switch (extremalPts.length) {
    case 0:
      return input.getFactory().createLineString((CoordinateSequence) null);
    case 1:
      return input.getFactory().createPoint(centre);
    }
    // TODO: handle case of 3 extremal points, by computing a line from one of
    // them through the centre point with len = 2*radius
    Coordinate p0 = extremalPts[0];
    Coordinate p1 = extremalPts[1];
    return input.getFactory().createLineString(new Coordinate[] { p0, p1 });
  }

	/**
	 * Gets the extremal points which define the computed Minimum Bounding Circle.
	 * There may be zero, one, two or three of these points,
	 * depending on the number of points in the input
	 * and the geometry of those points.
	 * 
	 * @return the points defining the Minimum Bounding Circle
	 */
	public Coordinate[] getExtremalPoints() 
	{
		compute();
		return extremalPts;
	}
	
        /**
         * Gets the centre point of the computed Minimum Bounding Circle.
         * 
         * @return the centre point of the Minimum Bounding Circle
         * @return null if the input is empty
         */
        public Coordinate getCentre() 
        {
                compute();
                return centre;
        }
                
	/**
	 * Gets the radius of the computed Minimum Bounding Circle.
	 * 
	 * @return the radius of the Minimum Bounding Circle
	 */
	public double getRadius() 
	{
		compute();
		return radius;
	}
	
	private void computeCentre() 
	{
		switch (extremalPts.length) {
		case 0:
			centre = null;
			break;
		case 1:
			centre = extremalPts[0];
			break;
		case 2:
			centre = new Coordinate(
					(extremalPts[0].x + extremalPts[1].x) / 2.0,
					(extremalPts[0].y + extremalPts[1].y) / 2.0
					);
			break;
		case 3:
			centre = Triangle.circumcentre(extremalPts[0], extremalPts[1], extremalPts[2]);
			break;
		}
	}
	
	private void compute()
	{		
		if (extremalPts != null) return;

		computeCirclePoints();
		computeCentre();
		if (centre != null)
			radius = centre.distance(extremalPts[0]);
	}
	
	private void computeCirclePoints()
	{
		// handle degenerate or trivial cases
		if (input.isEmpty()) {
			extremalPts = new Coordinate[0];
			return;
		}
		if (input.getNumPoints() == 1) {
			Coordinate[] pts = input.getCoordinates();
			extremalPts = new Coordinate[] { new Coordinate(pts[0]) };
			return;
		}
		
		/**
		 * The problem is simplified by reducing to the convex hull.
		 * Computing the convex hull also has the useful effect of eliminating duplicate points
		 */
		Geometry convexHull = input.convexHull();
		
		Coordinate[] hullPts = convexHull.getCoordinates();
		
		// strip duplicate final point, if any
		Coordinate[] pts = hullPts;
		if (hullPts[0].equals2D(hullPts[hullPts.length - 1])) {
			pts = new Coordinate[hullPts.length - 1];
			CoordinateArrays.copyDeep(hullPts, 0, pts, 0, hullPts.length - 1);
		}
		
		/**
		 * Optimization for the trivial case where the CH has fewer than 3 points
		 */
		if (pts.length <= 2) {
			extremalPts = CoordinateArrays.copyDeep(pts);
			return;
		}
		
		// find a point P with minimum Y ordinate
		Coordinate P = lowestPoint(pts);
		
		// find a point Q such that the angle that PQ makes with the x-axis is minimal
		Coordinate Q = pointWitMinAngleWithX(pts, P);
		
		/**
		 * Iterate over the remaining points to find 
		 * a pair or triplet of points which determine the minimal circle.
		 * By the design of the algorithm, 
		 * at most <tt>pts.length</tt> iterations are required to terminate 
		 * with a correct result.
		 */ 
		for (int i = 0; i < pts.length; i++) {
			Coordinate R = pointWithMinAngleWithSegment(pts, P, Q);
			
			// if PRQ is obtuse, then MBC is determined by P and Q
			if (Angle.isObtuse(P, R, Q)) {
				extremalPts = new Coordinate[] { new Coordinate(P), new Coordinate(Q) };
				return;
			}
			// if RPQ is obtuse, update baseline and iterate
			if (Angle.isObtuse(R, P, Q)) {
				P = R;
				continue;
			}
			// if RQP is obtuse, update baseline and iterate
			if (Angle.isObtuse(R, Q, P)) {
				Q = R;
				continue;
			}
			// otherwise all angles are acute, and the MBC is determined by the triangle PQR
			extremalPts = new Coordinate[] { new Coordinate(P), new Coordinate(Q), new Coordinate(R) };
			return;
		}
		Assert.shouldNeverReachHere("Logic failure in Minimum Bounding Circle algorithm!"); 
	}
	
	private static Coordinate lowestPoint(Coordinate[] pts)
	{
		Coordinate min = pts[0];
		for (int i = 1; i < pts.length; i++) {
			if (pts[i].y < min.y)
				min = pts[i];
		}
		return min;
	}
	
	private static Coordinate pointWitMinAngleWithX(Coordinate[] pts, Coordinate P)
	{
		double minSin = Double.MAX_VALUE;
		Coordinate minAngPt = null;
		for (int i = 0; i < pts.length; i++) {
			
			Coordinate p = pts[i];
			if (p == P) continue;
			
			/**
			 * The sin of the angle is a simpler proxy for the angle itself
			 */
			double dx = p.x - P.x;
			double dy = p.y - P.y;
			if (dy < 0) dy = -dy;
			double len = Math.sqrt(dx * dx + dy * dy);
			double sin = dy / len;
			
			if (sin < minSin) {
				minSin = sin;
				minAngPt = p;
			}
		}
		return minAngPt;
	}
	
	private static Coordinate pointWithMinAngleWithSegment(Coordinate[] pts, Coordinate P, Coordinate Q)
	{
		double minAng = Double.MAX_VALUE;
		Coordinate minAngPt = null;
		for (int i = 0; i < pts.length; i++) {
			
			Coordinate p = pts[i];
			if (p == P) continue;
			if (p == Q) continue;
			
			double ang = Angle.angleBetween(P, p, Q);
			if (ang < minAng) {
				minAng = ang;
				minAngPt = p;
			}
		}
		return minAngPt;
		
	}
}
