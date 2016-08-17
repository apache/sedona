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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.math.Vector3D;

/**
 * Basic computational geometry algorithms 
 * for geometry and coordinates defined in 3-dimensional Cartesian space.
 * 
 * @author mdavis
 *
 */
public class CGAlgorithms3D 
{
	public static double distance(Coordinate p0, Coordinate p1)
	{
		// default to 2D distance if either Z is not set
		if (Double.isNaN(p0.z) || Double.isNaN(p1.z))
			return p0.distance(p1);
		
	    double dx = p0.x - p1.x;
	    double dy = p0.y - p1.y;
	    double dz = p0.z - p1.z;
	    return Math.sqrt(dx * dx + dy * dy + dz * dz);
	}

	public static double distancePointSegment(Coordinate p,
			Coordinate A, Coordinate B) {
	    // if start = end, then just compute distance to one of the endpoints
	    if (A.equals3D(B))
	      return distance(p, A);

	    // otherwise use comp.graphics.algorithms Frequently Asked Questions method
	    /*
	     * (1) r = AC dot AB 
	     *         --------- 
	     *         ||AB||^2 
	     *         
	     * r has the following meaning: 
	     *   r=0 P = A 
	     *   r=1 P = B 
	     *   r<0 P is on the backward extension of AB 
	     *   r>1 P is on the forward extension of AB 
	     *   0<r<1 P is interior to AB
	     */

	    double len2 = (B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y) + (B.z - A.z) * (B.z - A.z);
	    if (Double.isNaN(len2))
	    	throw new IllegalArgumentException("Ordinates must not be NaN");
	    double r = ((p.x - A.x) * (B.x - A.x) + (p.y - A.y) * (B.y - A.y) + (p.z - A.z) * (B.z - A.z))
	        / len2;

	    if (r <= 0.0)
	      return distance(p, A);
	    if (r >= 1.0)
	      return distance(p, B);

	    // compute closest point q on line segment
	    double qx = A.x + r * (B.x - A.x);
	    double qy = A.y + r * (B.y - A.y);
	    double qz = A.z + r * (B.z - A.z);
	    // result is distance from p to q
	    double dx = p.x - qx;
	    double dy = p.y - qy;
	    double dz = p.z - qz;
	    return Math.sqrt(dx*dx + dy*dy + dz*dz);
	}
	

	/**
	 * Computes the distance between two 3D segments.
	 * 
	 * @param A the start point of the first segment
	 * @param B the end point of the first segment
	 * @param C the start point of the second segment
	 * @param D the end point of the second segment
	 * @return the distance between the segments
	 */
	public static double distanceSegmentSegment(
			Coordinate A, Coordinate B, Coordinate C, Coordinate D) 
	{
		/**
		 * This calculation is susceptible to roundoff errors when 
		 * passed large ordinate values.
		 * It may be possible to improve this by using {@link DD} arithmetic.
		 */
	    if (A.equals3D(B))
		      return distancePointSegment(A, C, D);
	    if (C.equals3D(B))
		      return distancePointSegment(C, A, B);
	    
	    /**
	     * Algorithm derived from http://softsurfer.com/Archive/algorithm_0106/algorithm_0106.htm
	     */
		double a = Vector3D.dot(A, B, A, B);
		double b = Vector3D.dot(A, B, C, D);
		double c = Vector3D.dot(C, D, C, D);
		double d = Vector3D.dot(A, B, C, A);
		double e = Vector3D.dot(C, D, C, A);
		
		double denom = a*c - b*b;
	    if (Double.isNaN(denom))
	    	throw new IllegalArgumentException("Ordinates must not be NaN");
		
		double s;
		double t;
		if (denom <= 0.0) {
			/**
			 * The lines are parallel. 
			 * In this case solve for the parameters s and t by assuming s is 0.
			 */
			s = 0;
			// choose largest denominator for optimal numeric conditioning
			if (b > c)
				t = d/b;
			else 
				t = e/c;
		}
		else {
			s = (b*e - c*d) / denom;
			t = (a*e - b*d) / denom;
		}
		if (s < 0) 
			return distancePointSegment(A, C, D);
		else if (s > 1)
			return distancePointSegment(B, C, D);
		else if (t < 0)	
			return distancePointSegment(C, A, B);
		else if(t > 1) {
			return distancePointSegment(D, A, B);
		}
		/**
		 * The closest points are in interiors of segments,
		 * so compute them directly
		 */
		double x1 = A.x + s * (B.x - A.x);
		double y1 = A.y + s * (B.y - A.y);
		double z1 = A.z + s * (B.z - A.z);

		double x2 = C.x + t * (D.x - C.x);
		double y2 = C.y + t * (D.y - C.y);
		double z2 = C.z + t * (D.z - C.z);
		
		// length (p1-p2)
		return distance(new Coordinate(x1, y1, z1), new Coordinate(x2, y2, z2));
	}

	
}
