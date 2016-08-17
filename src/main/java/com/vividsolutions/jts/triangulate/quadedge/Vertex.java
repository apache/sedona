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

package com.vividsolutions.jts.triangulate.quadedge;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Triangle;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.algorithm.*;

/**
 * Models a site (node) in a {@link QuadEdgeSubdivision}. 
 * The sites can be points on a line string representing a
 * linear site. 
 * <p>
 * The vertex can be considered as a vector with a norm, length, inner product, cross
 * product, etc. Additionally, point relations (e.g., is a point to the left of a line, the circle
 * defined by this point and two others, etc.) are also defined in this class.
 * <p>
 * It is common to want to attach user-defined data to 
 * the vertices of a subdivision.  
 * One way to do this is to subclass <tt>Vertex</tt>
 * to carry any desired information.
 * 
 * @author David Skea
 * @author Martin Davis
 */
public class Vertex 
{
    public static final int LEFT        = 0;
    public static final int RIGHT       = 1;
    public static final int BEYOND      = 2;
    public static final int BEHIND      = 3;
    public static final int BETWEEN     = 4;
    public static final int ORIGIN      = 5;
    public static final int DESTINATION = 6;

    private Coordinate      p;
    // private int edgeNumber = -1;

    public Vertex(double _x, double _y) {
        p = new Coordinate(_x, _y);
    }

    public Vertex(double _x, double _y, double _z) {
        p = new Coordinate(_x, _y, _z);
    }

    public Vertex(Coordinate _p) {
        p = new Coordinate(_p);
    }

    public double getX() {
        return p.x;
    }

    public double getY() {
        return p.y;
    }

    public double getZ() {
        return p.z;
    }

    public void setZ(double _z) {
        p.z = _z;
    }

    public Coordinate getCoordinate() {
        return p;
    }

    public String toString() {
        return "POINT (" + p.x + " " + p.y + ")";
    }

    public boolean equals(Vertex _x) {
        if (p.x == _x.getX() && p.y == _x.getY()) {
            return true;
        } else {
            return false;
        }
    }

    public boolean equals(Vertex _x, double tolerance) {
        if (p.distance(_x.getCoordinate()) < tolerance) {
            return true;
        } else {
            return false;
        }
    }

    public int classify(Vertex p0, Vertex p1) {
        Vertex p2 = this;
        Vertex a = p1.sub(p0);
        Vertex b = p2.sub(p0);
        double sa = a.crossProduct(b);
        if (sa > 0.0)
            return LEFT;
        if (sa < 0.0)
            return RIGHT;
        if ((a.getX() * b.getX() < 0.0) || (a.getY() * b.getY() < 0.0))
            return BEHIND;
        if (a.magn() < b.magn())
            return BEYOND;
        if (p0.equals(p2))
            return ORIGIN;
        if (p1.equals(p2))
            return DESTINATION;
        return BETWEEN;
    }

    /**
     * Computes the cross product k = u X v.
     * 
     * @param v a vertex
     * @return returns the magnitude of u X v
     */
    double crossProduct(Vertex v) {
        return (p.x * v.getY() - p.y * v.getX());
    }

    /**
     * Computes the inner or dot product
     * 
     * @param v a vertex
     * @return returns the dot product u.v
     */
    double dot(Vertex v) {
        return (p.x * v.getX() + p.y * v.getY());
    }

    /**
     * Computes the scalar product c(v)
     * 
     * @param v a vertex
     * @return returns the scaled vector
     */
    Vertex times(double c) {
        return (new Vertex(c * p.x, c * p.y));
    }

    /* Vector addition */
    Vertex sum(Vertex v) {
        return (new Vertex(p.x + v.getX(), p.y + v.getY()));
    }

    /* and subtraction */
    Vertex sub(Vertex v) {
        return (new Vertex(p.x - v.getX(), p.y - v.getY()));
    }

    /* magnitude of vector */
    double magn() {
        return (Math.sqrt(p.x * p.x + p.y * p.y));
    }

    /* returns k X v (cross product). this is a vector perpendicular to v */
    Vertex cross() {
        return (new Vertex(p.y, -p.x));
    }

  /** ************************************************************* */
  /***********************************************************************************************
   * Geometric primitives /
   **********************************************************************************************/

  /**
   * Tests if the vertex is inside the circle defined by 
   * the triangle with vertices a, b, c (oriented counter-clockwise). 
   * 
   * @param a a vertex of the triangle
   * @param b a vertex of the triangle
   * @param c a vertex of the triangle
   * @return true if this vertex is in the circumcircle of (a,b,c)
   */
  public boolean isInCircle(Vertex a, Vertex b, Vertex c) 
  {
    return TrianglePredicate.isInCircleRobust(a.p, b.p, c.p, this.p);
    // non-robust - best to not use
    //return TrianglePredicate.isInCircle(a.p, b.p, c.p, this.p);
  }

  /**
   * Tests whether the triangle formed by this vertex and two
   * other vertices is in CCW orientation.
   * 
   * @param b a vertex
   * @param c a vertex
   * @returns true if the triangle is oriented CCW
   */
  public final boolean isCCW(Vertex b, Vertex c) 
  {
      /*
      // test code used to check for robustness of triArea 
      boolean isCCW = (b.p.x - p.x) * (c.p.y - p.y) 
      - (b.p.y - p.y) * (c.p.x - p.x) > 0;
     //boolean isCCW = triArea(this, b, c) > 0;
     boolean isCCWRobust = CGAlgorithms.orientationIndex(p, b.p, c.p) == CGAlgorithms.COUNTERCLOCKWISE; 
     if (isCCWRobust != isCCW)
      System.out.println("CCW failure");
     //*/

    	// is equal to the signed area of the triangle
    	
      return (b.p.x - p.x) * (c.p.y - p.y) 
           - (b.p.y - p.y) * (c.p.x - p.x) > 0;
      
      // original rolled code
      //boolean isCCW = triArea(this, b, c) > 0;
      //return isCCW;
      
    }

    public final boolean rightOf(QuadEdge e) {
        return isCCW(e.dest(), e.orig());
    }

    public final boolean leftOf(QuadEdge e) {
        return isCCW(e.orig(), e.dest());
    }

    private HCoordinate bisector(Vertex a, Vertex b) {
        // returns the perpendicular bisector of the line segment ab
        double dx = b.getX() - a.getX();
        double dy = b.getY() - a.getY();
        HCoordinate l1 = new HCoordinate(a.getX() + dx / 2.0, a.getY() + dy / 2.0, 1.0);
        HCoordinate l2 = new HCoordinate(a.getX() - dy + dx / 2.0, a.getY() + dx + dy / 2.0, 1.0);
        return new HCoordinate(l1, l2);
    }

    private double distance(Vertex v1, Vertex v2) {
        return Math.sqrt(Math.pow(v2.getX() - v1.getX(), 2.0)
                + Math.pow(v2.getY() - v1.getY(), 2.0));
    }

    /**
     * Computes the value of the ratio of the circumradius to shortest edge. If smaller than some
     * given tolerance B, the associated triangle is considered skinny. For an equal lateral
     * triangle this value is 0.57735. The ratio is related to the minimum triangle angle theta by:
     * circumRadius/shortestEdge = 1/(2sin(theta)).
     * 
     * @param b second vertex of the triangle
     * @param c third vertex of the triangle
     * @return ratio of circumradius to shortest edge.
     */
    public double circumRadiusRatio(Vertex b, Vertex c) {
        Vertex x = this.circleCenter(b, c);
        double radius = distance(x, b);
        double edgeLength = distance(this, b);
        double el = distance(b, c);
        if (el < edgeLength) {
            edgeLength = el;
        }
        el = distance(c, this);
        if (el < edgeLength) {
            edgeLength = el;
        }
        return radius / edgeLength;
    }

    /**
     * returns a new vertex that is mid-way between this vertex and another end point.
     * 
     * @param a the other end point.
     * @return the point mid-way between this and that.
     */
    public Vertex midPoint(Vertex a) {
        double xm = (p.x + a.getX()) / 2.0;
        double ym = (p.y + a.getY()) / 2.0;
        double zm = (p.z + a.getZ()) / 2.0;
        return new Vertex(xm, ym, zm);
    }

    /**
     * Computes the centre of the circumcircle of this vertex and two others.
     * 
     * @param b
     * @param c
     * @return the Coordinate which is the circumcircle of the 3 points.
     */
    public Vertex circleCenter(Vertex b, Vertex c) {
        Vertex a = new Vertex(this.getX(), this.getY());
        // compute the perpendicular bisector of cord ab
        HCoordinate cab = bisector(a, b);
        // compute the perpendicular bisector of cord bc
        HCoordinate cbc = bisector(b, c);
        // compute the intersection of the bisectors (circle radii)
        HCoordinate hcc = new HCoordinate(cab, cbc);
        Vertex cc = null;
        try {
            cc = new Vertex(hcc.getX(), hcc.getY());
        } catch (NotRepresentableException nre) {
            System.err.println("a: " + a + "  b: " + b + "  c: " + c);
            System.err.println(nre);
        }
        return cc;
    }

    /**
     * For this vertex enclosed in a triangle defined by three vertices v0, v1 and v2, interpolate
     * a z value from the surrounding vertices.
     */
    public double interpolateZValue(Vertex v0, Vertex v1, Vertex v2) {
        double x0 = v0.getX();
        double y0 = v0.getY();
        double a = v1.getX() - x0;
        double b = v2.getX() - x0;
        double c = v1.getY() - y0;
        double d = v2.getY() - y0;
        double det = a * d - b * c;
        double dx = this.getX() - x0;
        double dy = this.getY() - y0;
        double t = (d * dx - b * dy) / det;
        double u = (-c * dx + a * dy) / det;
        double z = v0.getZ() + t * (v1.getZ() - v0.getZ()) + u * (v2.getZ() - v0.getZ());
        return z;
    }

    /**
     * Interpolates the Z-value (height) of a point enclosed in a triangle
     * whose vertices all have Z values.
     * The containing triangle must not be degenerate
     * (in other words, the three vertices must enclose a 
     * non-zero area).
     * 
     * @param p the point to interpolate the Z value of
     * @param v0 a vertex of a triangle containing the p
     * @param v1 a vertex of a triangle containing the p
     * @param v2 a vertex of a triangle containing the p
     * @return the interpolated Z-value (height) of the point  
     */
    public static double interpolateZ(Coordinate p, Coordinate v0, Coordinate v1, Coordinate v2) {
        double x0 = v0.x;
        double y0 = v0.y;
        double a = v1.x - x0;
        double b = v2.x - x0;
        double c = v1.y - y0;
        double d = v2.y - y0;
        double det = a * d - b * c;
        double dx = p.x - x0;
        double dy = p.y - y0;
        double t = (d * dx - b * dy) / det;
        double u = (-c * dx + a * dy) / det;
        double z = v0.z + t * (v1.z - v0.z) + u * (v2.z - v0.z);
        return z;
    }

    /**
     * Computes the interpolated Z-value for a point p lying on the segment p0-p1
     * 
     * @param p
     * @param p0
     * @param p1
     * @return the interpolated Z value
     */
    public static double interpolateZ(Coordinate p, Coordinate p0, Coordinate p1) {
        double segLen = p0.distance(p1);
        double ptLen = p.distance(p0);
        double dz = p1.z - p0.z;
        double pz = p0.z + dz * (ptLen / segLen);
        return pz;
    }







}
