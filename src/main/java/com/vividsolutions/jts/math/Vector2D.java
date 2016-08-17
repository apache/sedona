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
package com.vividsolutions.jts.math;

import com.vividsolutions.jts.algorithm.Angle;
import com.vividsolutions.jts.algorithm.RobustDeterminant;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.util.Assert;

/**
 * A 2-dimensional mathematical vector represented by double-precision X and Y components.
 * 
 * @author mbdavis
 * 
 */
public class Vector2D {
  /**
   * Creates a new vector with given X and Y components.
   * 
   * @param x the x component
   * @param y the y component
   * @return a new vector
   */
	public static Vector2D create(double x, double y) {
		return new Vector2D(x, y);
	}

  /**
   * Creates a new vector from an existing one.
   * 
   * @param v the vector to copy
   * @return a new vector
   */
	public static Vector2D create(Vector2D v) {
		return new Vector2D(v);
	}

  /**
   * Creates a vector from a {@link Coordinate}. 
   * 
   * @param coord the Coordinate to copy
   * @return a new vector
   */
	public static Vector2D create(Coordinate coord) {
		return new Vector2D(coord);
	}

  /**
   * Creates a vector with the direction and magnitude
   * of the difference between the 
   * <tt>to</tt> and <tt>from</tt> {@link Coordinate}s.
   * 
   * @param from the origin Coordinate
   * @param to the destination Coordinate
   * @return a new vector
   */
	public static Vector2D create(Coordinate from, Coordinate to) {
		return new Vector2D(from, to);
	}

	/**
	 * The X component of this vector.
	 */
	private double x;

	/**
	 * The Y component of this vector.
	 */
	private double y;

	public Vector2D() {
		this(0.0, 0.0);
	}

	public Vector2D(double x, double y) {
		this.x = x;
		this.y = y;
	}

	public Vector2D(Vector2D v) {
		x = v.x;
		y = v.y;
	}

	public Vector2D(Coordinate from, Coordinate to) {
		x = to.x - from.x;
		y = to.y - from.y;
	}

	public Vector2D(Coordinate v) {
		x = v.x;
		y = v.y;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	public double getComponent(int index) {
		if (index == 0)
			return x;
		return y;
	}

	public Vector2D add(Vector2D v) {
		return create(x + v.x, y + v.y);
	}

	public Vector2D subtract(Vector2D v) {
		return create(x - v.x, y - v.y);
	}

  /**
   * Multiplies the vector by a scalar value.
   * 
   * @param d the value to multiply by
   * @return a new vector with the value v * d
   */
	public Vector2D multiply(double d) {
		return create(x * d, y * d);
	}

  /**
   * Divides the vector by a scalar value.
   * 
   * @param d the value to divide by
   * @return a new vector with the value v / d
   */
	public Vector2D divide(double d) {
		return create(x / d, y / d);
	}

	public Vector2D negate() {
		return create(-x , -y);
	}

	public double length() {
		return Math.sqrt(x * x + y * y);
	}

	public double lengthSquared() {
		return x * x + y * y;
	}

	public Vector2D normalize() {
		double length = length();
		if (length > 0.0)
			return divide(length);
		return create(0.0, 0.0);
	}

	public Vector2D average(Vector2D v) {
		return weightedSum(v, 0.5);
	}
  
	/**
	 * Computes the weighted sum of this vector
	 * with another vector,
	 * with this vector contributing a fraction
	 * of <tt>frac</tt> to the total.
	 * <p>
	 * In other words, 
	 * <pre>
	 * sum = frac * this + (1 - frac) * v
	 * </pre>
	 * 
	 * @param v the vector to sum
	 * @param frac the fraction of the total contributed by this vector
	 * @return the weighted sum of the two vectors
	 */
	public Vector2D weightedSum(Vector2D v, double frac) {
		return create(
				frac * x + (1.0 - frac) * v.x, 
				frac * y + (1.0 - frac) * v.y);
	}

  /**
   * Computes the distance between this vector and another one.
   * @param v a vector
   * @return the distance between the vectors
   */
  public double distance(Vector2D v)
  {
    double delx = v.x - x;
    double dely = v.y - y;
    return Math.sqrt(delx * delx + dely * dely);
  }
  
	/**
	 * Computes the dot-product of two vectors
	 * 
	 * @param v a vector
	 * @return the dot product of the vectors
	 */
	public double dot(Vector2D v) {
		return x * v.x + y * v.y;
	}

	public double angle()
	{
		return Math.atan2(y, x);
	}
	
  public double angle(Vector2D v)
  {
    return Angle.diff(v.angle(), angle());
  }
  
  public double angleTo(Vector2D v)
  {
    double a1 = angle();
    double a2 = v.angle();
    double angDel = a2 - a1;
    
    // normalize, maintaining orientation
    if (angDel <= -Math.PI)
      return angDel + Angle.PI_TIMES_2;
    if (angDel > Math.PI)
      return angDel - Angle.PI_TIMES_2;
    return angDel;
  }
  
	public Vector2D rotate(double angle)
	{
		double cos = Math.cos(angle);
		double sin = Math.sin(angle);
		return create(
				x * cos - y * sin,
				x * sin + y * cos
				);
	}
	
	/**
	 * Rotates a vector by a given number of quarter-circles (i.e. multiples of 90
	 * degrees or Pi/2 radians). A positive number rotates counter-clockwise, a
	 * negative number rotates clockwise. Under this operation the magnitude of
	 * the vector and the absolute values of the ordinates do not change, only
	 * their sign and ordinate index.
	 * 
	 * @param numQuarters
	 *          the number of quarter-circles to rotate by
	 * @return the rotated vector.
	 */
	public Vector2D rotateByQuarterCircle(int numQuarters) {
		int nQuad = numQuarters % 4;
		if (numQuarters < 0 && nQuad != 0) {
			nQuad = nQuad + 4;
		}
		switch (nQuad) {
		case 0:
			return create(x, y);
		case 1:
			return create(-y, x);
		case 2:
			return create(-x, -y);
		case 3:
			return create(y, -x);
		}
		Assert.shouldNeverReachHere();
		return null;
	}

  public boolean isParallel(Vector2D v)
  {
    return 0.0 == RobustDeterminant.signOfDet2x2(x, y, v.x, v.y);
  }
  
	public Coordinate translate(Coordinate coord) {
		return new Coordinate(x + coord.x, y + coord.y);
	}

	public Coordinate toCoordinate() {
		return new Coordinate(x, y);
	}

  /**
   * Creates a copy of this vector
   * 
   * @return a copy of this vector
   */
  public Object clone()
  {
    return new Vector2D(this);
  }
  
  /**
   * Gets a string representation of this vector
   * 
   * @return a string representing this vector
   */
	public String toString() {
		return "[" + x + ", " + y + "]";
	}
	
	/**
	 * Tests if a vector <tt>o</tt> has the same values for the x and y
	 * components.
	 * 
	 * @param o
	 *          a <tt>Vector2D</tt> with which to do the comparison.
	 * @return true if <tt>other</tt> is a <tt>Vector2D</tt> with the same
	 *         values for the x and y components.
	 */
	public boolean equals(Object o) {
		if (!(o instanceof Vector2D)) {
			return false;
		}
		Vector2D v = (Vector2D) o;
		return x == v.x && y == v.y;
	}

	/**
	 * Gets a hashcode for this vector.
	 * 
	 * @return a hashcode for this vector
	 */
	public int hashCode() {
		// Algorithm from Effective Java by Joshua Bloch
		int result = 17;
		result = 37 * result + Coordinate.hashCode(x);
		result = 37 * result + Coordinate.hashCode(y);
		return result;
	}


}
