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

/**
 * Various utility functions for mathematical and numerical operations.
 * 
 * @author mbdavis
 *
 */
public class MathUtil 
{
  /**
   * Clamps a <tt>double</tt> value to a given range.
   * @param x the value to clamp
   * @param min the minimum value of the range
   * @param max the maximum value of the range
   * @return the clamped value
   */
  public static double clamp(double x, double min, double max)
  {
    if (x < min) return min;
    if (x > max) return max;
    return x;
  }
  
  /**
   * Clamps an <tt>int</tt> value to a given range.
   * @param x the value to clamp
   * @param min the minimum value of the range
   * @param max the maximum value of the range
   * @return the clamped value
   */
  public static int clamp(int x, int min, int max)
  {
    if (x < min) return min;
    if (x > max) return max;
    return x;
  }
  
  private static final double LOG_10 = Math.log(10);
  
  /**
   * Computes the base-10 logarithm of a <tt>double</tt> value.
   * <ul>
   * <li>If the argument is NaN or less than zero, then the result is NaN.
   * <li>If the argument is positive infinity, then the result is positive infinity.
   * <li>If the argument is positive zero or negative zero, then the result is negative infinity.
   * </ul>
   *   
   * @param x a positive number
   * @return the value log a, the base-10 logarithm of the input value
   */
  public static double log10(double x)
  {
    double ln = Math.log(x);
    if (Double.isInfinite(ln)) return ln;
    if (Double.isNaN(ln)) return ln;
    return ln / LOG_10;
  }
  
  /**
   * Computes an index which wraps around a given maximum value.
   * For values >= 0, this is equals to <tt>val % max</tt>.
   * For values < 0, this is equal to <tt>max - (-val) % max</tt> 
   * 
   * @param index the value to wrap
   * @param max the maximum value (or modulus)
   * @return the wrapped index
   */
  public static int wrap(int index, int max)
  {
    if (index < 0) {
      return max - ((-index) % max);
    }
    return index % max;
  }

  /**
   * Computes the average of two numbers.
   * 
   * @param x1 a number
   * @param x2 a number
   * @return the average of the inputs
   */
  public static double average(double x1, double x2)
  {
    return (x1 + x2) / 2.0;
  }
  
  public static double max(double v1, double v2, double v3)
  {
    double max = v1;
    if (v2 > max) max = v2;
    if (v3 > max) max = v3;
    return max;
  }
  
  public static double max(double v1, double v2, double v3, double v4)
  {
    double max = v1;
    if (v2 > max) max = v2;
    if (v3 > max) max = v3;
    if (v4 > max) max = v4;
    return max;
  }
  
  public static double min(double v1, double v2, double v3, double v4)
  {
    double min = v1;
    if (v2 < min) min = v2;
    if (v3 < min) min = v3;
    if (v4 < min) min = v4;
    return min;
  }
}
