
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
package com.vividsolutions.jts.precision;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.overlay.OverlayOp;

/**
  * Provides versions of Geometry spatial functions which use
  * enhanced precision techniques to reduce the likelihood of robustness problems.
 *
 * @version 1.7
 */
public class EnhancedPrecisionOp
{
  /**
   * Computes the set-theoretic intersection of two {@link Geometry}s, using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry
   * @return the Geometry representing the set-theoretic intersection of the input Geometries.
   */
  public static Geometry intersection(Geometry geom0, Geometry geom1)
  {
    RuntimeException originalEx;
    try {
      Geometry result = geom0.intersection(geom1);
      return result;
    }
    catch (RuntimeException ex)
    {
      originalEx = ex;
    }
    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem).  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
      CommonBitsOp cbo = new CommonBitsOp(true);
      Geometry resultEP = cbo.intersection(geom0, geom1);
      // check that result is a valid geometry after the reshift to orginal precision
      if (! resultEP.isValid())
        throw originalEx;
      return resultEP;
    }
    catch (RuntimeException ex2)
    {
      throw originalEx;
    }
  }
  /**
   * Computes the set-theoretic union of two {@link Geometry}s, using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry
   * @return the Geometry representing the set-theoretic union of the input Geometries.
   */
  public static Geometry union(Geometry geom0, Geometry geom1)
  {
    RuntimeException originalEx;
    try {
      Geometry result = geom0.union(geom1);
      return result;
    }
    catch (RuntimeException ex)
    {
      originalEx = ex;
    }
    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem).  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
      CommonBitsOp cbo = new CommonBitsOp(true);
      Geometry resultEP = cbo.union(geom0, geom1);
      // check that result is a valid geometry after the reshift to orginal precision
      if (! resultEP.isValid())
        throw originalEx;
      return resultEP;
    }
    catch (RuntimeException ex2)
    {
      throw originalEx;
    }
  }
  /**
   * Computes the set-theoretic difference of two {@link Geometry}s, using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry
   * @return the Geometry representing the set-theoretic difference of the input Geometries.
   */
  public static Geometry difference(Geometry geom0, Geometry geom1)
  {
    RuntimeException originalEx;
    try {
      Geometry result = geom0.difference(geom1);
      return result;
    }
    catch (RuntimeException ex)
    {
      originalEx = ex;
    }
    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem).  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
      CommonBitsOp cbo = new CommonBitsOp(true);
      Geometry resultEP = cbo.difference(geom0, geom1);
      // check that result is a valid geometry after the reshift to orginal precision
      if (! resultEP.isValid())
        throw originalEx;
      return resultEP;
    }
    catch (RuntimeException ex2)
    {
      throw originalEx;
    }
  }
  /**
   * Computes the set-theoretic symmetric difference of two {@link Geometry}s, using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry
   * @return the Geometry representing the set-theoretic symmetric difference of the input Geometries.
   */
  public static Geometry symDifference(Geometry geom0, Geometry geom1)
  {
    RuntimeException originalEx;
    try {
      Geometry result = geom0.symDifference(geom1);
      return result;
    }
    catch (RuntimeException ex)
    {
      originalEx = ex;
    }
    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem).  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
      CommonBitsOp cbo = new CommonBitsOp(true);
      Geometry resultEP = cbo.symDifference(geom0, geom1);
      // check that result is a valid geometry after the reshift to orginal precision
      if (! resultEP.isValid())
        throw originalEx;
      return resultEP;
    }
    catch (RuntimeException ex2)
    {
      throw originalEx;
    }
  }
  /**
   * Computes the buffer of a {@link Geometry}, using enhanced precision.
   * This method should no longer be necessary, since the buffer algorithm
   * now is highly robust.
   *
   * @param geom the first Geometry
   * @param distance the buffer distance
   * @return the Geometry representing the buffer of the input Geometry.
   */
  public static Geometry buffer(Geometry geom, double distance)
  {
    RuntimeException originalEx;
    try {
      Geometry result = geom.buffer(distance);
      return result;
    }
    catch (RuntimeException ex)
    {
      originalEx = ex;
    }
    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem).  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
      CommonBitsOp cbo = new CommonBitsOp(true);
      Geometry resultEP = cbo.buffer(geom, distance);
      // check that result is a valid geometry after the reshift to orginal precision
      if (! resultEP.isValid())
        throw originalEx;
      return resultEP;
    }
    catch (RuntimeException ex2)
    {
      throw originalEx;
    }
  }

}
