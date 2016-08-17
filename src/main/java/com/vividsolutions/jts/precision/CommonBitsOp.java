
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

/**
 * Provides versions of Geometry spatial functions which use
 * common bit removal to reduce the likelihood of robustness problems.
 * <p>
 * In the current implementation no rounding is performed on the
 * reshifted result geometry, which means that it is possible
 * that the returned Geometry is invalid.
 * Client classes should check the validity of the returned result themselves.
 *
 * @version 1.7
 */
public class CommonBitsOp {

  private boolean returnToOriginalPrecision = true;
  private CommonBitsRemover cbr;

  /**
   * Creates a new instance of class, which reshifts result {@link Geometry}s.
   */
  public CommonBitsOp()
  {
    this(true);
  }

  /**
   * Creates a new instance of class, specifying whether
   * the result {@link Geometry}s should be reshifted.
   *
   * @param returnToOriginalPrecision
   */
  public CommonBitsOp(boolean returnToOriginalPrecision)
  {
    this.returnToOriginalPrecision = returnToOriginalPrecision;
  }

  /**
   * Computes the set-theoretic intersection of two {@link Geometry}s, using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry
   * @return the Geometry representing the set-theoretic intersection of the input Geometries.
   */
  public Geometry intersection(Geometry geom0, Geometry geom1)
  {
    Geometry[] geom = removeCommonBits(geom0, geom1);
    return computeResultPrecision(geom[0].intersection(geom[1]));
  }

  /**
   * Computes the set-theoretic union of two {@link Geometry}s, using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry
   * @return the Geometry representing the set-theoretic union of the input Geometries.
   */
  public Geometry union(Geometry geom0, Geometry geom1)
  {
    Geometry[] geom = removeCommonBits(geom0, geom1);
    return computeResultPrecision(geom[0].union(geom[1]));
  }

  /**
   * Computes the set-theoretic difference of two {@link Geometry}s, using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry, to be subtracted from the first
   * @return the Geometry representing the set-theoretic difference of the input Geometries.
   */
  public Geometry difference(Geometry geom0, Geometry geom1)
  {
    Geometry[] geom = removeCommonBits(geom0, geom1);
    return computeResultPrecision(geom[0].difference(geom[1]));
  }

  /**
   * Computes the set-theoretic symmetric difference of two geometries,
   * using enhanced precision.
   * @param geom0 the first Geometry
   * @param geom1 the second Geometry
   * @return the Geometry representing the set-theoretic symmetric difference of the input Geometries.
   */
  public Geometry symDifference(Geometry geom0, Geometry geom1)
  {
    Geometry[] geom = removeCommonBits(geom0, geom1);
    return computeResultPrecision(geom[0].symDifference(geom[1]));
  }

  /**
   * Computes the buffer a geometry,
   * using enhanced precision.
   * @param geom0 the Geometry to buffer
   * @param distance the buffer distance
   * @return the Geometry representing the buffer of the input Geometry.
   */
  public Geometry buffer(Geometry geom0, double distance)
  {
    Geometry geom = removeCommonBits(geom0);
    return computeResultPrecision(geom.buffer(distance));
  }

  /**
   * If required, returning the result to the orginal precision if required.
   * <p>
   * In this current implementation, no rounding is performed on the
   * reshifted result geometry, which means that it is possible
   * that the returned Geometry is invalid.
   *
   * @param result the result Geometry to modify
   * @return the result Geometry with the required precision
   */
  private Geometry computeResultPrecision(Geometry result)
  {
    if (returnToOriginalPrecision)
      cbr.addCommonBits(result);
    return result;
  }

  /**
   * Computes a copy of the input {@link Geometry} with the calculated common bits
   * removed from each coordinate.
   * @param geom0 the Geometry to remove common bits from
   * @return a copy of the input Geometry with common bits removed
   */
  private Geometry removeCommonBits(Geometry geom0)
  {
    cbr = new CommonBitsRemover();
    cbr.add(geom0);
    Geometry geom = cbr.removeCommonBits((Geometry) geom0.clone());
    return geom;
  }

  /**
   * Computes a copy of each input {@link Geometry}s with the calculated common bits
   * removed from each coordinate.
   * @param geom0 a Geometry to remove common bits from
   * @param geom1 a Geometry to remove common bits from
   * @return an array containing copies
   * of the input Geometry's with common bits removed
   */
  private Geometry[] removeCommonBits(Geometry geom0, Geometry geom1)
  {
    cbr = new CommonBitsRemover();
    cbr.add(geom0);
    cbr.add(geom1);
    Geometry geom[] = new Geometry[2];
    geom[0] = cbr.removeCommonBits((Geometry) geom0.clone());
    geom[1] = cbr.removeCommonBits((Geometry) geom1.clone());
    return geom;
  }
}
