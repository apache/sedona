


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
package com.vividsolutions.jts.operation.relate;

/**
 * @version 1.7
 */

import com.vividsolutions.jts.algorithm.BoundaryNodeRule;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.GeometryGraphOperation;
import java.util.*;

/**
 * Implements the SFS <tt>relate()</tt> generalized spatial predicate on two {@link Geometry}s.
 * <b>
 * The class supports specifying a custom {@link BoundaryNodeRule}
 * to be used during the relate computation.
 * <p>
 * If named spatial predicates are used on the result {@link IntersectionMatrix}
 * of the RelateOp, the result may or not be affected by the 
 * choice of <tt>BoundaryNodeRule</tt>, depending on the exact nature of the pattern.
 * For instance, {@link IntersectionMatrix#isIntersects()} is insensitive 
 * to the choice of <tt>BoundaryNodeRule</tt>, 
 * whereas {@link IntersectionMatrix#isTouches(int, int)} is affected by the rule chosen.
 * <p>
 * <b>Note:</b> custom Boundary Node Rules do not (currently)
 * affect the results of other {@link Geometry} methods (such
 * as {@link Geometry#getBoundary}.  The results of
 * these methods may not be consistent with the relationship computed by
 * a custom Boundary Node Rule.
 *
 * @version 1.7
 */
public class RelateOp
  extends GeometryGraphOperation
{
  /**
   * Computes the {@link IntersectionMatrix} for the spatial relationship
   * between two {@link Geometry}s, using the default (OGC SFS) Boundary Node Rule
   *
   * @param a a Geometry to test
   * @param b a Geometry to test
   * @return the IntersectonMatrix for the spatial relationship between the geometries
   */
  public static IntersectionMatrix relate(Geometry a, Geometry b)
  {
    RelateOp relOp = new RelateOp(a, b);
    IntersectionMatrix im = relOp.getIntersectionMatrix();
    return im;
  }

  /**
   * Computes the {@link IntersectionMatrix} for the spatial relationship
   * between two {@link Geometry}s using a specified Boundary Node Rule.
   *
   * @param a a Geometry to test
   * @param b a Geometry to test
   * @param boundaryNodeRule the Boundary Node Rule to use
   * @return the IntersectonMatrix for the spatial relationship between the input geometries
   */
  public static IntersectionMatrix relate(Geometry a, Geometry b, BoundaryNodeRule boundaryNodeRule)
  {
    RelateOp relOp = new RelateOp(a, b, boundaryNodeRule);
    IntersectionMatrix im = relOp.getIntersectionMatrix();
    return im;
  }

  private RelateComputer relate;

  /**
   * Creates a new Relate operation, using the default (OGC SFS) Boundary Node Rule.
   *
   * @param g0 a Geometry to relate
   * @param g1 another Geometry to relate
   */
  public RelateOp(Geometry g0, Geometry g1)
  {
    super(g0, g1);
    relate = new RelateComputer(arg);
  }

  /**
   * Creates a new Relate operation with a specified Boundary Node Rule.
   *
   * @param g0 a Geometry to relate
   * @param g1 another Geometry to relate
   * @param boundaryNodeRule the Boundary Node Rule to use
   */
  public RelateOp(Geometry g0, Geometry g1, BoundaryNodeRule boundaryNodeRule)
  {
    super(g0, g1, boundaryNodeRule);
    relate = new RelateComputer(arg);
  }

  /**
   * Gets the IntersectionMatrix for the spatial relationship
   * between the input geometries.
   *
   * @return the IntersectonMatrix for the spatial relationship between the input geometries
   */
  public IntersectionMatrix getIntersectionMatrix()
  {
    return relate.computeIM();
  }

}
