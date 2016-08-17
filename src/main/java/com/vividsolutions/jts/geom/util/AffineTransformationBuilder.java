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

package com.vividsolutions.jts.geom.util;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.math.Matrix;
import com.vividsolutions.jts.algorithm.*;

/**
 * Builds an {@link AffineTransformation} defined by a set of control vectors. 
 * A control vector consists of a source point and a destination point, 
 * which is the image of the source point under the desired transformation.
 * <p>
 * A transformation is well-defined 
 * by a set of three control vectors 
 * if and only if the source points are not collinear. 
 * (In particular, the degenerate situation
 * where two or more source points are identical will not produce a well-defined transformation).
 * A well-defined transformation exists and is unique.
 * If the control vectors are not well-defined, the system of equations
 * defining the transformation matrix entries is not solvable,
 * and no transformation can be determined.
 * <p>
 * No such restriction applies to the destination points.
 * However, if the destination points are collinear or non-unique,
 * a non-invertible transformations will be generated.
 * <p>
 * This technique of recovering a transformation
 * from its effect on known points is used in the Bilinear Interpolated Triangulation
 * algorithm for warping planar surfaces.
 *
 * @author Martin Davis
 */
public class AffineTransformationBuilder
{
  private Coordinate src0;
  private Coordinate src1;
  private Coordinate src2;
  private Coordinate dest0;
  private Coordinate dest1;
  private Coordinate dest2;
  
  // the matrix entries for the transformation
  private double m00, m01, m02, m10, m11, m12;
  
 
  /**
   * Constructs a new builder for
   * the transformation defined by the given 
   * set of control point mappings.
   * 
   * @param src0 a control point
   * @param src1 a control point
   * @param src2 a control point
   * @param dest0 the image of control point 0 under the required transformation
   * @param dest1 the image of control point 1 under the required transformation
   * @param dest2 the image of control point 2 under the required transformation
   */
  public AffineTransformationBuilder(Coordinate src0,
      Coordinate src1,
      Coordinate src2,
      Coordinate dest0,
      Coordinate dest1,
      Coordinate dest2)
  {
    this.src0 = src0;
    this.src1 = src1;
    this.src2 = src2;
    this.dest0 = dest0;
    this.dest1 = dest1;
    this.dest2 = dest2;
  }
    
  /**
   * Computes the {@link AffineTransformation}
   * determined by the control point mappings,
   * or <code>null</code> if the control vectors do not determine a well-defined transformation.
   * 
   * @return an affine transformation
   * @return null if the control vectors do not determine a well-defined transformation
   */
  public AffineTransformation getTransformation()
  {
  	// compute full 3-point transformation
    boolean isSolvable = compute();
    if (isSolvable)
      return new AffineTransformation(m00, m01, m02, m10, m11, m12);
    return null;
  }
    
  /**
   * Computes the transformation matrix by 
   * solving the two systems of linear equations
   * defined by the control point mappings,
   * if this is possible.
   * 
   * @return true if the transformation matrix is solvable
   */
  private boolean compute()
  {
    double[] bx = new double[] { dest0.x, dest1.x, dest2.x };
    double[] row0 = solve(bx);
    if (row0 == null) return false;
    m00 = row0[0];
    m01 = row0[1];
    m02 = row0[2];
    
    double[] by = new double[] { dest0.y, dest1.y, dest2.y };
    double[] row1 = solve(by);
    if (row1 == null) return false;
    m10 = row1[0];
    m11 = row1[1];
    m12 = row1[2];
    return true;
  }

  /**
   * Solves the transformation matrix system of linear equations
   * for the given right-hand side vector.
   * 
   * @param b the vector for the right-hand side of the system
   * @return the solution vector
   * @return null if no solution could be determined
   */
  private double[] solve(double[] b)
  {
    double[][] a = new double[][] {
        { src0.x, src0.y, 1 },
        { src1.x, src1.y, 1},
        { src2.x, src2.y, 1}
    };
    return Matrix.solve(a, b);
  }
}
