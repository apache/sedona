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
package com.vividsolutions.jts.operation.buffer;

/**
 * A value class containing the parameters which 
 * specify how a buffer should be constructed.
 * <p>
 * The parameters allow control over:
 * <ul>
 * <li>Quadrant segments (accuracy of approximation for circular arcs)
 * <li>End Cap style
 * <li>Join style
 * <li>Mitre limit
 * <li>whether the buffer is single-sided
 * </ul>
 * 
 * @author Martin Davis
 *
 */
public class BufferParameters 
{
  /**
   * Specifies a round line buffer end cap style.
   */
  public static final int CAP_ROUND = 1;
  /**
   * Specifies a flat line buffer end cap style.
   */
  public static final int CAP_FLAT = 2;
  /**
   * Specifies a square line buffer end cap style.
   */
  public static final int CAP_SQUARE = 3;
  
  /**
   * Specifies a round join style.
   */
  public static final int JOIN_ROUND = 1;
  /**
   * Specifies a mitre join style.
   */
  public static final int JOIN_MITRE = 2;
  /**
   * Specifies a bevel join style.
   */
  public static final int JOIN_BEVEL = 3;

  /**
   * The default number of facets into which to divide a fillet of 90 degrees.
   * A value of 8 gives less than 2% max error in the buffer distance.
   * For a max error of < 1%, use QS = 12.
   * For a max error of < 0.1%, use QS = 18.
   */
  public static final int DEFAULT_QUADRANT_SEGMENTS = 8;

  /**
   * The default mitre limit
   * Allows fairly pointy mitres.
   */
  public static final double DEFAULT_MITRE_LIMIT = 5.0;
  
  /**
   * The default simplify factor
   * Provides an accuracy of about 1%, which matches the accuracy of the default Quadrant Segments parameter.
   */
  public static final double DEFAULT_SIMPLIFY_FACTOR = 0.01;
  

  private int quadrantSegments = DEFAULT_QUADRANT_SEGMENTS;
  private int endCapStyle = CAP_ROUND;
  private int joinStyle = JOIN_ROUND;
  private double mitreLimit = DEFAULT_MITRE_LIMIT;
  private boolean isSingleSided = false;
  private double simplifyFactor = DEFAULT_SIMPLIFY_FACTOR;
  
  /**
   * Creates a default set of parameters
   *
   */
  public BufferParameters() {
  }

  /**
   * Creates a set of parameters with the
   * given quadrantSegments value.
   * 
   * @param quadrantSegments the number of quadrant segments to use
   */
  public BufferParameters(int quadrantSegments) 
  {
    setQuadrantSegments(quadrantSegments);
  }

  /**
   * Creates a set of parameters with the
   * given quadrantSegments and endCapStyle values.
   * 
   * @param quadrantSegments the number of quadrant segments to use
   * @param endCapStyle the end cap style to use
   */
  public BufferParameters(int quadrantSegments,
      int endCapStyle) 
  {
    setQuadrantSegments(quadrantSegments);
    setEndCapStyle(endCapStyle);
  }

  /**
   * Creates a set of parameters with the
   * given parameter values.
   * 
   * @param quadrantSegments the number of quadrant segments to use
   * @param endCapStyle the end cap style to use
   * @param joinStyle the join style to use
   * @param mitreLimit the mitre limit to use
   */
  public BufferParameters(int quadrantSegments,
      int endCapStyle,
      int joinStyle,
      double mitreLimit) 
  {
    setQuadrantSegments(quadrantSegments);
    setEndCapStyle(endCapStyle);
    setJoinStyle(joinStyle);
    setMitreLimit(mitreLimit);
  }

  /**
   * Gets the number of quadrant segments which will be used
   * 
   * @return the number of quadrant segments
   */
  public int getQuadrantSegments()
  {
    return quadrantSegments;
  }
  
  /**
   * Sets the number of line segments used to approximate an angle fillet.
   * <ul>
   * <li>If <tt>quadSegs</tt> >= 1, joins are round, and <tt>quadSegs</tt> indicates the number of 
   * segments to use to approximate a quarter-circle.
   * <li>If <tt>quadSegs</tt> = 0, joins are bevelled (flat)
   * <li>If <tt>quadSegs</tt> < 0, joins are mitred, and the value of qs
   * indicates the mitre ration limit as
   * <pre>
   * mitreLimit = |<tt>quadSegs</tt>|
   * </pre>
   * </ul>
   * For round joins, <tt>quadSegs</tt> determines the maximum
   * error in the approximation to the true buffer curve.
   * The default value of 8 gives less than 2% max error in the buffer distance.
   * For a max error of < 1%, use QS = 12.
   * For a max error of < 0.1%, use QS = 18.
   * The error is always less than the buffer distance 
   * (in other words, the computed buffer curve is always inside the true
   * curve).
   * 
   * @param quadSegs the number of segments in a fillet for a quadrant
   */
  public void setQuadrantSegments(int quadSegs)
  {
    quadrantSegments = quadSegs;
    
    /** 
     * Indicates how to construct fillets.
     * If qs >= 1, fillet is round, and qs indicates number of 
     * segments to use to approximate a quarter-circle.
     * If qs = 0, fillet is bevelled flat (i.e. no filleting is performed)
     * If qs < 0, fillet is mitred, and absolute value of qs
     * indicates maximum length of mitre according to
     * 
     * mitreLimit = |qs|
     */
    if (quadrantSegments == 0)
      joinStyle = JOIN_BEVEL;
    if (quadrantSegments < 0) {
      joinStyle = JOIN_MITRE;
      mitreLimit = Math.abs(quadrantSegments);
    }
    
    if (quadSegs <= 0) {
      quadrantSegments = 1;
    }
    
    /**
     * If join style was set by the quadSegs value,
     * use the default for the actual quadrantSegments value.
     */
    if (joinStyle != JOIN_ROUND) {
      quadrantSegments = DEFAULT_QUADRANT_SEGMENTS;
    }
  }

  /**
   * Computes the maximum distance error due to a given level
   * of approximation to a true arc.
   * 
   * @param quadSegs the number of segments used to approximate a quarter-circle
   * @return the error of approximation
   */
  public static double bufferDistanceError(int quadSegs)
  {
    double alpha = Math.PI / 2.0 / quadSegs;
    return 1 - Math.cos(alpha / 2.0);
  }
  
  /**
   * Gets the end cap style.
   * 
   * @return the end cap style
   */
  public int getEndCapStyle()
  {
    return endCapStyle;
  }
  
  /**
   * Specifies the end cap style of the generated buffer.
   * The styles supported are {@link #CAP_ROUND}, {@link #CAP_FLAT}, and {@link #CAP_SQUARE}.
   * The default is CAP_ROUND.
   *
   * @param endCapStyle the end cap style to specify
   */
  public void setEndCapStyle(int endCapStyle)
  {
    this.endCapStyle = endCapStyle;
  }
  
  /**
   * Gets the join style
   * 
   * @return the join style code
   */
  public int getJoinStyle()
  {
    return joinStyle;
  }
  
  /**
   * Sets the join style for outside (reflex) corners between line segments.
   * Allowable values are {@link #JOIN_ROUND} (which is the default),
   * {@link #JOIN_MITRE} and {link JOIN_BEVEL}.
   * 
   * @param joinStyle the code for the join style
   */
  public void setJoinStyle(int joinStyle)
  {
    this.joinStyle = joinStyle;
  }
  
  /**
   * Gets the mitre ratio limit.
   * 
   * @return the limit value
   */
  public double getMitreLimit()
  {
    return mitreLimit;
  }
  
  /**
   * Sets the limit on the mitre ratio used for very sharp corners.
   * The mitre ratio is the ratio of the distance from the corner
   * to the end of the mitred offset corner.
   * When two line segments meet at a sharp angle, 
   * a miter join will extend far beyond the original geometry.
   * (and in the extreme case will be infinitely far.)
   * To prevent unreasonable geometry, the mitre limit 
   * allows controlling the maximum length of the join corner.
   * Corners with a ratio which exceed the limit will be beveled.
   *
   * @param mitreLimit the mitre ratio limit
   */
  public void setMitreLimit(double mitreLimit)
  {
    this.mitreLimit = mitreLimit;
  }

  /**
   * Sets whether the computed buffer should be single-sided.
   * A single-sided buffer is constructed on only one side of each input line.
   * <p>
   * The side used is determined by the sign of the buffer distance:
   * <ul>
   * <li>a positive distance indicates the left-hand side
   * <li>a negative distance indicates the right-hand side
   * </ul>
   * The single-sided buffer of point geometries is 
   * the same as the regular buffer.
   * <p>
   * The End Cap Style for single-sided buffers is 
   * always ignored, 
   * and forced to the equivalent of <tt>CAP_FLAT</tt>. 
   * 
   * @param isSingleSided true if a single-sided buffer should be constructed
   */
  public void setSingleSided(boolean isSingleSided)
  {
    this.isSingleSided = isSingleSided;
  }

  /**
   * Tests whether the buffer is to be generated on a single side only.
   * 
   * @return true if the generated buffer is to be single-sided
   */
  public boolean isSingleSided() {
    return isSingleSided;
  }

  /**
   * Gets the simplify factor.
   * 
   * @return the simplify factor
   */
  public double getSimplifyFactor() {
    return simplifyFactor;
  }
  
  /**
   * Sets the factor used to determine the simplify distance tolerance
   * for input simplification.
   * Simplifying can increase the performance of computing buffers.
   * Generally the simplify factor should be greater than 0.
   * Values between 0.01 and .1 produce relatively good accuracy for the generate buffer.
   * Larger values sacrifice accuracy in return for performance.
   * 
   * @param simplifyFactor a value greater than or equal to zero.
   */
  public void setSimplifyFactor(double simplifyFactor)
  {
    this.simplifyFactor = simplifyFactor < 0 ? 0 : simplifyFactor;
  }
}
