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

package com.vividsolutions.jts.noding.snapround;

import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.noding.*;
import com.vividsolutions.jts.util.*;

/**
 * Implements a "hot pixel" as used in the Snap Rounding algorithm.
 * A hot pixel contains the interior of the tolerance square and
 * the boundary
 * <b>minus</b> the top and right segments.
 * <p>
 * The hot pixel operations are all computed in the integer domain
 * to avoid rounding problems.
 *
 * @version 1.7
 */
public class HotPixel
{
  // testing only
//  public static int nTests = 0;

  private LineIntersector li;

  private Coordinate pt;
  private Coordinate originalPt;
  private Coordinate ptScaled;

  private Coordinate p0Scaled;
  private Coordinate p1Scaled;

  private double scaleFactor;

  private double minx;
  private double maxx;
  private double miny;
  private double maxy;
  
  /**
   * The corners of the hot pixel, in the order:
   *  10
   *  23
   */
  private Coordinate[] corner = new Coordinate[4];

  private Envelope safeEnv = null;

  /**
   * Creates a new hot pixel, using a given scale factor.
   * The scale factor must be strictly positive (non-zero).
   * 
   * @param pt the coordinate at the centre of the pixel
   * @param scaleFactor the scaleFactor determining the pixel size.  Must be > 0
   * @param li the intersector to use for testing intersection with line segments
   * 
   */
  public HotPixel(Coordinate pt, double scaleFactor, LineIntersector li) {
    originalPt = pt;
    this.pt = pt;
    this.scaleFactor = scaleFactor;
    this.li = li;
    //tolerance = 0.5;
    if (scaleFactor <= 0) 
      throw new IllegalArgumentException("Scale factor must be non-zero");
    if (scaleFactor != 1.0) {
      this.pt = new Coordinate(scale(pt.x), scale(pt.y));
      p0Scaled = new Coordinate();
      p1Scaled = new Coordinate();
    }
    initCorners(this.pt);
  }

  /**
   * Gets the coordinate this hot pixel is based at.
   * 
   * @return the coordinate of the pixel
   */
  public Coordinate getCoordinate() { return originalPt; }

  private static final double SAFE_ENV_EXPANSION_FACTOR = 0.75;
  
  /**
   * Returns a "safe" envelope that is guaranteed to contain the hot pixel.
   * The envelope returned will be larger than the exact envelope of the 
   * pixel.
   * 
   * @return an envelope which contains the hot pixel
   */
  public Envelope getSafeEnvelope()
  {
    if (safeEnv == null) {
      double safeTolerance = SAFE_ENV_EXPANSION_FACTOR / scaleFactor;
      safeEnv = new Envelope(originalPt.x - safeTolerance,
                             originalPt.x + safeTolerance,
                             originalPt.y - safeTolerance,
                             originalPt.y + safeTolerance
                             );
    }
    return safeEnv;
  }

  private void initCorners(Coordinate pt)
  {
    double tolerance = 0.5;
    minx = pt.x - tolerance;
    maxx = pt.x + tolerance;
    miny = pt.y - tolerance;
    maxy = pt.y + tolerance;

    corner[0] = new Coordinate(maxx, maxy);
    corner[1] = new Coordinate(minx, maxy);
    corner[2] = new Coordinate(minx, miny);
    corner[3] = new Coordinate(maxx, miny);
  }

  private double scale(double val)
  {
    return (double) Math.round(val * scaleFactor);
  }

  /**
   * Tests whether the line segment (p0-p1) 
   * intersects this hot pixel.
   * 
   * @param p0 the first coordinate of the line segment to test
   * @param p1 the second coordinate of the line segment to test
   * @return true if the line segment intersects this hot pixel
   */
  public boolean intersects(Coordinate p0, Coordinate p1)
  {
    if (scaleFactor == 1.0)
      return intersectsScaled(p0, p1);

    copyScaled(p0, p0Scaled);
    copyScaled(p1, p1Scaled);
    return intersectsScaled(p0Scaled, p1Scaled);
  }

  private void copyScaled(Coordinate p, Coordinate pScaled)
  {
    pScaled.x = scale(p.x);
    pScaled.y = scale(p.y);
  }

  private boolean intersectsScaled(Coordinate p0, Coordinate p1)
  {
    double segMinx = Math.min(p0.x, p1.x);
    double segMaxx = Math.max(p0.x, p1.x);
    double segMiny = Math.min(p0.y, p1.y);
    double segMaxy = Math.max(p0.y, p1.y);

    boolean isOutsidePixelEnv =  maxx < segMinx
                         || minx > segMaxx
                         || maxy < segMiny
                         || miny > segMaxy;
    if (isOutsidePixelEnv)
      return false;
    boolean intersects = intersectsToleranceSquare(p0, p1);
//    boolean intersectsPixelClosure = intersectsPixelClosure(p0, p1);

//    if (intersectsPixel != intersects) {
//      Debug.println("Found hot pixel intersection mismatch at " + pt);
//      Debug.println("Test segment: " + p0 + " " + p1);
//    }

/*
    if (scaleFactor != 1.0) {
      boolean intersectsScaled = intersectsScaledTest(p0, p1);
      if (intersectsScaled != intersects) {
        intersectsScaledTest(p0, p1);
//        Debug.println("Found hot pixel scaled intersection mismatch at " + pt);
//        Debug.println("Test segment: " + p0 + " " + p1);
      }
      return intersectsScaled;
    }
*/

    Assert.isTrue(! (isOutsidePixelEnv && intersects), "Found bad envelope test");
//    if (isOutsideEnv && intersects) {
//      Debug.println("Found bad envelope test");
//    }

    return intersects;
    //return intersectsPixelClosure;
  }

  /**
   * Tests whether the segment p0-p1 intersects the hot pixel tolerance square.
   * Because the tolerance square point set is partially open (along the
   * top and right) the test needs to be more sophisticated than
   * simply checking for any intersection.  
   * However, it can take advantage of the fact that the hot pixel edges
   * do not lie on the coordinate grid.  
   * It is sufficient to check if any of the following occur:
   * <ul>
   * <li>a proper intersection between the segment and any hot pixel edge
   * <li>an intersection between the segment and <b>both</b> the left and bottom hot pixel edges
   * (which detects the case where the segment intersects the bottom left hot pixel corner)
   * <li>an intersection between a segment endpoint and the hot pixel coordinate
   * </ul>
   *
   * @param p0
   * @param p1
   * @return
   */
  private boolean intersectsToleranceSquare(Coordinate p0, Coordinate p1)
  {
    boolean intersectsLeft = false;
    boolean intersectsBottom = false;
    //System.out.println("Hot Pixel: " + WKTWriter.toLineString(corner));
    //System.out.println("Line: " + WKTWriter.toLineString(p0, p1));
    
    li.computeIntersection(p0, p1, corner[0], corner[1]);
    if (li.isProper()) return true;

    li.computeIntersection(p0, p1, corner[1], corner[2]);
    if (li.isProper()) return true;
    if (li.hasIntersection()) intersectsLeft = true;

    li.computeIntersection(p0, p1, corner[2], corner[3]);
    if (li.isProper()) return true;
    if (li.hasIntersection()) intersectsBottom = true;

    li.computeIntersection(p0, p1, corner[3], corner[0]);
    if (li.isProper()) return true;

    if (intersectsLeft && intersectsBottom) return true;

    if (p0.equals(pt)) return true;
    if (p1.equals(pt)) return true;

    return false;
  }
  /**
   * Test whether the given segment intersects
   * the closure of this hot pixel.
   * This is NOT the test used in the standard snap-rounding
   * algorithm, which uses the partially closed tolerance square
   * instead.
   * This routine is provided for testing purposes only.
   *
   * @param p0 the start point of a line segment
   * @param p1 the end point of a line segment
   * @return <code>true</code> if the segment intersects the closure of the pixel's tolerance square
   */
  private boolean intersectsPixelClosure(Coordinate p0, Coordinate p1)
  {
    li.computeIntersection(p0, p1, corner[0], corner[1]);
    if (li.hasIntersection()) return true;
    li.computeIntersection(p0, p1, corner[1], corner[2]);
    if (li.hasIntersection()) return true;
    li.computeIntersection(p0, p1, corner[2], corner[3]);
    if (li.hasIntersection()) return true;
    li.computeIntersection(p0, p1, corner[3], corner[0]);
    if (li.hasIntersection()) return true;

    return false;
  }
  
  /**
   * Adds a new node (equal to the snap pt) to the specified segment
   * if the segment passes through the hot pixel
   *
   * @param segStr
   * @param segIndex
   * @return true if a node was added to the segment
   */
  public boolean addSnappedNode(
      NodedSegmentString segStr,
      int segIndex
      )
  {
    Coordinate p0 = segStr.getCoordinate(segIndex);
    Coordinate p1 = segStr.getCoordinate(segIndex + 1);

    if (intersects(p0, p1)) {
      //System.out.println("snapped: " + snapPt);
      //System.out.println("POINT (" + snapPt.x + " " + snapPt.y + ")");
      segStr.addIntersection(getCoordinate(), segIndex);

      return true;
    }
    return false;
  }

}
