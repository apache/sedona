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
package com.vividsolutions.jts.operation.overlay.validate;

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.overlay.OverlayOp;
import com.vividsolutions.jts.operation.overlay.snap.GeometrySnapper;

/**
 * Validates that the result of an overlay operation is
 * geometrically correct, within a determined tolerance.
 * Uses fuzzy point location to find points which are 
 * definitely in either the interior or exterior of the result
 * geometry, and compares these results with the expected ones.
 * <p>
 * This algorithm is only useful where the inputs are polygonal.
 * This is a heuristic test, and may return false positive results
 * (I.e. it may fail to detect an invalid result.)
 * It should never return a false negative result, however
 * (I.e. it should never report a valid result as invalid.)
 *
 * @author Martin Davis
 * @version 1.7
 * @see OverlayOp
 */
public class OverlayResultValidator
{
  public static boolean isValid(Geometry a, Geometry b, int overlayOp, Geometry result)
  {
    OverlayResultValidator validator = new OverlayResultValidator(a, b, result);
    return validator.isValid(overlayOp);
  }

  private static double computeBoundaryDistanceTolerance(Geometry g0, Geometry g1)
  {
  	return Math.min(GeometrySnapper.computeSizeBasedSnapTolerance(g0),
  			GeometrySnapper.computeSizeBasedSnapTolerance(g1));
  }
  
  private static final double TOLERANCE = 0.000001;

  private Geometry[] geom;
  private FuzzyPointLocator[] locFinder;
  private int[] location = new int[3] ;
  private Coordinate invalidLocation = null;
  private double boundaryDistanceTolerance = TOLERANCE;

  private List testCoords = new ArrayList();

  public OverlayResultValidator(Geometry a, Geometry b, Geometry result) 
  {
  	/**
  	 * The tolerance to use needs to depend on the size of the geometries.
  	 * It should not be more precise than double-precision can support. 
  	 */
    boundaryDistanceTolerance = computeBoundaryDistanceTolerance(a, b);
    geom = new Geometry[] { a, b, result };
    locFinder = new FuzzyPointLocator[] {
      new FuzzyPointLocator(geom[0], boundaryDistanceTolerance),
      new FuzzyPointLocator(geom[1], boundaryDistanceTolerance),
      new FuzzyPointLocator(geom[2], boundaryDistanceTolerance)
      };
  }

  public boolean isValid(int overlayOp)
  {
    addTestPts(geom[0]);
    addTestPts(geom[1]);
    boolean isValid = checkValid(overlayOp);

    /*
    System.out.println("OverlayResultValidator: " + isValid);
    System.out.println("G0");
    System.out.println(geom[0]);
    System.out.println("G1");
    System.out.println(geom[1]);
    System.out.println("Result");
    System.out.println(geom[2]);
    */
    
    return isValid;
  }

  public Coordinate getInvalidLocation() { return invalidLocation; }

  private void addTestPts(Geometry g)
  {
    OffsetPointGenerator ptGen = new OffsetPointGenerator(g);
    testCoords.addAll(ptGen.getPoints(5 * boundaryDistanceTolerance));
  }

  private boolean checkValid(int overlayOp)
  {
    for (int i = 0; i < testCoords.size(); i++) {
      Coordinate pt = (Coordinate) testCoords.get(i);
      if (! checkValid(overlayOp, pt)) {
        invalidLocation = pt;
        return false;
      }
    }
    return true;
  }

  private boolean checkValid(int overlayOp, Coordinate pt)
  {
    location[0] = locFinder[0].getLocation(pt);
    location[1] = locFinder[1].getLocation(pt);
    location[2] = locFinder[2].getLocation(pt);

    /**
     * If any location is on the Boundary, can't deduce anything, so just return true
     */
    if (hasLocation(location, Location.BOUNDARY))
      return true;

    return isValidResult(overlayOp, location);
  }

  private static boolean hasLocation(int[] location, int loc)
  {
    for (int i = 0; i < 3; i ++) {
      if (location[i] == loc)
        return true;
    }
    return false;
  }

  private boolean isValidResult(int overlayOp, int[] location)
  {
    boolean expectedInterior = OverlayOp.isResultOfOp(location[0], location[1], overlayOp);

    boolean resultInInterior = (location[2] == Location.INTERIOR);
    // MD use simpler: boolean isValid = (expectedInterior == resultInInterior);
    boolean isValid = ! (expectedInterior ^ resultInInterior);
    
    if (! isValid) reportResult(overlayOp, location, expectedInterior);
    
    return isValid;
 }

  private void reportResult(int overlayOp, int[] location, boolean expectedInterior)
  {
  	System.out.println(
  			"Overlay result invalid - A:" + Location.toLocationSymbol(location[0])
  			+ " B:" + Location.toLocationSymbol(location[1])
  			+ " expected:" + (expectedInterior ? 'i' : 'e')
  			+ " actual:" + Location.toLocationSymbol(location[2])
  			);
  }
}

