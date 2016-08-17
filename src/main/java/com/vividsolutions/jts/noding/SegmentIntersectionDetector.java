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
package com.vividsolutions.jts.noding;

import com.vividsolutions.jts.algorithm.LineIntersector;
import com.vividsolutions.jts.algorithm.RobustLineIntersector;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Detects and records an intersection between two {@link SegmentString}s,
 * if one exists.  Only a single intersection is recorded.
 * This strategy can be configured to search for <b>proper intersections>/b>.
 * In this case, the presence of <i>any</i> kind of intersection will still be recorded,
 * but searching will continue until either a proper intersection has been found
 * or no intersections are detected.
 *
 * @version 1.7
 */
public class SegmentIntersectionDetector
    implements SegmentIntersector
{
  private LineIntersector li;
  private boolean findProper = false;
  private boolean findAllTypes = false;
  
  private boolean hasIntersection = false;
  private boolean hasProperIntersection = false;
  private boolean hasNonProperIntersection = false;
  
  private Coordinate intPt = null;
  private Coordinate[] intSegments = null;

  /**
   * Creates an intersection finder using a {@link RobustLineIntersector}.
   */
  public SegmentIntersectionDetector()
  {
    this(new RobustLineIntersector());
  }

  /**
   * Creates an intersection finder using a given LineIntersector.
   *
   * @param li the LineIntersector to use
   */
  public SegmentIntersectionDetector(LineIntersector li)
  {
    this.li = li;
  }

  /**
   * Sets whether processing must continue until a proper intersection is found.
   * 
   * @param findProper true if processing should continue until a proper intersection is found
   */
  public void setFindProper(boolean findProper)
  {
    this.findProper = findProper;
  }
  
  /**
   * Sets whether processing can terminate once any intersection is found.
   * 
   * @param findAllTypes true if processing can terminate once any intersection is found.
   */
  public void setFindAllIntersectionTypes(boolean findAllTypes)
  {
    this.findAllTypes = findAllTypes;
  }
  
  /**
   * Tests whether an intersection was found.
   * 
   * @return true if an intersection was found
   */
  public boolean hasIntersection() 
  { 
  	return hasIntersection; 
  }
  
  /**
   * Tests whether a proper intersection was found.
   * 
   * @return true if a proper intersection was found
   */
  public boolean hasProperIntersection() 
  { 
    return hasProperIntersection; 
  }
  
  /**
   * Tests whether a non-proper intersection was found.
   * 
   * @return true if a non-proper intersection was found
   */
  public boolean hasNonProperIntersection() 
  { 
    return hasNonProperIntersection; 
  }
  
  /**
   * Gets the computed location of the intersection.
   * Due to round-off, the location may not be exact.
   * 
   * @return the coordinate for the intersection location
   */
  public Coordinate getIntersection()  
  {    
  	return intPt;  
  }


  /**
   * Gets the endpoints of the intersecting segments.
   * 
   * @return an array of the segment endpoints (p00, p01, p10, p11)
   */
  public Coordinate[] getIntersectionSegments()
  {
  	return intSegments;
  }
  
  /**
   * This method is called by clients
   * of the {@link SegmentIntersector} class to process
   * intersections for two segments of the {@link SegmentString}s being intersected.
   * Note that some clients (such as <code>MonotoneChain</code>s) may optimize away
   * this call for segment pairs which they have determined do not intersect
   * (e.g. by an disjoint envelope test).
   */
  public void processIntersections(
      SegmentString e0,  int segIndex0,
      SegmentString e1,  int segIndex1
      )
  {  	
    // don't bother intersecting a segment with itself
    if (e0 == e1 && segIndex0 == segIndex1) return;
    
    Coordinate p00 = e0.getCoordinates()[segIndex0];
    Coordinate p01 = e0.getCoordinates()[segIndex0 + 1];
    Coordinate p10 = e1.getCoordinates()[segIndex1];
    Coordinate p11 = e1.getCoordinates()[segIndex1 + 1];
    
    li.computeIntersection(p00, p01, p10, p11);
//  if (li.hasIntersection() && li.isProper()) Debug.println(li);

    if (li.hasIntersection()) {
			// System.out.println(li);
    	
    	// record intersection info
			hasIntersection = true;
			
			boolean isProper = li.isProper();
			if (isProper)
				hasProperIntersection = true;
      if (! isProper)
        hasNonProperIntersection = true;
			
			/**
			 * If this is the kind of intersection we are searching for
			 * OR no location has yet been recorded
			 * save the location data
			 */
			boolean saveLocation = true;
			if (findProper && ! isProper) saveLocation = false;
			
			if (intPt == null || saveLocation) {

				// record intersection location (approximate)
				intPt = li.getIntersection(0);

				// record intersecting segments
				intSegments = new Coordinate[4];
				intSegments[0] = p00;
				intSegments[1] = p01;
				intSegments[2] = p10;
				intSegments[3] = p11;
			}
		}
  }
  
  /**
   * Tests whether processing can terminate,
   * because all required information has been obtained
   * (e.g. an intersection of the desired type has been detected).
   * 
   * @return true if processing can terminate
   */
  public boolean isDone()
  { 
    /**
     * If finding all types, we can stop
     * when both possible types have been found.
     */
    if (findAllTypes) {
      return hasProperIntersection && hasNonProperIntersection;
    }
    
  	/**
  	 * If searching for a proper intersection, only stop if one is found
  	 */
  	if (findProper) {
  		return hasProperIntersection;
  	}
  	return hasIntersection;
  }
}
