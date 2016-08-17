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

import java.util.*;

/**
 * Finds if two sets of {@link SegmentString}s intersect.
 * Uses indexing for fast performance and to optimize repeated tests
 * against a target set of lines.
 * Short-circuited to return as soon an intersection is found.
 *
 * Immutable and thread-safe.
 *
 * @version 1.7
 */
public class FastSegmentSetIntersectionFinder 
{
	private final SegmentSetMutualIntersector segSetMutInt; 
	// for testing purposes
	// private SimpleSegmentSetMutualIntersector mci;  

	/**
	 * Creates an intersection finder against a given set of segment strings.
	 * 
	 * @param baseSegStrings the segment strings to search for intersections
	 */
	public FastSegmentSetIntersectionFinder(Collection baseSegStrings)
	{
	    segSetMutInt = new MCIndexSegmentSetMutualIntersector(baseSegStrings);
	}
		
	/**
	 * Gets the segment set intersector used by this class.
	 * This allows other uses of the same underlying indexed structure.
	 * 
	 * @return the segment set intersector used
	 */
	public SegmentSetMutualIntersector getSegmentSetIntersector()
	{
		return segSetMutInt;
	}
	
	/**
	 * Tests for intersections with a given set of target {@link SegmentString}s.
	 * 
	 * @param segStrings the SegmentStrings to test
	 * @return true if an intersection is found
	 */
	public boolean intersects(Collection segStrings)
	{
		SegmentIntersectionDetector intFinder = new SegmentIntersectionDetector();
		return intersects(segStrings, intFinder);
	}
	
	/**
	 * Tests for intersections with a given set of target {@link SegmentString}s.
	 * using a given SegmentIntersectionDetector.
	 * 
	 * @param segStrings the SegmentStrings to test
	 * @param intDetector the intersection detector to use
	 * @return true if the detector reports intersections
	 */
	public boolean intersects(Collection segStrings, SegmentIntersectionDetector intDetector)
	{
		segSetMutInt.process(segStrings, intDetector);
 		return intDetector.hasIntersection();
	}
}
