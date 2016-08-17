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

import com.vividsolutions.jts.geom.Coordinate;


/**
 * Intersects two sets of {@link SegmentString}s using 
 * brute-force comparison.
 *
 * @version 1.7
 */
public class SimpleSegmentSetMutualIntersector implements SegmentSetMutualIntersector
{
  private final Collection baseSegStrings;

  /**
   * Constructs a new intersector for a given set of {@link SegmentStrings}.
   * 
   * @param baseSegStrings the base segment strings to intersect
   */
  public SimpleSegmentSetMutualIntersector(Collection segStrings)
  {
	  this.baseSegStrings = segStrings;
  }

  /**
   * Calls {@link SegmentIntersector#processIntersections(SegmentString, int, SegmentString, int)} 
   * for all <i>candidate</i> intersections between
   * the given collection of SegmentStrings and the set of base segments. 
   * 
   * @param a set of segments to intersect
   * @param the segment intersector to use
   */
  public void process(Collection segStrings, SegmentIntersector segInt) {
    for (Iterator i = baseSegStrings.iterator(); i.hasNext(); ) {
    	SegmentString baseSS = (SegmentString) i.next();
    	for (Iterator j = segStrings.iterator(); j.hasNext(); ) {
	      	SegmentString ss = (SegmentString) j.next();
	      	intersect(baseSS, ss, segInt);
	        if (segInt.isDone()) 
	        	return;
    	}
    }
  }

  /**
   * Processes all of the segment pairs in the given segment strings
   * using the given SegmentIntersector.
   * 
   * @param ss0 a Segment string
   * @param ss1 a segment string
   * @param segInt the segment intersector to use
   */
  private void intersect(SegmentString ss0, SegmentString ss1, SegmentIntersector segInt)
  {
    Coordinate[] pts0 = ss0.getCoordinates();
    Coordinate[] pts1 = ss1.getCoordinates();
    for (int i0 = 0; i0 < pts0.length - 1; i0++) {
      for (int i1 = 0; i1 < pts1.length - 1; i1++) {
        segInt.processIntersections(ss0, i0, ss1, i1);
        if (segInt.isDone()) 
        	return;
      }
    }

  }

}
