

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
package com.vividsolutions.jts.index.chain;

import java.util.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geomgraph.Quadrant;

/**
 * Constructs {@link MonotoneChain}s
 * for sequences of {@link Coordinate}s.
 *
 * @version 1.7
 */
public class MonotoneChainBuilder {

  public static int[] toIntArray(List list)
  {
    int[] array = new int[list.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = ((Integer) list.get(i)).intValue();
    }
    return array;
  }

  public static List getChains(Coordinate[] pts)
  {
    return getChains(pts, null);
  }

  /**
   * Return a list of the {@link MonotoneChain}s
   * for the given list of coordinates.
   */
  public static List getChains(Coordinate[] pts, Object context)
  {
    List mcList = new ArrayList();
    int[] startIndex = getChainStartIndices(pts);
    for (int i = 0; i < startIndex.length - 1; i++) {
      MonotoneChain mc = new MonotoneChain(pts, startIndex[i], startIndex[i + 1], context);
      mcList.add(mc);
    }
    return mcList;
  }

  /**
   * Return an array containing lists of start/end indexes of the monotone chains
   * for the given list of coordinates.
   * The last entry in the array points to the end point of the point array,
   * for use as a sentinel.
   */
  public static int[] getChainStartIndices(Coordinate[] pts)
  {
    // find the startpoint (and endpoints) of all monotone chains in this edge
    int start = 0;
    List startIndexList = new ArrayList();
    startIndexList.add(new Integer(start));
    do {
      int last = findChainEnd(pts, start);
      startIndexList.add(new Integer(last));
      start = last;
    } while (start < pts.length - 1);
    // copy list to an array of ints, for efficiency
    int[] startIndex = toIntArray(startIndexList);
    return startIndex;
  }

  /**
   * Finds the index of the last point in a monotone chain
   * starting at a given point.
   * Any repeated points (0-length segments) will be included
   * in the monotone chain returned.
   * 
   * @return the index of the last point in the monotone chain 
   * starting at <code>start</code>.
   */
  private static int findChainEnd(Coordinate[] pts, int start)
  {
  	int safeStart = start;
  	// skip any zero-length segments at the start of the sequence
  	// (since they cannot be used to establish a quadrant)
  	while (safeStart < pts.length - 1 && pts[safeStart].equals2D(pts[safeStart + 1])) {
  		safeStart++;
  	}
  	// check if there are NO non-zero-length segments
  	if (safeStart >= pts.length - 1) {
  		return pts.length - 1;
  	}
    // determine overall quadrant for chain (which is the starting quadrant)
    int chainQuad = Quadrant.quadrant(pts[safeStart], pts[safeStart + 1]);
    int last = start + 1;
    while (last < pts.length) {
    	// skip zero-length segments, but include them in the chain
    	if (! pts[last - 1].equals2D(pts[last])) {
        // compute quadrant for next possible segment in chain
    		int quad = Quadrant.quadrant(pts[last - 1], pts[last]);
      	if (quad != chainQuad) break;
    	}
      last++;
    }
    return last - 1;
  }


  public MonotoneChainBuilder() {
  }
}
