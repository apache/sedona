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
import com.vividsolutions.jts.geom.*;

/**
 * Dissolves a noded collection of {@link SegmentString}s to produce
 * a set of merged linework with unique segments.
 * A custom {@link SegmentStringMerger} merging strategy
 * can be supplied.  
 * This strategy will be called when two identical (up to orientation)
 * strings are dissolved together.
 * The default merging strategy is simply to discard one of the merged strings.
 * <p>
 * A common use for this class is to merge noded edges
 * while preserving topological labelling.
 * This requires a custom merging strategy to be supplied 
 * to merge the topology labels appropriately.
 *
 * @version 1.7
 * @see SegmentStringMerger
 */
public class SegmentStringDissolver
{
	/**
	 * A merging strategy which can be used to update the context data of {@link SegmentString}s 
	 * which are merged during the dissolve process.
	 * 
	 * @author mbdavis
	 *
	 */
  public interface SegmentStringMerger 
  {
    /**
     * Updates the context data of a SegmentString
     * when an identical (up to orientation) one is found during dissolving.
     *
     * @param mergeTarget the segment string to update
     * @param ssToMerge the segment string being dissolved
     * @param isSameOrientation <code>true</code> if the strings are in the same direction,
     * <code>false</code> if they are opposite
     */
    void merge(SegmentString mergeTarget, SegmentString ssToMerge, boolean isSameOrientation);
  }

  private SegmentStringMerger merger;
  private Map ocaMap = new TreeMap();

  // testing only
  //private List testAddedSS = new ArrayList();

  /**
   * Creates a dissolver with a user-defined merge strategy.
   *
   * @param merger the merging strategy to use
   */
  public SegmentStringDissolver(SegmentStringMerger merger) {
    this.merger = merger;
  }

  /**
   * Creates a dissolver with the default merging strategy.
   */
  public SegmentStringDissolver() {
    this(null);
  }

  /**
   * Dissolve all {@link SegmentString}s in the input {@link Collection}
   * @param segStrings
   */
  public void dissolve(Collection segStrings)
  {
    for (Iterator i = segStrings.iterator(); i.hasNext(); ) {
      dissolve((SegmentString) i.next());
    }
  }

  private void add(OrientedCoordinateArray oca, SegmentString segString)
  {
    ocaMap.put(oca, segString);
    //testAddedSS.add(oca);
  }

  /**
   * Dissolve the given {@link SegmentString}.
   *
   * @param segString the string to dissolve
   */
  public void dissolve(SegmentString segString)
  {
    OrientedCoordinateArray oca = new OrientedCoordinateArray(segString.getCoordinates());
    SegmentString existing = findMatching(oca, segString);
    if (existing == null) {
      add(oca, segString);
    }
    else {
      if (merger != null) {
        boolean isSameOrientation
            = CoordinateArrays.equals(existing.getCoordinates(), segString.getCoordinates());
        merger.merge(existing, segString, isSameOrientation);
      }
    }
  }

  private SegmentString findMatching(OrientedCoordinateArray oca,
                                    SegmentString segString)
  {
    SegmentString matchSS = (SegmentString) ocaMap.get(oca);
    /*
    boolean hasBeenAdded = checkAdded(oca);
    if (matchSS == null && hasBeenAdded) {
      System.out.println("added!");
    }
    */
    return matchSS;
  }

/*

  private boolean checkAdded(OrientedCoordinateArray oca)
  {
    for (Iterator i = testAddedSS.iterator(); i.hasNext(); ) {
      OrientedCoordinateArray addedOCA = (OrientedCoordinateArray) i.next();
      if (oca.compareTo(addedOCA) == 0)
        return true;
    }
    return false;
  }
*/

  /**
   * Gets the collection of dissolved (i.e. unique) {@link SegmentString}s
   *
   * @return the unique {@link SegmentString}s
   */
  public Collection getDissolved() { return ocaMap.values(); }
}



