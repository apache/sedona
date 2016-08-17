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

import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.chain.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.noding.SegmentIntersector;
import com.vividsolutions.jts.noding.SegmentString;

/**
 * Intersects two sets of {@link SegmentString}s using a index based
 * on {@link MonotoneChain}s and a {@link SpatialIndex}.
 *
 * Thread-safe and immutable.
 * 
 * @version 1.7
 */
public class MCIndexSegmentSetMutualIntersector implements SegmentSetMutualIntersector
{
  /**
  * The {@link SpatialIndex} used should be something that supports
  * envelope (range) queries efficiently (such as a 
  * {@link com.vividsolutions.jts.index.quadtree.Quadtree}
  * or {@link STRtree}.
  */
  private STRtree index = new STRtree();

  /**
   * Constructs a new intersector for a given set of {@link SegmentStrings}.
   * 
   * @param baseSegStrings the base segment strings to intersect
   */
  public MCIndexSegmentSetMutualIntersector(Collection baseSegStrings)
  {
	  initBaseSegments(baseSegStrings);
  }

  /** 
   * Gets the index constructed over the base segment strings.
   * 
   * NOTE: To retain thread-safety, treat returned value as immutable!
   * 
   * @return the constructed index
   */
  public SpatialIndex getIndex() { return index; }

  private void initBaseSegments(Collection segStrings)
  {
    for (Iterator i = segStrings.iterator(); i.hasNext(); ) {
      addToIndex((SegmentString) i.next());
    }
    // build index to ensure thread-safety
    index.build();
  }
  
  private void addToIndex(SegmentString segStr)
  {
    List segChains = MonotoneChainBuilder.getChains(segStr.getCoordinates(), segStr);
    for (Iterator i = segChains.iterator(); i.hasNext(); ) {
      MonotoneChain mc = (MonotoneChain) i.next();
      index.insert(mc.getEnvelope(), mc);
    }
  }

  /**
   * Calls {@link SegmentIntersector#processIntersections(SegmentString, int, SegmentString, int)} 
   * for all <i>candidate</i> intersections between
   * the given collection of SegmentStrings and the set of indexed segments. 
   * 
   * @param a set of segments to intersect
   * @param the segment intersector to use
   */
  public void process(Collection segStrings, SegmentIntersector segInt)
  {
  	List monoChains = new ArrayList();
    for (Iterator i = segStrings.iterator(); i.hasNext(); ) {
      addToMonoChains((SegmentString) i.next(), monoChains);
    }
    intersectChains(monoChains, segInt);
//    System.out.println("MCIndexBichromaticIntersector: # chain overlaps = " + nOverlaps);
//    System.out.println("MCIndexBichromaticIntersector: # oct chain overlaps = " + nOctOverlaps);
  }

  private void addToMonoChains(SegmentString segStr, List monoChains)
  {
    List segChains = MonotoneChainBuilder.getChains(segStr.getCoordinates(), segStr);
    for (Iterator i = segChains.iterator(); i.hasNext(); ) {
      MonotoneChain mc = (MonotoneChain) i.next();
      monoChains.add(mc);
    }
  }

  private void intersectChains(List monoChains, SegmentIntersector segInt)
  {
    MonotoneChainOverlapAction overlapAction = new SegmentOverlapAction(segInt);

    for (Iterator i = monoChains.iterator(); i.hasNext(); ) {
      MonotoneChain queryChain = (MonotoneChain) i.next();
      List overlapChains = index.query(queryChain.getEnvelope());
      for (Iterator j = overlapChains.iterator(); j.hasNext(); ) {
        MonotoneChain testChain = (MonotoneChain) j.next();
        queryChain.computeOverlaps(testChain, overlapAction);
        if (segInt.isDone()) return;
      }
    }
  }

  public class SegmentOverlapAction
      extends MonotoneChainOverlapAction
  {
    private SegmentIntersector si = null;

    public SegmentOverlapAction(SegmentIntersector si)
    {
      this.si = si;
    }

    public void overlap(MonotoneChain mc1, int start1, MonotoneChain mc2, int start2)
    {
      SegmentString ss1 = (SegmentString) mc1.getContext();
      SegmentString ss2 = (SegmentString) mc2.getContext();
      si.processIntersections(ss1, start1, ss2, start2);
    }

  }
}
