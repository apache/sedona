

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

import com.vividsolutions.jts.geom.*;


/**
 * Monotone Chains are a way of partitioning the segments of a linestring to
 * allow for fast searching of intersections.
 * They have the following properties:
 * <ol>
 * <li>the segments within a monotone chain never intersect each other
 * <li>the envelope of any contiguous subset of the segments in a monotone chain
 * is equal to the envelope of the endpoints of the subset.
 * </ol>
 * Property 1 means that there is no need to test pairs of segments from within
 * the same monotone chain for intersection.
 * <p>
 * Property 2 allows
 * an efficient binary search to be used to find the intersection points of two monotone chains.
 * For many types of real-world data, these properties eliminate a large number of
 * segment comparisons, producing substantial speed gains.
 * <p>
 * One of the goals of this implementation of MonotoneChains is to be
 * as space and time efficient as possible. One design choice that aids this
 * is that a MonotoneChain is based on a subarray of a list of points.
 * This means that new arrays of points (potentially very large) do not
 * have to be allocated.
 * <p>
 *
 * MonotoneChains support the following kinds of queries:
 * <ul>
 * <li>Envelope select: determine all the segments in the chain which
 * intersect a given envelope
 * <li>Overlap: determine all the pairs of segments in two chains whose
 * envelopes overlap
 * </ul>
 *
 * This implementation of MonotoneChains uses the concept of internal iterators
 * ({@link MonotoneChainSelectAction} and {@link MonotoneChainOverlapAction})
 * to return the results for queries.
 * This has time and space advantages, since it
 * is not necessary to build lists of instantiated objects to represent the segments
 * returned by the query.
 * Queries made in this manner are thread-safe.
 *
 * @version 1.7
 */
public class MonotoneChain {

  private Coordinate[] pts;
  private int start, end;
  private Envelope env = null;
  private Object context = null;// user-defined information
  private int id;// useful for optimizing chain comparisons

  public MonotoneChain(Coordinate[] pts, int start, int end, Object context)
  {
    this.pts    = pts;
    this.start  = start;
    this.end    = end;
    this.context = context;
  }

  public void setId(int id) { this.id = id; }
  public int getId() { return id; }

  public Object getContext() { return context; }

  public Envelope getEnvelope()
  {
    if (env == null) {
      Coordinate p0 = pts[start];
      Coordinate p1 = pts[end];
      env = new Envelope(p0, p1);
    }
    return env;
  }

  public int getStartIndex()  { return start; }
  public int getEndIndex()    { return end; }

  /**
   * Gets the line segment starting at <code>index</code>
   * 
   * @param index index of segment
   * @param ls line segment to extract into
   */
  public void getLineSegment(int index, LineSegment ls)
  {
    ls.p0 = pts[index];
    ls.p1 = pts[index + 1];
  }
  /**
   * Return the subsequence of coordinates forming this chain.
   * Allocates a new array to hold the Coordinates
   */
  public Coordinate[] getCoordinates()
  {
    Coordinate coord[] = new Coordinate[end - start + 1];
    int index = 0;
    for (int i = start; i <= end; i++) {
      coord[index++] = pts[i];
    }
    return coord;
  }

  /**
   * Determine all the line segments in the chain whose envelopes overlap
   * the searchEnvelope, and process them.
   * <p>
   * The monotone chain search algorithm attempts to optimize 
   * performance by not calling the select action on chain segments
   * which it can determine are not in the search envelope.
   * However, it *may* call the select action on segments
   * which do not intersect the search envelope.
   * This saves on the overhead of checking envelope intersection
   * each time, since clients may be able to do this more efficiently.
   * 
   * @param searchEnv the search envelope
   * @param mcs the select action to execute on selected segments
   */
  public void select(Envelope searchEnv, MonotoneChainSelectAction mcs)
  {
    computeSelect(searchEnv, start, end, mcs);
  }

  private void computeSelect(
    Envelope searchEnv,
    int start0, int end0,
    MonotoneChainSelectAction mcs )
  {
    Coordinate p0 = pts[start0];
    Coordinate p1 = pts[end0];
    mcs.tempEnv1.init(p0, p1);

//Debug.println("trying:" + p0 + p1 + " [ " + start0 + ", " + end0 + " ]");
    // terminating condition for the recursion
    if (end0 - start0 == 1) {
      //Debug.println("computeSelect:" + p0 + p1);
      mcs.select(this, start0);
      return;
    }
    // nothing to do if the envelopes don't overlap
    if (! searchEnv.intersects(mcs.tempEnv1))
      return;

    // the chains overlap, so split each in half and iterate  (binary search)
    int mid = (start0 + end0) / 2;

    // Assert: mid != start or end (since we checked above for end - start <= 1)
    // check terminating conditions before recursing
    if (start0 < mid) {
      computeSelect(searchEnv, start0, mid, mcs);
    }
    if (mid < end0) {
      computeSelect(searchEnv, mid, end0, mcs);
    }
  }

  /**
   * Determine all the line segments in two chains which may overlap, and process them.
   * <p>
   * The monotone chain search algorithm attempts to optimize 
   * performance by not calling the overlap action on chain segments
   * which it can determine do not overlap.
   * However, it *may* call the overlap action on segments
   * which do not actually interact.
   * This saves on the overhead of checking intersection
   * each time, since clients may be able to do this more efficiently.
   * 
   * @param searchEnv the search envelope
   * @param mco the overlap action to execute on selected segments
   */
  public void computeOverlaps(MonotoneChain mc, MonotoneChainOverlapAction mco)
  {
    computeOverlaps(start, end, mc, mc.start, mc.end, mco);
  }

  private void computeOverlaps(
    int start0, int end0,
    MonotoneChain mc,
    int start1, int end1,
    MonotoneChainOverlapAction mco)
  {
    Coordinate p00 = pts[start0];
    Coordinate p01 = pts[end0];
    Coordinate p10 = mc.pts[start1];
    Coordinate p11 = mc.pts[end1];
//Debug.println("computeIntersectsForChain:" + p00 + p01 + p10 + p11);
    // terminating condition for the recursion
    if (end0 - start0 == 1 && end1 - start1 == 1) {
      mco.overlap(this, start0, mc, start1);
      return;
    }
    // nothing to do if the envelopes of these chains don't overlap
    mco.tempEnv1.init(p00, p01);
    mco.tempEnv2.init(p10, p11);
    if (! mco.tempEnv1.intersects(mco.tempEnv2)) return;

    // the chains overlap, so split each in half and iterate  (binary search)
    int mid0 = (start0 + end0) / 2;
    int mid1 = (start1 + end1) / 2;

    // Assert: mid != start or end (since we checked above for end - start <= 1)
    // check terminating conditions before recursing
    if (start0 < mid0) {
      if (start1 < mid1) computeOverlaps(start0, mid0, mc, start1,  mid1, mco);
      if (mid1 < end1)   computeOverlaps(start0, mid0, mc, mid1,    end1, mco);
    }
    if (mid0 < end0) {
      if (start1 < mid1) computeOverlaps(mid0,   end0, mc, start1,  mid1, mco);
      if (mid1 < end1)   computeOverlaps(mid0,   end0, mc, mid1,    end1, mco);
    }
  }
}
