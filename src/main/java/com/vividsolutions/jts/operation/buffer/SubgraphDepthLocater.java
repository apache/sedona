
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

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.algorithm.*;

/**
 * Locates a subgraph inside a set of subgraphs,
 * in order to determine the outside depth of the subgraph.
 * The input subgraphs are assumed to have had depths
 * already calculated for their edges.
 *
 * @version 1.7
 */
class SubgraphDepthLocater
{
  private Collection subgraphs;
  private LineSegment seg = new LineSegment();
  private CGAlgorithms cga = new CGAlgorithms();

  public SubgraphDepthLocater(List subgraphs)
  {
    this.subgraphs = subgraphs;
  }

  public int getDepth(Coordinate p)
  {
    List stabbedSegments = findStabbedSegments(p);
    // if no segments on stabbing line subgraph must be outside all others.
    if (stabbedSegments.size() == 0)
      return 0;
    DepthSegment ds = (DepthSegment) Collections.min(stabbedSegments);
    return ds.leftDepth;
  }

  /**
   * Finds all non-horizontal segments intersecting the stabbing line.
   * The stabbing line is the ray to the right of stabbingRayLeftPt.
   *
   * @param stabbingRayLeftPt the left-hand origin of the stabbing line
   * @return a List of {@link DepthSegments} intersecting the stabbing line
   */
  private List findStabbedSegments(Coordinate stabbingRayLeftPt)
  {
    List stabbedSegments = new ArrayList();
    for (Iterator i = subgraphs.iterator(); i.hasNext(); ) {
      BufferSubgraph bsg = (BufferSubgraph) i.next();

      // optimization - don't bother checking subgraphs which the ray does not intersect
      Envelope env = bsg.getEnvelope();
      if (stabbingRayLeftPt.y < env.getMinY()
          || stabbingRayLeftPt.y > env.getMaxY())
        continue;

      findStabbedSegments(stabbingRayLeftPt, bsg.getDirectedEdges(), stabbedSegments);
    }
    return stabbedSegments;
  }

  /**
   * Finds all non-horizontal segments intersecting the stabbing line
   * in the list of dirEdges.
   * The stabbing line is the ray to the right of stabbingRayLeftPt.
   *
   * @param stabbingRayLeftPt the left-hand origin of the stabbing line
   * @param stabbedSegments the current list of {@link DepthSegments} intersecting the stabbing line
   */
  private void findStabbedSegments(Coordinate stabbingRayLeftPt,
                                   List dirEdges,
                                   List stabbedSegments)
  {
    /**
     * Check all forward DirectedEdges only.  This is still general,
     * because each Edge has a forward DirectedEdge.
     */
    for (Iterator i = dirEdges.iterator(); i.hasNext();) {
      DirectedEdge de = (DirectedEdge) i.next();
      if (! de.isForward())
        continue;
      findStabbedSegments(stabbingRayLeftPt, de, stabbedSegments);
    }
  }

  /**
   * Finds all non-horizontal segments intersecting the stabbing line
   * in the input dirEdge.
   * The stabbing line is the ray to the right of stabbingRayLeftPt.
   *
   * @param stabbingRayLeftPt the left-hand origin of the stabbing line
   * @param stabbedSegments the current list of {@link DepthSegments} intersecting the stabbing line
   */
  private void findStabbedSegments(Coordinate stabbingRayLeftPt,
                                   DirectedEdge dirEdge,
                                   List stabbedSegments)
  {
    Coordinate[] pts = dirEdge.getEdge().getCoordinates();
    for (int i = 0; i < pts.length - 1; i++) {
      seg.p0 = pts[i];
      seg.p1 = pts[i + 1];
      // ensure segment always points upwards
      if (seg.p0.y > seg.p1.y)
        seg.reverse();

      // skip segment if it is left of the stabbing line
      double maxx = Math.max(seg.p0.x, seg.p1.x);
      if (maxx < stabbingRayLeftPt.x)
        continue;

      // skip horizontal segments (there will be a non-horizontal one carrying the same depth info
      if (seg.isHorizontal())
        continue;

      // skip if segment is above or below stabbing line
      if (stabbingRayLeftPt.y < seg.p0.y || stabbingRayLeftPt.y > seg.p1.y)
        continue;

      // skip if stabbing ray is right of the segment
      if (CGAlgorithms.computeOrientation(seg.p0, seg.p1, stabbingRayLeftPt)
          == CGAlgorithms.RIGHT)
        continue;

      // stabbing line cuts this segment, so record it
      int depth = dirEdge.getDepth(Position.LEFT);
      // if segment direction was flipped, use RHS depth instead
      if (! seg.p0.equals(pts[i]))
        depth = dirEdge.getDepth(Position.RIGHT);
      DepthSegment ds = new DepthSegment(seg, depth);
      stabbedSegments.add(ds);
    }
  }


  /**
   * A segment from a directed edge which has been assigned a depth value
   * for its sides.
   */
  static class DepthSegment
      implements Comparable
  {
    private LineSegment upwardSeg;
    private int leftDepth;

    public DepthSegment(LineSegment seg, int depth)
    {
      // input seg is assumed to be normalized
      upwardSeg = new LineSegment(seg);
      //upwardSeg.normalize();
      this.leftDepth = depth;
    }
    /**
     * Defines a comparison operation on DepthSegments
     * which orders them left to right.
     * Assumes the segments are normalized.
     * <p>
     * The definition of the ordering is:
     * <ul>
     * <li>-1 : if DS1.seg is left of or below DS2.seg (DS1 < DS2)
     * <li>1 : if   DS1.seg is right of or above DS2.seg (DS1 > DS2) 
     * <li>0 : if the segments are identical 
     * </ul>
     *
     * KNOWN BUGS:
     * <ul>
     * <li>The logic does not obey the {@link Comparator.compareTo} contract. 
     * This is acceptable for the intended usage, but may cause problems if used with some
     * utilities in the Java standard library (e.g. {@link Collections.sort()}.
     * </ul>
     * 
     * @param obj a DepthSegment
     * @return the comparison value
     */
    public int compareTo(Object obj)
    {
      DepthSegment other = (DepthSegment) obj;
      
      // fast check if segments are trivially ordered along X
      if (upwardSeg.minX() >= other.upwardSeg.maxX()) return 1;
      if (upwardSeg.maxX() <= other.upwardSeg.minX()) return -1;
      
      /**
       * try and compute a determinate orientation for the segments.
       * Test returns 1 if other is left of this (i.e. this > other)
       */
      int orientIndex = upwardSeg.orientationIndex(other.upwardSeg);
      if (orientIndex != 0) return orientIndex;

      /**
       * If comparison between this and other is indeterminate,
       * try the opposite call order.
       * The sign of the result needs to be flipped.
       */
      orientIndex = -1 * other.upwardSeg.orientationIndex(upwardSeg);
      if (orientIndex != 0) return orientIndex;

      // otherwise, use standard lexocographic segment ordering
      return upwardSeg.compareTo(other.upwardSeg);
    }

    /**
     * Compare two collinear segments for left-most ordering.
     * If segs are vertical, use vertical ordering for comparison.
     * If segs are equal, return 0.
     * Segments are assumed to be directed so that the second coordinate is >= to the first
     * (e.g. up and to the right).
     *
     * @param seg0 a segment to compare
     * @param seg1 a segment to compare
     * @return
     */
    private int compareX(LineSegment seg0, LineSegment seg1)
    {
      int compare0 = seg0.p0.compareTo(seg1.p0);
      if (compare0 != 0)
        return compare0;
      return seg0.p1.compareTo(seg1.p1);

    }

    public String toString()
    {
      return upwardSeg.toString();
    }
  }
}
