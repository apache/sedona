


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
package com.vividsolutions.jts.geomgraph.index;

/**
 * @version 1.7
 */
import java.util.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geomgraph.*;

/**
 * Finds all intersections in one or two sets of edges,
 * using a simple x-axis sweepline algorithm.
 * While still O(n^2) in the worst case, this algorithm
 * drastically improves the average-case time.
 *
 * @version 1.7
 */
public class SimpleSweepLineIntersector
  extends EdgeSetIntersector
{

  List events = new ArrayList();
  // statistics information
  int nOverlaps;

  public SimpleSweepLineIntersector() {
  }

  public void computeIntersections(List edges, SegmentIntersector si, boolean testAllSegments)
  {
    if (testAllSegments)
      add(edges, null);
    else
      add(edges);
    computeIntersections(si);
  }

  public void computeIntersections(List edges0, List edges1, SegmentIntersector si)
  {
    add(edges0, edges0);
    add(edges1, edges1);
    computeIntersections(si);
  }

  private void add(List edges)
  {
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      Edge edge = (Edge) i.next();
      // edge is its own group
      add(edge, edge);
    }
  }
  private void add(List edges, Object edgeSet)
  {
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      Edge edge = (Edge) i.next();
      add(edge, edgeSet);
    }
  }


  private void add(Edge edge, Object edgeSet)
  {
    Coordinate[] pts = edge.getCoordinates();
    for (int i = 0; i < pts.length - 1; i++) {
      SweepLineSegment ss = new SweepLineSegment(edge, i);
      SweepLineEvent insertEvent = new SweepLineEvent(edgeSet, ss.getMinX(), null);
      events.add(insertEvent);
      events.add(new SweepLineEvent(ss.getMaxX(), insertEvent));
    }
  }

  /**
   * Because DELETE events have a link to their corresponding INSERT event,
   * it is possible to compute exactly the range of events which must be
   * compared to a given INSERT event object.
   */
  private void prepareEvents()
  {
    Collections.sort(events);
    // set DELETE event indexes
    for (int i = 0; i < events.size(); i++ )
    {
      SweepLineEvent ev = (SweepLineEvent) events.get(i);
      if (ev.isDelete()) {
        ev.getInsertEvent().setDeleteEventIndex(i);
      }
    }
  }

  private void computeIntersections(SegmentIntersector si)
  {
    nOverlaps = 0;
    prepareEvents();

    for (int i = 0; i < events.size(); i++ )
    {
      SweepLineEvent ev = (SweepLineEvent) events.get(i);
      if (ev.isInsert()) {
        processOverlaps(i, ev.getDeleteEventIndex(), ev, si);
      }
    }
  }

  private void processOverlaps(int start, int end, SweepLineEvent ev0, SegmentIntersector si)
  {
    SweepLineSegment ss0 = (SweepLineSegment) ev0.getObject();
    /**
     * Since we might need to test for self-intersections,
     * include current INSERT event object in list of event objects to test.
     * Last index can be skipped, because it must be a Delete event.
     */
    for (int i = start; i < end; i++ ) {
      SweepLineEvent ev1 = (SweepLineEvent) events.get(i);
      if (ev1.isInsert()) {
        SweepLineSegment ss1 = (SweepLineSegment) ev1.getObject();
        // don't compare edges in same group, if labels are present
        if (! ev0.isSameLabel(ev1)) {
          ss0.computeIntersections(ss1, si);
          nOverlaps++;
        }
      }
    }

  }
}
