


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

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geomgraph.*;

/**
 * Finds all intersections in one or two sets of edges,
 * using the straightforward method of
 * comparing all segments.
 * This algorithm is too slow for production use, but is useful for testing purposes.
 * @version 1.7
 */
public class SimpleEdgeSetIntersector
  extends EdgeSetIntersector
{
  // statistics information
  int nOverlaps;

  public SimpleEdgeSetIntersector() {
  }

  public void computeIntersections(List edges, SegmentIntersector si, boolean testAllSegments)
  {
    nOverlaps = 0;

    for (Iterator i0 = edges.iterator(); i0.hasNext(); ) {
      Edge edge0 = (Edge) i0.next();
      for (Iterator i1 = edges.iterator(); i1.hasNext(); ) {
        Edge edge1 = (Edge) i1.next();
        if (testAllSegments || edge0 != edge1)
          computeIntersects(edge0, edge1, si);
      }
    }
  }


  public void computeIntersections(List edges0, List edges1, SegmentIntersector si)
  {
    nOverlaps = 0;

    for (Iterator i0 = edges0.iterator(); i0.hasNext(); ) {
      Edge edge0 = (Edge) i0.next();
      for (Iterator i1 = edges1.iterator(); i1.hasNext(); ) {
        Edge edge1 = (Edge) i1.next();
        computeIntersects(edge0, edge1, si);
      }
    }
  }

  /**
   * Performs a brute-force comparison of every segment in each Edge.
   * This has n^2 performance, and is about 100 times slower than using
   * monotone chains.
   */
  private void computeIntersects(Edge e0, Edge e1, SegmentIntersector si)
  {
   Coordinate[] pts0 = e0.getCoordinates();
    Coordinate[] pts1 = e1.getCoordinates();
    for (int i0 = 0; i0 < pts0.length - 1; i0++) {
      for (int i1 = 0; i1 < pts1.length - 1; i1++) {
        si.addIntersections(e0, i0, e1, i1);
      }
    }
  }
}
