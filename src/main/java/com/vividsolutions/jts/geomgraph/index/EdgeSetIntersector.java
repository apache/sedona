


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
import com.vividsolutions.jts.geomgraph.*;

/**
 * An EdgeSetIntersector computes all the intersections between the
 * edges in the set.  It adds the computed intersections to each edge
 * they are found on.  It may be used in two scenarios:
 * <ul>
 * <li>determining the internal intersections between a single set of edges
 * <li>determining the mutual intersections between two different sets of edges
 * </ul>
 * It uses a {@link SegmentIntersector} to compute the intersections between
 * segments and to record statistics about what kinds of intersections were found.
 *
 * @version 1.7
 */
public abstract class EdgeSetIntersector 
{
  public EdgeSetIntersector() {
  }

  /**
   * Computes all self-intersections between edges in a set of edges,
   * allowing client to choose whether self-intersections are computed.
   *
   * @param edges a list of edges to test for intersections
   * @param si the SegmentIntersector to use
   * @param testAllSegments true if self-intersections are to be tested as well
   */
  abstract public void computeIntersections(List edges, SegmentIntersector si, boolean testAllSegments);

  /**
   * Computes all mutual intersections between two sets of edges.
   */
  abstract public void computeIntersections(List edges0, List edges1, SegmentIntersector si);



}
