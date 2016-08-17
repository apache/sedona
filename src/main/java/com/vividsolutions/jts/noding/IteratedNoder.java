
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

import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geom.*;
import java.util.*;

/**
 * Nodes a set of {@link NodedSegmentString}s completely.
 * The set of segment strings is fully noded;
 * i.e. noding is repeated until no further
 * intersections are detected.
 * <p>
 * Iterated noding using a FLOATING precision model is not guaranteed to converge,
 * due to roundoff error.   
 * This problem is detected and an exception is thrown.
 * Clients can choose to rerun the noding using a lower precision model.
 *
 * @version 1.7
 */
public class IteratedNoder
    implements Noder
{
  public static final int MAX_ITER = 5;

  private PrecisionModel pm;
  private LineIntersector li;
  private Collection nodedSegStrings;
  private int maxIter = MAX_ITER;

  public IteratedNoder(PrecisionModel pm)
  {
    li = new RobustLineIntersector();
    this.pm = pm;
    li.setPrecisionModel(pm);
  }

  /**
   * Sets the maximum number of noding iterations performed before
   * the noding is aborted.
   * Experience suggests that this should rarely need to be changed
   * from the default.
   * The default is MAX_ITER.
   *
   * @param maxIter the maximum number of iterations to perform
   */
  public void setMaximumIterations(int maxIter)
  {
    this.maxIter = maxIter;
  }

  public Collection getNodedSubstrings()  {    return nodedSegStrings;  }

  /**
   * Fully nodes a list of {@link SegmentString}s, i.e. peforms noding iteratively
   * until no intersections are found between segments.
   * Maintains labelling of edges correctly through
   * the noding.
   *
   * @param segStrings a collection of SegmentStrings to be noded
   * @throws TopologyException if the iterated noding fails to converge.
   */
  public void computeNodes(Collection segStrings)
    throws TopologyException
  {
    int[] numInteriorIntersections = new int[1];
    nodedSegStrings = segStrings;
    int nodingIterationCount = 0;
    int lastNodesCreated = -1;
    do {
      node(nodedSegStrings, numInteriorIntersections);
      nodingIterationCount++;
      int nodesCreated = numInteriorIntersections[0];

      /**
       * Fail if the number of nodes created is not declining.
       * However, allow a few iterations at least before doing this
       */
//System.out.println("# nodes created: " + nodesCreated);
      if (lastNodesCreated > 0
          && nodesCreated >= lastNodesCreated
          && nodingIterationCount > maxIter) {
        throw new TopologyException("Iterated noding failed to converge after "
                                    + nodingIterationCount + " iterations");
      }
      lastNodesCreated = nodesCreated;

    } while (lastNodesCreated > 0);
//System.out.println("# nodings = " + nodingIterationCount);
  }


/**
 * Node the input segment strings once
 * and create the split edges between the nodes
 */
  private void node(Collection segStrings, int[] numInteriorIntersections)
  {
    IntersectionAdder si = new IntersectionAdder(li);
    MCIndexNoder noder = new MCIndexNoder();
    noder.setSegmentIntersector(si);
    noder.computeNodes(segStrings);
    nodedSegStrings = noder.getNodedSubstrings();
    numInteriorIntersections[0] = si.numInteriorIntersections;
//System.out.println("# intersection tests: " + si.numTests);
  }

}
