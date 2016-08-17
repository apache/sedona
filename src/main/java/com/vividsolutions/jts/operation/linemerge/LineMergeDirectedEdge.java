
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
package com.vividsolutions.jts.operation.linemerge;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.planargraph.DirectedEdge;
import com.vividsolutions.jts.planargraph.Node;
import com.vividsolutions.jts.util.Assert;

/**
 * A {@link com.vividsolutions.jts.planargraph.DirectedEdge} of a 
 * {@link LineMergeGraph}. 
 *
 * @version 1.7
 */
public class LineMergeDirectedEdge extends DirectedEdge {
  /**
   * Constructs a LineMergeDirectedEdge connecting the <code>from</code> node to the
   * <code>to</code> node.
   *
   * @param directionPt
   *                  specifies this DirectedEdge's direction (given by an imaginary
   *                  line from the <code>from</code> node to <code>directionPt</code>)
   * @param edgeDirection
   *                  whether this DirectedEdge's direction is the same as or
   *                  opposite to that of the parent Edge (if any)
   */  
  public LineMergeDirectedEdge(Node from, Node to, Coordinate directionPt,
    boolean edgeDirection) {
    super(from, to, directionPt, edgeDirection);
  }

  /**
   * Returns the directed edge that starts at this directed edge's end point, or null
   * if there are zero or multiple directed edges starting there.  
   * @return the directed edge
   */
  public LineMergeDirectedEdge getNext() {
    if (getToNode().getDegree() != 2) {
      return null;
    }
    if (getToNode().getOutEdges().getEdges().get(0) == getSym()) {
      return (LineMergeDirectedEdge) getToNode().getOutEdges().getEdges().get(1);
    }
    Assert.isTrue(getToNode().getOutEdges().getEdges().get(1) == getSym());

    return (LineMergeDirectedEdge) getToNode().getOutEdges().getEdges().get(0);
  }
}
