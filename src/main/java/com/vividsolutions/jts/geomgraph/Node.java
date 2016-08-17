


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
package com.vividsolutions.jts.geomgraph;

import java.io.PrintStream;
import java.util.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.IntersectionMatrix;
import com.vividsolutions.jts.geom.Location;
import com.vividsolutions.jts.geomgraph.GraphComponent;
import com.vividsolutions.jts.geomgraph.Label;
import com.vividsolutions.jts.util.*;


/**
 * @version 1.7
 */
public class Node
  extends GraphComponent
{
  protected Coordinate coord; // only non-null if this node is precise
  protected EdgeEndStar edges;

  public Node(Coordinate coord, EdgeEndStar edges)
  {
    this.coord = coord;
    this.edges = edges;
    label = new Label(0, Location.NONE);
  }

  public Coordinate getCoordinate() { return coord; }
  public EdgeEndStar getEdges() { return edges; }

  /**
   * Tests whether any incident edge is flagged as
   * being in the result.
   * This test can be used to determine if the node is in the result,
   * since if any incident edge is in the result, the node must be in the result as well.
   *
   * @return <code>true</code> if any indicident edge in the in the result
   */
  public boolean isIncidentEdgeInResult()
  {
    for (Iterator it = getEdges().getEdges().iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      if (de.getEdge().isInResult())
        return true;
    }
    return false;
  }

  public boolean isIsolated()
  {
    return (label.getGeometryCount() == 1);
  }
  /**
   * Basic nodes do not compute IMs
   */
  protected void computeIM(IntersectionMatrix im) {}
  /**
   * Add the edge to the list of edges at this node
   */
  public void add(EdgeEnd e)
  {
    // Assert: start pt of e is equal to node point
    edges.insert(e);
    e.setNode(this);
  }

  public void mergeLabel(Node n)
  {
    mergeLabel(n.label);
  }

  /**
   * To merge labels for two nodes,
   * the merged location for each LabelElement is computed.
   * The location for the corresponding node LabelElement is set to the result,
   * as long as the location is non-null.
   */

  public void mergeLabel(Label label2)
  {
    for (int i = 0; i < 2; i++) {
      int loc = computeMergedLocation(label2, i);
      int thisLoc = label.getLocation(i);
      if (thisLoc == Location.NONE) label.setLocation(i, loc);
    }
  }

  public void setLabel(int argIndex, int onLocation)
  {
    if (label == null) {
      label = new Label(argIndex, onLocation);
    }
    else
      label.setLocation(argIndex, onLocation);
  }

  /**
   * Updates the label of a node to BOUNDARY,
   * obeying the mod-2 boundaryDetermination rule.
   */
  public void setLabelBoundary(int argIndex)
  {
    if (label == null) return;

    // determine the current location for the point (if any)
    int loc = Location.NONE;
    if (label != null)
      loc = label.getLocation(argIndex);
    // flip the loc
    int newLoc;
    switch (loc) {
    case Location.BOUNDARY: newLoc = Location.INTERIOR; break;
    case Location.INTERIOR: newLoc = Location.BOUNDARY; break;
    default: newLoc = Location.BOUNDARY;  break;
    }
    label.setLocation(argIndex, newLoc);
  }

  /**
   * The location for a given eltIndex for a node will be one
   * of { null, INTERIOR, BOUNDARY }.
   * A node may be on both the boundary and the interior of a geometry;
   * in this case, the rule is that the node is considered to be in the boundary.
   * The merged location is the maximum of the two input values.
   */
  int computeMergedLocation(Label label2, int eltIndex)
  {
    int loc = Location.NONE;
    loc = label.getLocation(eltIndex);
    if (! label2.isNull(eltIndex)) {
        int nLoc = label2.getLocation(eltIndex);
        if (loc != Location.BOUNDARY) loc = nLoc;
    }
    return loc;
  }

  public void print(PrintStream out)
  {
    out.println("node " + coord + " lbl: " + label);
  }
}
