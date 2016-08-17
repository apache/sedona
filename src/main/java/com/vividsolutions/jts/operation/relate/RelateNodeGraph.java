
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
package com.vividsolutions.jts.operation.relate;

/**
 * @version 1.7
 */
import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.*;
import com.vividsolutions.jts.geomgraph.*;

/**
 * Implements the simple graph of Nodes and EdgeEnd which is all that is
 * required to determine topological relationships between Geometries.
 * Also supports building a topological graph of a single Geometry, to
 * allow verification of valid topology.
 * <p>
 * It is <b>not</b> necessary to create a fully linked
 * PlanarGraph to determine relationships, since it is sufficient
 * to know how the Geometries interact locally around the nodes.
 * In fact, this is not even feasible, since it is not possible to compute
 * exact intersection points, and hence the topology around those nodes
 * cannot be computed robustly.
 * The only Nodes that are created are for improper intersections;
 * that is, nodes which occur at existing vertices of the Geometries.
 * Proper intersections (e.g. ones which occur between the interior of line segments)
 * have their topology determined implicitly, without creating a Node object
 * to represent them.
 *
 * @version 1.7
 */
public class RelateNodeGraph {

  private NodeMap nodes = new NodeMap(new RelateNodeFactory());

  public RelateNodeGraph() {
  }

  public Iterator getNodeIterator() { return nodes.iterator(); }

  public void build(GeometryGraph geomGraph)
  {
      // compute nodes for intersections between previously noded edges
    computeIntersectionNodes(geomGraph, 0);
    /**
     * Copy the labelling for the nodes in the parent Geometry.  These override
     * any labels determined by intersections.
     */
    copyNodesAndLabels(geomGraph, 0);

    /**
     * Build EdgeEnds for all intersections.
     */
    EdgeEndBuilder eeBuilder = new EdgeEndBuilder();
    List eeList = eeBuilder.computeEdgeEnds(geomGraph.getEdgeIterator());
    insertEdgeEnds(eeList);

//Debug.println("==== NodeList ===");
//Debug.print(nodes);
  }

  /**
   * Insert nodes for all intersections on the edges of a Geometry.
   * Label the created nodes the same as the edge label if they do not already have a label.
   * This allows nodes created by either self-intersections or
   * mutual intersections to be labelled.
   * Endpoint nodes will already be labelled from when they were inserted.
   * <p>
   * Precondition: edge intersections have been computed.
   */
  public void computeIntersectionNodes(GeometryGraph geomGraph, int argIndex)
  {
    for (Iterator edgeIt = geomGraph.getEdgeIterator(); edgeIt.hasNext(); ) {
      Edge e = (Edge) edgeIt.next();
      int eLoc = e.getLabel().getLocation(argIndex);
      for (Iterator eiIt = e.getEdgeIntersectionList().iterator(); eiIt.hasNext(); ) {
        EdgeIntersection ei = (EdgeIntersection) eiIt.next();
        RelateNode n = (RelateNode) nodes.addNode(ei.coord);
        if (eLoc == Location.BOUNDARY)
          n.setLabelBoundary(argIndex);
        else {
          if (n.getLabel().isNull(argIndex))
            n.setLabel(argIndex, Location.INTERIOR);
        }
//Debug.println(n);
      }
    }
  }

    /**
     * Copy all nodes from an arg geometry into this graph.
     * The node label in the arg geometry overrides any previously computed
     * label for that argIndex.
     * (E.g. a node may be an intersection node with
     * a computed label of BOUNDARY,
     * but in the original arg Geometry it is actually
     * in the interior due to the Boundary Determination Rule)
     */
  public void copyNodesAndLabels(GeometryGraph geomGraph, int argIndex)
  {
    for (Iterator nodeIt = geomGraph.getNodeIterator(); nodeIt.hasNext(); ) {
      Node graphNode = (Node) nodeIt.next();
      Node newNode = nodes.addNode(graphNode.getCoordinate());
      newNode.setLabel(argIndex, graphNode.getLabel().getLocation(argIndex));
//node.print(System.out);
    }
  }

  public void insertEdgeEnds(List ee)
  {
    for (Iterator i = ee.iterator(); i.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) i.next();
      nodes.add(e);
    }
  }


}
