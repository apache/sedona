


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
import com.vividsolutions.jts.geom.Location;
import com.vividsolutions.jts.geomgraph.Node;

/**
 * A map of nodes, indexed by the coordinate of the node
 * @version 1.7
 */
public class NodeMap

{
  //Map nodeMap = new HashMap();
  Map nodeMap = new TreeMap();
  NodeFactory nodeFact;

  public NodeMap(NodeFactory nodeFact) {
    this.nodeFact = nodeFact;
  }

  /**
   * Factory function - subclasses can override to create their own types of nodes
   */
   /*
  protected Node createNode(Coordinate coord)
  {
    return new Node(coord);
  }
  */
  /**
   * This method expects that a node has a coordinate value.
   */
  public Node addNode(Coordinate coord)
  {
    Node node = (Node) nodeMap.get(coord);
    if (node == null) {
      node = nodeFact.createNode(coord);
      nodeMap.put(coord, node);
    }
    return node;
  }

  public Node addNode(Node n)
  {
    Node node = (Node) nodeMap.get(n.getCoordinate());
    if (node == null) {
      nodeMap.put(n.getCoordinate(), n);
      return n;
    }
    node.mergeLabel(n);
    return node;
  }

  /**
   * Adds a node for the start point of this EdgeEnd
   * (if one does not already exist in this map).
   * Adds the EdgeEnd to the (possibly new) node.
   */
  public void add(EdgeEnd e)
  {
    Coordinate p = e.getCoordinate();
    Node n = addNode(p);
    n.add(e);
  }
  /**
   * @return the node if found; null otherwise
   */
  public Node find(Coordinate coord)  {    return (Node) nodeMap.get(coord);  }

  public Iterator iterator()
  {
    return nodeMap.values().iterator();
  }
  public Collection values()
  {
    return nodeMap.values();
  }

  public Collection getBoundaryNodes(int geomIndex)
  {
    Collection bdyNodes = new ArrayList();
    for (Iterator i = iterator(); i.hasNext(); ) {
      Node node = (Node) i.next();
      if (node.getLabel().getLocation(geomIndex) == Location.BOUNDARY)
        bdyNodes.add(node);
    }
    return bdyNodes;
  }

  public void print(PrintStream out)
  {
    for (Iterator it = iterator(); it.hasNext(); )
    {
      Node n = (Node) it.next();
      n.print(out);
    }
  }
}
