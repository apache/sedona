
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
package com.vividsolutions.jts.planargraph;

import java.util.*;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Represents a directed graph which is embeddable in a planar surface.
 * <p>
 * This class and the other classes in this package serve as a framework for
 * building planar graphs for specific algorithms. This class must be
 * subclassed to expose appropriate methods to construct the graph. This allows
 * controlling the types of graph components ({@link DirectedEdge}s,
 * {@link Edge}s and {@link Node}s) which can be added to the graph. An
 * application which uses the graph framework will almost always provide
 * subclasses for one or more graph components, which hold application-specific
 * data and graph algorithms.
 *
 * @version 1.7
 */
public abstract class PlanarGraph
{
  protected Set edges = new HashSet();
  protected Set dirEdges = new HashSet();
  protected NodeMap nodeMap = new NodeMap();

  /**
   * Constructs a empty graph.
   */
  public PlanarGraph()
  {
  }

  /**
   * Returns the {@link Node} at the given location,
   * or null if no {@link Node} was there.
   *
   * @param pt the location to query
   * @return the node found
   * or <code>null</code> if this graph contains no node at the location
   */
  public Node findNode(Coordinate pt)
  {
    return (Node) nodeMap.find(pt);
  }

  /**
   * Adds a node to the map, replacing any that is already at that location.
   * Only subclasses can add Nodes, to ensure Nodes are of the right type.
   * 
   * @param node the node to add
   */
  protected void add(Node node)
  {
    nodeMap.add(node);
  }

  /**
   * Adds the Edge and its DirectedEdges with this PlanarGraph.
   * Assumes that the Edge has already been created with its associated DirectEdges.
   * Only subclasses can add Edges, to ensure the edges added are of the right class.
   */
  protected void add(Edge edge)
  {
    edges.add(edge);
    add(edge.getDirEdge(0));
    add(edge.getDirEdge(1));
  }

  /**
   * Adds the Edge to this PlanarGraph; only subclasses can add DirectedEdges,
   * to ensure the edges added are of the right class.
   */
  protected void add(DirectedEdge dirEdge)
  {
    dirEdges.add(dirEdge);
  }
  /**
   * Returns an Iterator over the Nodes in this PlanarGraph.
   */
  public Iterator nodeIterator()  {    return nodeMap.iterator();  }
  /**
   * Returns the Nodes in this PlanarGraph.
   */

  /**
   * Tests whether this graph contains the given {@link Edge}
   *
   * @param e the edge to query
   * @return <code>true</code> if the graph contains the edge
   */
  public boolean contains(Edge e)
  {
    return edges.contains(e);
  }

  /**
   * Tests whether this graph contains the given {@link DirectedEdge}
   *
   * @param de the directed edge to query
   * @return <code>true</code> if the graph contains the directed edge
   */
  public boolean contains(DirectedEdge de)
  {
    return dirEdges.contains(de);
  }

  public Collection getNodes()  {    return nodeMap.values();  }

  /**
   * Returns an Iterator over the DirectedEdges in this PlanarGraph, in the order in which they
   * were added.
   *
   * @see #add(Edge)
   * @see #add(DirectedEdge)
   */
  public Iterator dirEdgeIterator()  {    return dirEdges.iterator();  }
  /**
   * Returns an Iterator over the Edges in this PlanarGraph, in the order in which they
   * were added.
   *
   * @see #add(Edge)
   */
  public Iterator edgeIterator()  {    return edges.iterator();  }

  /**
   * Returns the Edges that have been added to this PlanarGraph
   * @see #add(Edge)
   */
  public Collection getEdges()  {    return edges;  }

  /**
   * Removes an {@link Edge} and its associated {@link DirectedEdge}s
   * from their from-Nodes and from the graph.
   * Note: This method does not remove the {@link Node}s associated
   * with the {@link Edge}, even if the removal of the {@link Edge}
   * reduces the degree of a {@link Node} to zero.
   */
  public void remove(Edge edge)
  {
    remove(edge.getDirEdge(0));
    remove(edge.getDirEdge(1));
    edges.remove(edge);
    edge.remove();
  }

  /**
   * Removes a {@link DirectedEdge} from its from-{@link Node} and from this graph.
   * This method does not remove the {@link Node}s associated with the DirectedEdge,
   * even if the removal of the DirectedEdge reduces the degree of a Node to zero.
   */
  public void remove(DirectedEdge de)
  {
    DirectedEdge sym = de.getSym();
    if (sym != null) sym.setSym(null);
    
    de.getFromNode().remove(de);
    de.remove();
    dirEdges.remove(de);
  }

  /**
   * Removes a node from the graph, along with any associated DirectedEdges and
   * Edges.
   */
  public void remove(Node node)
  {
    // unhook all directed edges
    List outEdges = node.getOutEdges().getEdges();
    for (Iterator i = outEdges.iterator(); i.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) i.next();
      DirectedEdge sym = de.getSym();
      // remove the diredge that points to this node
      if (sym != null) remove(sym);
      // remove this diredge from the graph collection
      dirEdges.remove(de);

      Edge edge = de.getEdge();
      if (edge != null) {
        edges.remove(edge);
      }

    }
    // remove the node from the graph
    nodeMap.remove(node.getCoordinate());
    node.remove();
  }

  /**
   * Returns all Nodes with the given number of Edges around it.
   */
  public List findNodesOfDegree(int degree)
  {
    List nodesFound = new ArrayList();
    for (Iterator i = nodeIterator(); i.hasNext(); ) {
      Node node = (Node) i.next();
      if (node.getDegree() == degree)
        nodesFound.add(node);
    }
    return nodesFound;
  }

}
