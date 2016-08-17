
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

/**
 * Represents an undirected edge of a {@link PlanarGraph}. An undirected edge
 * in fact simply acts as a central point of reference for two opposite
 * {@link DirectedEdge}s.
 * <p>
 * Usually a client using a <code>PlanarGraph</code> will subclass <code>Edge</code>
 * to add its own application-specific data and methods.
 *
 * @version 1.7
 */
public class Edge
    extends GraphComponent
{

  /**
   * The two DirectedEdges associated with this Edge.
   * Index 0 is forward, 1 is reverse.
   */
  protected DirectedEdge[] dirEdge;

  /**
   * Constructs an Edge whose DirectedEdges are not yet set. Be sure to call
   * {@link #setDirectedEdges(DirectedEdge, DirectedEdge)}
   */
  public Edge()
  {
  }

  /**
   * Constructs an Edge initialized with the given DirectedEdges, and for each
   * DirectedEdge: sets the Edge, sets the symmetric DirectedEdge, and adds
   * this Edge to its from-Node.
   */
  public Edge(DirectedEdge de0, DirectedEdge de1)
  {
    setDirectedEdges(de0, de1);
  }

  /**
   * Initializes this Edge's two DirectedEdges, and for each DirectedEdge: sets the
   * Edge, sets the symmetric DirectedEdge, and adds this Edge to its from-Node.
   */
  public void setDirectedEdges(DirectedEdge de0, DirectedEdge de1)
  {
    dirEdge = new DirectedEdge[] { de0, de1 };
    de0.setEdge(this);
    de1.setEdge(this);
    de0.setSym(de1);
    de1.setSym(de0);
    de0.getFromNode().addOutEdge(de0);
    de1.getFromNode().addOutEdge(de1);
  }

  /**
   * Returns one of the DirectedEdges associated with this Edge.
   * @param i 0 or 1.  0 returns the forward directed edge, 1 returns the reverse
   */
  public DirectedEdge getDirEdge(int i)
  {
    return dirEdge[i];
  }

  /**
   * Returns the {@link DirectedEdge} that starts from the given node, or null if the
   * node is not one of the two nodes associated with this Edge.
   */
  public DirectedEdge getDirEdge(Node fromNode)
  {
    if (dirEdge[0].getFromNode() == fromNode) return dirEdge[0];
    if (dirEdge[1].getFromNode() == fromNode) return dirEdge[1];
    // node not found
    // possibly should throw an exception here?
    return null;
  }

  /**
   * If <code>node</code> is one of the two nodes associated with this Edge,
   * returns the other node; otherwise returns null.
   */
  public Node getOppositeNode(Node node)
  {
    if (dirEdge[0].getFromNode() == node) return dirEdge[0].getToNode();
    if (dirEdge[1].getFromNode() == node) return dirEdge[1].getToNode();
    // node not found
    // possibly should throw an exception here?
    return null;
  }

  /**
   * Removes this edge from its containing graph.
   */
  void remove() {
    this.dirEdge = null;
  }

  /**
   * Tests whether this edge has been removed from its containing graph
   *
   * @return <code>true</code> if this edge is removed
   */
  public boolean isRemoved()
  {
    return dirEdge == null;
  }

}
