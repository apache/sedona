

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
package com.vividsolutions.jts.operation.buffer;

/**
 * @version 1.7
 */

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.util.*;
//import debug.*;

/**
 * A connected subset of the graph of
 * {@link DirectedEdge}s and {@link Node}s.
 * Its edges will generate either
 * <ul>
 * <li> a single polygon in the complete buffer, with zero or more holes, or
 * <li> one or more connected holes
 * </ul>
 *
 *
 * @version 1.7
 */
class BufferSubgraph
  implements Comparable
{
  private RightmostEdgeFinder finder;
  private List dirEdgeList  = new ArrayList();
  private List nodes        = new ArrayList();
  private Coordinate rightMostCoord = null;
  private Envelope env = null;

  public BufferSubgraph()
  {
    finder = new RightmostEdgeFinder();
  }

  public List getDirectedEdges() { return dirEdgeList; }
  public List getNodes() { return nodes; }

  /**
   * Computes the envelope of the edges in the subgraph.
   * The envelope is cached after being computed.
   *
   * @return the envelope of the graph.
   */
  public Envelope getEnvelope()
  {
    if (env == null) {
      Envelope edgeEnv = new Envelope();
      for (Iterator it = dirEdgeList.iterator(); it.hasNext(); ) {
        DirectedEdge dirEdge = (DirectedEdge) it.next();
        Coordinate[] pts = dirEdge.getEdge().getCoordinates();
        for (int i = 0; i < pts.length - 1; i++) {
          edgeEnv.expandToInclude(pts[i]);
        }
      }
      env = edgeEnv;
    }
    return env;
  }

  /**
   * Gets the rightmost coordinate in the edges of the subgraph
   */
  public Coordinate getRightmostCoordinate()
  {
    return rightMostCoord;
  }

  /**
   * Creates the subgraph consisting of all edges reachable from this node.
   * Finds the edges in the graph and the rightmost coordinate.
   *
   * @param node a node to start the graph traversal from
   */
  public void create(Node node)
  {
    addReachable(node);
    finder.findEdge(dirEdgeList);
    rightMostCoord = finder.getCoordinate();
  }

  /**
   * Adds all nodes and edges reachable from this node to the subgraph.
   * Uses an explicit stack to avoid a large depth of recursion.
   *
   * @param node a node known to be in the subgraph
   */
  private void addReachable(Node startNode)
  {
    Stack nodeStack = new Stack();
    nodeStack.add(startNode);
    while (! nodeStack.empty()) {
      Node node = (Node) nodeStack.pop();
      add(node, nodeStack);
    }
  }

  /**
   * Adds the argument node and all its out edges to the subgraph
   * @param node the node to add
   * @param nodeStack the current set of nodes being traversed
   */
  private void add(Node node, Stack nodeStack)
  {
    node.setVisited(true);
    nodes.add(node);
    for (Iterator i = ((DirectedEdgeStar) node.getEdges()).iterator(); i.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) i.next();
      dirEdgeList.add(de);
      DirectedEdge sym = de.getSym();
      Node symNode = sym.getNode();
      /**
       * NOTE: this is a depth-first traversal of the graph.
       * This will cause a large depth of recursion.
       * It might be better to do a breadth-first traversal.
       */
      if (! symNode.isVisited()) nodeStack.push(symNode);
    }
  }

  private void clearVisitedEdges()
  {
    for (Iterator it = dirEdgeList.iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      de.setVisited(false);
    }
  }

  public void computeDepth(int outsideDepth)
  {
    clearVisitedEdges();
    // find an outside edge to assign depth to
    DirectedEdge de = finder.getEdge();
    Node n = de.getNode();
    Label label = de.getLabel();
    // right side of line returned by finder is on the outside
    de.setEdgeDepths(Position.RIGHT, outsideDepth);
    copySymDepths(de);

    //computeNodeDepth(n, de);
    computeDepths(de);
  }

  /**
   * Compute depths for all dirEdges via breadth-first traversal of nodes in graph
   * @param startEdge edge to start processing with
   */
  // <FIX> MD - use iteration & queue rather than recursion, for speed and robustness
  private void computeDepths(DirectedEdge startEdge)
  {
    Set nodesVisited = new HashSet();
    LinkedList nodeQueue = new LinkedList();

    Node startNode = startEdge.getNode();
    nodeQueue.addLast(startNode);
    nodesVisited.add(startNode);
    startEdge.setVisited(true);

    while (! nodeQueue.isEmpty()) {
//System.out.println(nodes.size() + " queue: " + nodeQueue.size());
      Node n = (Node) nodeQueue.removeFirst();
      nodesVisited.add(n);
      // compute depths around node, starting at this edge since it has depths assigned
      computeNodeDepth(n);

      // add all adjacent nodes to process queue,
      // unless the node has been visited already
      for (Iterator i = ((DirectedEdgeStar) n.getEdges()).iterator(); i.hasNext(); ) {
        DirectedEdge de = (DirectedEdge) i.next();
        DirectedEdge sym = de.getSym();
        if (sym.isVisited()) continue;
        Node adjNode = sym.getNode();
        if (! (nodesVisited.contains(adjNode)) ) {
          nodeQueue.addLast(adjNode);
          nodesVisited.add(adjNode);
        }
      }
    }
  }

  private void computeNodeDepth(Node n)
  {
    // find a visited dirEdge to start at
    DirectedEdge startEdge = null;
    for (Iterator i = ((DirectedEdgeStar) n.getEdges()).iterator(); i.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) i.next();
      if (de.isVisited() || de.getSym().isVisited()) {
        startEdge = de;
        break;
      }
    }
    // MD - testing  Result: breaks algorithm
    //if (startEdge == null) return;
    
    // only compute string append if assertion would fail
    if (startEdge == null)
    	throw new TopologyException("unable to find edge to compute depths at " + n.getCoordinate());

    ((DirectedEdgeStar) n.getEdges()).computeDepths(startEdge);

    // copy depths to sym edges
    for (Iterator i = ((DirectedEdgeStar) n.getEdges()).iterator(); i.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) i.next();
      de.setVisited(true);
      copySymDepths(de);
    }
  }

  private void copySymDepths(DirectedEdge de)
  {
    DirectedEdge sym = de.getSym();
    sym.setDepth(Position.LEFT, de.getDepth(Position.RIGHT));
    sym.setDepth(Position.RIGHT, de.getDepth(Position.LEFT));
  }

  /**
   * Find all edges whose depths indicates that they are in the result area(s).
   * Since we want polygon shells to be
   * oriented CW, choose dirEdges with the interior of the result on the RHS.
   * Mark them as being in the result.
   * Interior Area edges are the result of dimensional collapses.
   * They do not form part of the result area boundary.
   */
  public void findResultEdges()
  {
    for (Iterator it = dirEdgeList.iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      /**
       * Select edges which have an interior depth on the RHS
       * and an exterior depth on the LHS.
       * Note that because of weird rounding effects there may be
       * edges which have negative depths!  Negative depths
       * count as "outside".
       */
      // <FIX> - handle negative depths
      if (    de.getDepth(Position.RIGHT) >= 1
          &&  de.getDepth(Position.LEFT)  <= 0
          &&  ! de.isInteriorAreaEdge()) {
        de.setInResult(true);
//Debug.print("in result "); Debug.println(de);
      }
    }
  }

  /**
   * BufferSubgraphs are compared on the x-value of their rightmost Coordinate.
   * This defines a partial ordering on the graphs such that:
   * <p>
   * g1 >= g2 <==> Ring(g2) does not contain Ring(g1)
   * <p>
   * where Polygon(g) is the buffer polygon that is built from g.
   * <p>
   * This relationship is used to sort the BufferSubgraphs so that shells are guaranteed to
   * be built before holes.
   */
  public int compareTo(Object o) {
    BufferSubgraph graph = (BufferSubgraph) o;
    if (this.rightMostCoord.x < graph.rightMostCoord.x) {
      return -1;
    }
    if (this.rightMostCoord.x > graph.rightMostCoord.x) {
      return 1;
    }
    return 0;
  }

/*
// DEBUGGING only - comment out
  private static final String SAVE_DIREDGES = "saveDirEdges";
  private static int saveCount = 0;
  public void saveDirEdges()
  {
    GeometryFactory fact = new GeometryFactory();
    for (Iterator it = dirEdgeList.iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      double dx = de.getDx();
      double dy = de.getDy();
      Coordinate p0 = de.getCoordinate();
      double ang = Math.atan2(dy, dx);
      Coordinate p1 = new Coordinate(
          p0.x + .4 * Math.cos(ang),
          p0.y + .4 * Math.sin(ang));
//      DebugFeature.add(SAVE_DIREDGES,
//                       fact.createLineString(new Coordinate[] { p0, p1 } ),
//                       de.getDepth(Position.LEFT) + "/" + de.getDepth(Position.RIGHT)
//                       );
    }
  String filepath = "x:\\jts\\testBuffer\\dirEdges" + saveCount++ + ".jml";
    DebugFeature.saveFeatures(SAVE_DIREDGES, filepath);
  }
  */
}
