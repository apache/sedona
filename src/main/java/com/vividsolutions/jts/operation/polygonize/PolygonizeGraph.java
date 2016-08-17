
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


package com.vividsolutions.jts.operation.polygonize;

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.Assert;
import com.vividsolutions.jts.planargraph.*;

/**
 * Represents a planar graph of edges that can be used to compute a
 * polygonization, and implements the algorithms to compute the
 * {@link EdgeRings} formed by the graph.
 * <p>
 * The marked flag on {@link DirectedEdge}s is used to indicate that a directed edge
 * has be logically deleted from the graph.
 *
 * @version 1.7
 */
class PolygonizeGraph
    extends PlanarGraph
{

  private static int getDegreeNonDeleted(Node node)
  {
    List edges = node.getOutEdges().getEdges();
    int degree = 0;
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      if (! de.isMarked()) degree++;
    }
    return degree;
  }

  private static int getDegree(Node node, long label)
  {
    List edges = node.getOutEdges().getEdges();
    int degree = 0;
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      if (de.getLabel() == label) degree++;
    }
    return degree;
  }

  /**
   * Deletes all edges at a node
   */
  public static void deleteAllEdges(Node node)
  {
    List edges = node.getOutEdges().getEdges();
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      de.setMarked(true);
      PolygonizeDirectedEdge sym = (PolygonizeDirectedEdge) de.getSym();
      if (sym != null)
        sym.setMarked(true);
    }
  }

  private GeometryFactory factory;

  //private List labelledRings;
 
  /**
   * Create a new polygonization graph.
   */
  public PolygonizeGraph(GeometryFactory factory)
  {
    this.factory = factory;
  }

  /**
   * Add a {@link LineString} forming an edge of the polygon graph.
   * @param line the line to add
   */
  public void addEdge(LineString line)
  {
    if (line.isEmpty()) { return; }
    Coordinate[] linePts = CoordinateArrays.removeRepeatedPoints(line.getCoordinates());
    
    if (linePts.length < 2) { return; }
    
    Coordinate startPt = linePts[0];
    Coordinate endPt = linePts[linePts.length - 1];

    Node nStart = getNode(startPt);
    Node nEnd = getNode(endPt);

    DirectedEdge de0 = new PolygonizeDirectedEdge(nStart, nEnd, linePts[1], true);
    DirectedEdge de1 = new PolygonizeDirectedEdge(nEnd, nStart, linePts[linePts.length - 2], false);
    Edge edge = new PolygonizeEdge(line);
    edge.setDirectedEdges(de0, de1);
    add(edge);
  }

  private Node getNode(Coordinate pt)
  {
    Node node = findNode(pt);
    if (node == null) {
      node = new Node(pt);
      // ensure node is only added once to graph
      add(node);
    }
    return node;
  }

  private void computeNextCWEdges()
  {
    // set the next pointers for the edges around each node
    for (Iterator iNode = nodeIterator(); iNode.hasNext(); ) {
      Node node = (Node) iNode.next();
      computeNextCWEdges(node);
    }
  }

  /**
   * Convert the maximal edge rings found by the initial graph traversal
   * into the minimal edge rings required by JTS polygon topology rules.
   *
   * @param ringEdges the list of start edges for the edgeRings to convert.
   */
  private void convertMaximalToMinimalEdgeRings(List ringEdges)
  {
    for (Iterator i = ringEdges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      long label = de.getLabel();
      List intNodes = findIntersectionNodes(de, label);

      if (intNodes == null) continue;
      // flip the next pointers on the intersection nodes to create minimal edge rings
      for (Iterator iNode = intNodes.iterator(); iNode.hasNext(); ) {
        Node node = (Node) iNode.next();
        computeNextCCWEdges(node, label);
      }
    }
  }

  /**
   * Finds all nodes in a maximal edgering which are self-intersection nodes
   * @param startDE
   * @param label
   * @return the list of intersection nodes found,
   * or <code>null</code> if no intersection nodes were found
   */
  private static List findIntersectionNodes(PolygonizeDirectedEdge startDE, long label)
  {
    PolygonizeDirectedEdge de = startDE;
    List intNodes = null;
    do {
      Node node = de.getFromNode();
      if (getDegree(node, label) > 1) {
        if (intNodes == null)
          intNodes = new ArrayList();
        intNodes.add(node);
      }

      de = de.getNext();
      Assert.isTrue(de != null, "found null DE in ring");
      Assert.isTrue(de == startDE || ! de.isInRing(), "found DE already in ring");
    } while (de != startDE);

    return intNodes;
  }

  /**
   * Computes the minimal EdgeRings formed by the edges in this graph.
   * @return a list of the {@link EdgeRing}s found by the polygonization process.
   */
  public List getEdgeRings()
  {
    // maybe could optimize this, since most of these pointers should be set correctly already
    // by deleteCutEdges()
    computeNextCWEdges();
    // clear labels of all edges in graph
    label(dirEdges, -1);
    List maximalRings = findLabeledEdgeRings(dirEdges);
    convertMaximalToMinimalEdgeRings(maximalRings);

    // find all edgerings (which will now be minimal ones, as required)
    List edgeRingList = new ArrayList();
    for (Iterator i = dirEdges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      if (de.isMarked()) continue;
      if (de.isInRing()) continue;

      EdgeRing er = findEdgeRing(de);
      edgeRingList.add(er);
    }
    return edgeRingList;
  }

  /**
   * Finds and labels all edgerings in the graph.
   * The edge rings are labeling with unique integers.
   * The labeling allows detecting cut edges.
   * 
   * @param dirEdges a List of the DirectedEdges in the graph
   * @return a List of DirectedEdges, one for each edge ring found
   */
  private static List findLabeledEdgeRings(Collection dirEdges)
  {
    List edgeRingStarts = new ArrayList();
    // label the edge rings formed
    long currLabel = 1;
    for (Iterator i = dirEdges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      if (de.isMarked()) continue;
      if (de.getLabel() >= 0) continue;

      edgeRingStarts.add(de);
      List edges = EdgeRing.findDirEdgesInRing(de);

      label(edges, currLabel);
      currLabel++;
    }
    return edgeRingStarts;
  }

  /**
   * Finds and removes all cut edges from the graph.
   * @return a list of the {@link LineString}s forming the removed cut edges
   */
  public List deleteCutEdges()
  {
    computeNextCWEdges();
    // label the current set of edgerings
    findLabeledEdgeRings(dirEdges);

    /**
     * Cut Edges are edges where both dirEdges have the same label.
     * Delete them, and record them
     */
    List cutLines = new ArrayList();
    for (Iterator i = dirEdges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      if (de.isMarked()) continue;

      PolygonizeDirectedEdge sym = (PolygonizeDirectedEdge) de.getSym();

      if (de.getLabel() == sym.getLabel()) {
        de.setMarked(true);
        sym.setMarked(true);

        // save the line as a cut edge
        PolygonizeEdge e = (PolygonizeEdge) de.getEdge();
        cutLines.add(e.getLine());
      }
    }
    return cutLines;
  }

  private static void label(Collection dirEdges, long label)
  {
    for (Iterator i = dirEdges.iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
      de.setLabel(label);
    }
  }
  private static void computeNextCWEdges(Node node)
  {
    DirectedEdgeStar deStar = node.getOutEdges();
    PolygonizeDirectedEdge startDE = null;
    PolygonizeDirectedEdge prevDE = null;

    // the edges are stored in CCW order around the star
    for (Iterator i = deStar.getEdges().iterator(); i.hasNext(); ) {
      PolygonizeDirectedEdge outDE = (PolygonizeDirectedEdge) i.next();
      if (outDE.isMarked()) continue;

      if (startDE == null)
        startDE = outDE;
      if (prevDE != null) {
        PolygonizeDirectedEdge sym = (PolygonizeDirectedEdge) prevDE.getSym();
        sym.setNext(outDE);
      }
      prevDE = outDE;
    }
    if (prevDE != null) {
      PolygonizeDirectedEdge sym = (PolygonizeDirectedEdge) prevDE.getSym();
      sym.setNext(startDE);
    }
  }
  /**
   * Computes the next edge pointers going CCW around the given node, for the
   * given edgering label.
   * This algorithm has the effect of converting maximal edgerings into minimal edgerings
   */
  private static void computeNextCCWEdges(Node node, long label)
  {
    DirectedEdgeStar deStar = node.getOutEdges();
    //PolyDirectedEdge lastInDE = null;
    PolygonizeDirectedEdge firstOutDE = null;
    PolygonizeDirectedEdge prevInDE = null;

    // the edges are stored in CCW order around the star
    List edges = deStar.getEdges();
    //for (Iterator i = deStar.getEdges().iterator(); i.hasNext(); ) {
    for (int i = edges.size() - 1; i >= 0; i--) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) edges.get(i);
      PolygonizeDirectedEdge sym = (PolygonizeDirectedEdge) de.getSym();

      PolygonizeDirectedEdge outDE = null;
      if (  de.getLabel() == label) outDE = de;
      PolygonizeDirectedEdge inDE = null;
      if (  sym.getLabel() == label) inDE =  sym;

      if (outDE == null && inDE == null) continue;  // this edge is not in edgering

      if (inDE != null) {
        prevInDE = inDE;
      }

      if (outDE != null) {
        if (prevInDE != null) {
          prevInDE.setNext(outDE);
          prevInDE = null;
        }
        if (firstOutDE == null)
          firstOutDE = outDE;
      }
    }
    if (prevInDE != null) {
      Assert.isTrue(firstOutDE != null);
      prevInDE.setNext(firstOutDE);
    }
  }

  private EdgeRing findEdgeRing(PolygonizeDirectedEdge startDE)
  {
    EdgeRing er = new EdgeRing(factory);
    er.build(startDE);
    return er;
  }

  /**
   * Marks all edges from the graph which are "dangles".
   * Dangles are which are incident on a node with degree 1.
   * This process is recursive, since removing a dangling edge
   * may result in another edge becoming a dangle.
   * In order to handle large recursion depths efficiently,
   * an explicit recursion stack is used
   *
   * @return a List containing the {@link LineString}s that formed dangles
   */
  public Collection deleteDangles()
  {
    List nodesToRemove = findNodesOfDegree(1);
    Set dangleLines = new HashSet();

    Stack nodeStack = new Stack();
    for (Iterator i = nodesToRemove.iterator(); i.hasNext(); ) {
      nodeStack.push(i.next());
    }

    while (! nodeStack.isEmpty()) {
      Node node = (Node) nodeStack.pop();

      deleteAllEdges(node);
      List nodeOutEdges = node.getOutEdges().getEdges();
      for (Iterator i = nodeOutEdges.iterator(); i.hasNext(); ) {
        PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) i.next();
        // delete this edge and its sym
        de.setMarked(true);
        PolygonizeDirectedEdge sym = (PolygonizeDirectedEdge) de.getSym();
        if (sym != null)
          sym.setMarked(true);

        // save the line as a dangle
        PolygonizeEdge e = (PolygonizeEdge) de.getEdge();
        dangleLines.add(e.getLine());

        Node toNode = de.getToNode();
        // add the toNode to the list to be processed, if it is now a dangle
        if (getDegreeNonDeleted(toNode) == 1)
          nodeStack.push(toNode);
      }
    }
    return dangleLines;
  }
  
  /**
   * Traverses the polygonized edge rings in the graph
   * and computes the depth parity (odd or even)
   * relative to the exterior of the graph.
   * If the client has requested that the output
   * be polygonally valid, only odd polygons will be constructed. 
   *
   */
  public void computeDepthParity()
  {
    while (true) {
      PolygonizeDirectedEdge de = null; //findLowestDirEdge();
      if (de == null)
        return;
      computeDepthParity(de);
    }
  }
  
  /**
   * Traverses all connected edges, computing the depth parity
   * of the associated polygons.
   * 
   * @param de
   */
  private void computeDepthParity(PolygonizeDirectedEdge de)
  {
    
  }
  
}
