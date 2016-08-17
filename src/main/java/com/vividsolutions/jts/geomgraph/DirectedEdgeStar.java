


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
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.*;

/**
 * A DirectedEdgeStar is an ordered list of <b>outgoing</b> DirectedEdges around a node.
 * It supports labelling the edges as well as linking the edges to form both
 * MaximalEdgeRings and MinimalEdgeRings.
 *
 * @version 1.7
 */
public class DirectedEdgeStar
  extends EdgeEndStar
{

  /**
   * A list of all outgoing edges in the result, in CCW order
   */
  private List resultAreaEdgeList;
  private Label label;

  public DirectedEdgeStar() {
  }
  /**
   * Insert a directed edge in the list
   */
  public void insert(EdgeEnd ee)
  {
    DirectedEdge de = (DirectedEdge) ee;
    insertEdgeEnd(de, de);
  }

  public Label getLabel() { return label; }

  public int getOutgoingDegree()
  {
    int degree = 0;
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      if (de.isInResult()) degree++;
    }
    return degree;
  }
  public int getOutgoingDegree(EdgeRing er)
  {
    int degree = 0;
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      if (de.getEdgeRing() == er) degree++;
    }
    return degree;
  }

  public DirectedEdge getRightmostEdge()
  {
    List edges = getEdges();
    int size = edges.size();
    if (size < 1) return null;
    DirectedEdge de0 = (DirectedEdge) edges.get(0);
    if (size == 1) return de0;
    DirectedEdge deLast = (DirectedEdge) edges.get(size - 1);

    int quad0 = de0.getQuadrant();
    int quad1 = deLast.getQuadrant();
    if (Quadrant.isNorthern(quad0) && Quadrant.isNorthern(quad1))
      return de0;
    else if (! Quadrant.isNorthern(quad0) && ! Quadrant.isNorthern(quad1))
      return deLast;
    else {
      // edges are in different hemispheres - make sure we return one that is non-horizontal
      //Assert.isTrue(de0.getDy() != 0, "should never return horizontal edge!");
      DirectedEdge nonHorizontalEdge = null;
      if (de0.getDy() != 0)
        return de0;
      else if (deLast.getDy() != 0)
        return deLast;
    }
    Assert.shouldNeverReachHere("found two horizontal edges incident on node");
    return null;

  }
  /**
   * Compute the labelling for all dirEdges in this star, as well
   * as the overall labelling
   */
  public void computeLabelling(GeometryGraph[] geom)
  {
//Debug.print(this);
    super.computeLabelling(geom);

    // determine the overall labelling for this DirectedEdgeStar
    // (i.e. for the node it is based at)
    label = new Label(Location.NONE);
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd ee = (EdgeEnd) it.next();
      Edge e = ee.getEdge();
      Label eLabel = e.getLabel();
      for (int i = 0; i < 2; i++) {
        int eLoc = eLabel.getLocation(i);
        if (eLoc == Location.INTERIOR || eLoc == Location.BOUNDARY)
          label.setLocation(i, Location.INTERIOR);
      }
    }
//Debug.print(this);
  }

  /**
   * For each dirEdge in the star,
   * merge the label from the sym dirEdge into the label
   */
  public void mergeSymLabels()
  {
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      Label label = de.getLabel();
      label.merge(de.getSym().getLabel());
    }
  }

  /**
   * Update incomplete dirEdge labels from the labelling for the node
   */
  public void updateLabelling(Label nodeLabel)
  {
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      Label label = de.getLabel();
      label.setAllLocationsIfNull(0, nodeLabel.getLocation(0));
      label.setAllLocationsIfNull(1, nodeLabel.getLocation(1));
    }
  }

  private List getResultAreaEdges()
  {
//print(System.out);
    if (resultAreaEdgeList != null) return resultAreaEdgeList;
    resultAreaEdgeList = new ArrayList();
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      if (de.isInResult() || de.getSym().isInResult() )
        resultAreaEdgeList.add(de);
    }
    return resultAreaEdgeList;
  }

  private final int SCANNING_FOR_INCOMING = 1;
  private final int LINKING_TO_OUTGOING = 2;
  /**
   * Traverse the star of DirectedEdges, linking the included edges together.
   * To link two dirEdges, the <next> pointer for an incoming dirEdge
   * is set to the next outgoing edge.
   * <p>
   * DirEdges are only linked if:
   * <ul>
   * <li>they belong to an area (i.e. they have sides)
   * <li>they are marked as being in the result
   * </ul>
   * <p>
   * Edges are linked in CCW order (the order they are stored).
   * This means that rings have their face on the Right
   * (in other words,
   * the topological location of the face is given by the RHS label of the DirectedEdge)
   * <p>
   * PRECONDITION: No pair of dirEdges are both marked as being in the result
   */
  public void linkResultDirectedEdges()
  {
    // make sure edges are copied to resultAreaEdges list
    getResultAreaEdges();
    // find first area edge (if any) to start linking at
    DirectedEdge firstOut = null;
    DirectedEdge incoming = null;
    int state = SCANNING_FOR_INCOMING;
    // link edges in CCW order
    for (int i = 0; i < resultAreaEdgeList.size(); i++) {
      DirectedEdge nextOut = (DirectedEdge) resultAreaEdgeList.get(i);
      DirectedEdge nextIn = nextOut.getSym();

      // skip de's that we're not interested in
      if (! nextOut.getLabel().isArea()) continue;

      // record first outgoing edge, in order to link the last incoming edge
      if (firstOut == null && nextOut.isInResult()) firstOut = nextOut;
      // assert: sym.isInResult() == false, since pairs of dirEdges should have been removed already

      switch (state) {
      case SCANNING_FOR_INCOMING:
        if (! nextIn.isInResult()) continue;
        incoming = nextIn;
        state = LINKING_TO_OUTGOING;
        break;
      case LINKING_TO_OUTGOING:
        if (! nextOut.isInResult()) continue;
        incoming.setNext(nextOut);
        state = SCANNING_FOR_INCOMING;
        break;
      }
    }
//Debug.print(this);
    if (state == LINKING_TO_OUTGOING) {
//Debug.print(firstOut == null, this);
      if (firstOut == null)
        throw new TopologyException("no outgoing dirEdge found", getCoordinate());
      //Assert.isTrue(firstOut != null, "no outgoing dirEdge found (at " + getCoordinate() );
      Assert.isTrue(firstOut.isInResult(), "unable to link last incoming dirEdge");
      incoming.setNext(firstOut);
    }
  }
  public void linkMinimalDirectedEdges(EdgeRing er)
  {
    // find first area edge (if any) to start linking at
    DirectedEdge firstOut = null;
    DirectedEdge incoming = null;
    int state = SCANNING_FOR_INCOMING;
    // link edges in CW order
    for (int i = resultAreaEdgeList.size() - 1; i >= 0; i--) {
      DirectedEdge nextOut = (DirectedEdge) resultAreaEdgeList.get(i);
      DirectedEdge nextIn = nextOut.getSym();

      // record first outgoing edge, in order to link the last incoming edge
      if (firstOut == null && nextOut.getEdgeRing() == er) firstOut = nextOut;

      switch (state) {
      case SCANNING_FOR_INCOMING:
        if (nextIn.getEdgeRing() != er) continue;
        incoming = nextIn;
        state = LINKING_TO_OUTGOING;
        break;
      case LINKING_TO_OUTGOING:
        if (nextOut.getEdgeRing() != er) continue;
        incoming.setNextMin(nextOut);
        state = SCANNING_FOR_INCOMING;
        break;
      }
    }
//print(System.out);
    if (state == LINKING_TO_OUTGOING) {
      Assert.isTrue(firstOut != null, "found null for first outgoing dirEdge");
      Assert.isTrue(firstOut.getEdgeRing() == er, "unable to link last incoming dirEdge");
      incoming.setNextMin(firstOut);
    }
  }
  public void linkAllDirectedEdges()
  {
    getEdges();
    // find first area edge (if any) to start linking at
    DirectedEdge prevOut = null;
    DirectedEdge firstIn = null;
    // link edges in CW order
    for (int i = edgeList.size() - 1; i >= 0; i--) {
      DirectedEdge nextOut = (DirectedEdge) edgeList.get(i);
      DirectedEdge nextIn = nextOut.getSym();
      if (firstIn == null) firstIn = nextIn;
      if (prevOut != null) nextIn.setNext(prevOut);
      // record outgoing edge, in order to link the last incoming edge
      prevOut = nextOut;
    }
    firstIn.setNext(prevOut);
//Debug.print(this);
  }

  /**
   * Traverse the star of edges, maintaing the current location in the result
   * area at this node (if any).
   * If any L edges are found in the interior of the result, mark them as covered.
   */
  public void findCoveredLineEdges()
  {
//Debug.print("findCoveredLineEdges");
//Debug.print(this);
    // Since edges are stored in CCW order around the node,
    // as we move around the ring we move from the right to the left side of the edge

    /**
     * Find first DirectedEdge of result area (if any).
     * The interior of the result is on the RHS of the edge,
     * so the start location will be:
     * - INTERIOR if the edge is outgoing
     * - EXTERIOR if the edge is incoming
     */
    int startLoc = Location.NONE ;
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge nextOut  = (DirectedEdge) it.next();
      DirectedEdge nextIn   = nextOut.getSym();
      if (! nextOut.isLineEdge()) {
        if (nextOut.isInResult()) {
          startLoc = Location.INTERIOR;
          break;
        }
        if (nextIn.isInResult()) {
          startLoc = Location.EXTERIOR;
          break;
        }
      }
    }
    // no A edges found, so can't determine if L edges are covered or not
    if (startLoc == Location.NONE) return;

    /**
     * move around ring, keeping track of the current location
     * (Interior or Exterior) for the result area.
     * If L edges are found, mark them as covered if they are in the interior
     */
    int currLoc = startLoc;
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge nextOut  = (DirectedEdge) it.next();
      DirectedEdge nextIn   = nextOut.getSym();
      if (nextOut.isLineEdge()) {
        nextOut.getEdge().setCovered(currLoc == Location.INTERIOR);
//Debug.println(nextOut);
      }
      else {  // edge is an Area edge
        if (nextOut.isInResult())
          currLoc = Location.EXTERIOR;
        if (nextIn.isInResult())
          currLoc = Location.INTERIOR;
      }
    }
  }

  public void computeDepths(DirectedEdge de)
  {
    int edgeIndex = findIndex(de);
    Label label = de.getLabel();
    int startDepth = de.getDepth(Position.LEFT);
    int targetLastDepth = de.getDepth(Position.RIGHT);
    // compute the depths from this edge up to the end of the edge array
    int nextDepth = computeDepths(edgeIndex + 1, edgeList.size(), startDepth);
    // compute the depths for the initial part of the array
    int lastDepth = computeDepths(0, edgeIndex, nextDepth);
//Debug.print(lastDepth != targetLastDepth, this);
//Debug.print(lastDepth != targetLastDepth, "mismatch: " + lastDepth + " / " + targetLastDepth);
    if (lastDepth != targetLastDepth)
      throw new TopologyException("depth mismatch at " + de.getCoordinate());
    //Assert.isTrue(lastDepth == targetLastDepth, "depth mismatch at " + de.getCoordinate());
  }

  /**
   * Compute the DirectedEdge depths for a subsequence of the edge array.
   *
   * @return the last depth assigned (from the R side of the last edge visited)
   */
  private int computeDepths(int startIndex, int endIndex, int startDepth)
  {
    int currDepth = startDepth;
    for (int i = startIndex; i < endIndex ; i++) {
      DirectedEdge nextDe = (DirectedEdge) edgeList.get(i);
      Label label = nextDe.getLabel();
      nextDe.setEdgeDepths(Position.RIGHT, currDepth);
      currDepth = nextDe.getDepth(Position.LEFT);
    }
    return currDepth;
  }

  public void print(PrintStream out)
  {
    System.out.println("DirectedEdgeStar: " + getCoordinate());
    for (Iterator it = iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      out.print("out ");
      de.print(out);
      out.println();
      out.print("in ");
      de.getSym().print(out);
      out.println();
    }
  }
}
