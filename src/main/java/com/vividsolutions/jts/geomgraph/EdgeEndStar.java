


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
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.algorithm.locate.SimplePointInAreaLocator;
import com.vividsolutions.jts.util.*;

/**
 * A EdgeEndStar is an ordered list of EdgeEnds around a node.
 * They are maintained in CCW order (starting with the positive x-axis) around the node
 * for efficient lookup and topology building.
 *
 * @version 1.7
 */
abstract public class EdgeEndStar
{

  /**
   * A map which maintains the edges in sorted order around the node
   */
  protected Map edgeMap = new TreeMap();
  /**
   * A list of all outgoing edges in the result, in CCW order
   */
  protected List edgeList;
  /**
   * The location of the point for this star in Geometry i Areas
   */
  private int[] ptInAreaLocation = { Location.NONE, Location.NONE };

  public EdgeEndStar()
  {

  }

  /**
   * Insert a EdgeEnd into this EdgeEndStar
   */
  abstract public void insert(EdgeEnd e);

  /**
   * Insert an EdgeEnd into the map, and clear the edgeList cache,
   * since the list of edges has now changed
   */
  protected void insertEdgeEnd(EdgeEnd e, Object obj)
  {
    edgeMap.put(e, obj);
    edgeList = null;  // edge list has changed - clear the cache
  }

  /**
   * @return the coordinate for the node this star is based at
   */
  public Coordinate getCoordinate()
  {
    Iterator it = iterator();
    if (! it.hasNext()) return null;
    EdgeEnd e = (EdgeEnd) it.next();
    return e.getCoordinate();
  }
  public int getDegree()
  {
    return edgeMap.size();
  }

  /**
   * Iterator access to the ordered list of edges is optimized by
   * copying the map collection to a list.  (This assumes that
   * once an iterator is requested, it is likely that insertion into
   * the map is complete).
   */
  public Iterator iterator()
  {
    return getEdges().iterator();
  }
  public List getEdges()
  {
    if (edgeList == null) {
      edgeList = new ArrayList(edgeMap.values());
    }
    return edgeList;
  }
  public EdgeEnd getNextCW(EdgeEnd ee)
  {
    getEdges();
    int i = edgeList.indexOf(ee);
    int iNextCW = i - 1;
    if (i == 0)
      iNextCW = edgeList.size() - 1;
    return (EdgeEnd) edgeList.get(iNextCW);
  }

  public void computeLabelling(GeometryGraph[] geomGraph)
  {
    computeEdgeEndLabels(geomGraph[0].getBoundaryNodeRule());
    // Propagate side labels  around the edges in the star
    // for each parent Geometry
//Debug.print(this);
    propagateSideLabels(0);
//Debug.print(this);
//Debug.printIfWatch(this);
    propagateSideLabels(1);
//Debug.print(this);
//Debug.printIfWatch(this);

    /**
     * If there are edges that still have null labels for a geometry
     * this must be because there are no area edges for that geometry incident on this node.
     * In this case, to label the edge for that geometry we must test whether the
     * edge is in the interior of the geometry.
     * To do this it suffices to determine whether the node for the edge is in the interior of an area.
     * If so, the edge has location INTERIOR for the geometry.
     * In all other cases (e.g. the node is on a line, on a point, or not on the geometry at all) the edge
     * has the location EXTERIOR for the geometry.
     * <p>
     * Note that the edge cannot be on the BOUNDARY of the geometry, since then
     * there would have been a parallel edge from the Geometry at this node also labelled BOUNDARY
     * and this edge would have been labelled in the previous step.
     * <p>
     * This code causes a problem when dimensional collapses are present, since it may try and
     * determine the location of a node where a dimensional collapse has occurred.
     * The point should be considered to be on the EXTERIOR
     * of the polygon, but locate() will return INTERIOR, since it is passed
     * the original Geometry, not the collapsed version.
     *
     * If there are incident edges which are Line edges labelled BOUNDARY,
     * then they must be edges resulting from dimensional collapses.
     * In this case the other edges can be labelled EXTERIOR for this Geometry.
     *
     * MD 8/11/01 - NOT TRUE!  The collapsed edges may in fact be in the interior of the Geometry,
     * which means the other edges should be labelled INTERIOR for this Geometry.
     * Not sure how solve this...  Possibly labelling needs to be split into several phases:
     * area label propagation, symLabel merging, then finally null label resolution.
     */
    boolean[] hasDimensionalCollapseEdge = { false, false };
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) it.next();
      Label label = e.getLabel();
      for (int geomi = 0; geomi < 2; geomi++) {
        if (label.isLine(geomi) && label.getLocation(geomi) == Location.BOUNDARY)
          hasDimensionalCollapseEdge[geomi] = true;
      }
    }
//Debug.print(this);
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) it.next();
      Label label = e.getLabel();
//Debug.println(e);
      for (int geomi = 0; geomi < 2; geomi++) {
        if (label.isAnyNull(geomi)) {
          int loc = Location.NONE;
          if (hasDimensionalCollapseEdge[geomi]) {
            loc = Location.EXTERIOR;
          }
          else {
            Coordinate p = e.getCoordinate();
            loc = getLocation(geomi, p, geomGraph);
          }
          label.setAllLocationsIfNull(geomi, loc);
        }
      }
//Debug.println(e);
    }
//Debug.print(this);
//Debug.printIfWatch(this);
  }

  private void computeEdgeEndLabels(BoundaryNodeRule boundaryNodeRule)
  {
    // Compute edge label for each EdgeEnd
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd ee = (EdgeEnd) it.next();
      ee.computeLabel(boundaryNodeRule);
    }
  }
  
  private int getLocation(int geomIndex, Coordinate p, GeometryGraph[] geom)
  {
    // compute location only on demand
    if (ptInAreaLocation[geomIndex] == Location.NONE) {
      ptInAreaLocation[geomIndex] = SimplePointInAreaLocator.locate(p, geom[geomIndex].getGeometry());
    }
    return ptInAreaLocation[geomIndex];
  }

  public boolean isAreaLabelsConsistent(GeometryGraph geomGraph)
  {
    computeEdgeEndLabels(geomGraph.getBoundaryNodeRule());
    return checkAreaLabelsConsistent(0);
  }

  private boolean checkAreaLabelsConsistent(int geomIndex)
  {
    // Since edges are stored in CCW order around the node,
    // As we move around the ring we move from the right to the left side of the edge
    List edges = getEdges();
    // if no edges, trivially consistent
    if (edges.size() <= 0)
      return true;
    // initialize startLoc to location of last L side (if any)
    int lastEdgeIndex = edges.size() - 1;
    Label startLabel = ((EdgeEnd) edges.get(lastEdgeIndex)).getLabel();
    int startLoc = startLabel.getLocation(geomIndex, Position.LEFT);
    Assert.isTrue(startLoc != Location.NONE, "Found unlabelled area edge");

    int currLoc = startLoc;
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) it.next();
      Label label = e.getLabel();
      // we assume that we are only checking a area
      Assert.isTrue(label.isArea(geomIndex), "Found non-area edge");
      int leftLoc   = label.getLocation(geomIndex, Position.LEFT);
      int rightLoc  = label.getLocation(geomIndex, Position.RIGHT);
//System.out.println(leftLoc + " " + rightLoc);
//Debug.print(this);
      // check that edge is really a boundary between inside and outside!
      if (leftLoc == rightLoc) {
        return false;
      }
      // check side location conflict
      //Assert.isTrue(rightLoc == currLoc, "side location conflict " + locStr);
      if (rightLoc != currLoc) {
//Debug.print(this);
        return false;
      }
      currLoc = leftLoc;
    }
    return true;
  }
  void propagateSideLabels(int geomIndex)
  {
    // Since edges are stored in CCW order around the node,
    // As we move around the ring we move from the right to the left side of the edge
    int startLoc = Location.NONE ;
    
    // initialize loc to location of last L side (if any)
//System.out.println("finding start location");
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) it.next();
      Label label = e.getLabel();
      if (label.isArea(geomIndex) && label.getLocation(geomIndex, Position.LEFT) != Location.NONE)
        startLoc = label.getLocation(geomIndex, Position.LEFT);
    }
    
    // no labelled sides found, so no labels to propagate
    if (startLoc == Location.NONE) return;

    int currLoc = startLoc;
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) it.next();
      Label label = e.getLabel();
      // set null ON values to be in current location
      if (label.getLocation(geomIndex, Position.ON) == Location.NONE)
          label.setLocation(geomIndex, Position.ON, currLoc);
      // set side labels (if any)
      if (label.isArea(geomIndex)) {
        int leftLoc   = label.getLocation(geomIndex, Position.LEFT);
        int rightLoc  = label.getLocation(geomIndex, Position.RIGHT);
        // if there is a right location, that is the next location to propagate
        if (rightLoc != Location.NONE) {
//Debug.print(rightLoc != currLoc, this);
          if (rightLoc != currLoc)
            throw new TopologyException("side location conflict", e.getCoordinate());
          if (leftLoc == Location.NONE) {
            Assert.shouldNeverReachHere("found single null side (at " + e.getCoordinate() + ")");
          }
          currLoc = leftLoc;
        }
        else {
          /** RHS is null - LHS must be null too.
           *  This must be an edge from the other geometry, which has no location
           *  labelling for this geometry.  This edge must lie wholly inside or outside
           *  the other geometry (which is determined by the current location).
           *  Assign both sides to be the current location.
           */
          Assert.isTrue(label.getLocation(geomIndex, Position.LEFT) == Location.NONE, "found single null side");
          label.setLocation(geomIndex, Position.RIGHT, currLoc);
          label.setLocation(geomIndex, Position.LEFT, currLoc);
        }
      }
    }
  }

  public int findIndex(EdgeEnd eSearch)
  {
    iterator();   // force edgelist to be computed
    for (int i = 0; i < edgeList.size(); i++ ) {
      EdgeEnd e = (EdgeEnd) edgeList.get(i);
      if (e == eSearch) return i;
    }
    return -1;
  }

  public void print(PrintStream out)
  {
    System.out.println("EdgeEndStar:   " + getCoordinate());
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) it.next();
      e.print(out);
    }
  }
  
  public String toString()
  {
    StringBuffer buf = new StringBuffer();
    buf.append("EdgeEndStar:   " + getCoordinate());
    buf.append("\n");
    for (Iterator it = iterator(); it.hasNext(); ) {
      EdgeEnd e = (EdgeEnd) it.next();
      buf.append(e);
      buf.append("\n");
    }
    return buf.toString();
  }
}
