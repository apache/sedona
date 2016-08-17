

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
package com.vividsolutions.jts.operation.valid;

import java.util.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.operation.overlay.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.*;

/**
 * This class tests that the interior of an area {@link Geometry}
 * ( {@link Polygon}  or {@link MultiPolygon} )
 * is connected.
 * This can happen if:
 * <ul>
 * <li>a shell self-intersects
 * <li>one or more holes form a connected chain touching a shell at two different points
 * <li>one or more holes form a ring around a subset of the interior
 * </ul>
 * If a disconnected situation is found the location of the problem is recorded.
 *
 * @version 1.7
 */
public class ConnectedInteriorTester {

  public static Coordinate findDifferentPoint(Coordinate[] coord, Coordinate pt)
  {
    for (int i = 0; i < coord.length; i++) {
      if (! coord[i].equals(pt))
        return coord[i];
    }
    return null;
  }

  private GeometryFactory geometryFactory = new GeometryFactory();

  private GeometryGraph geomGraph;
  // save a coordinate for any disconnected interior found
  // the coordinate will be somewhere on the ring surrounding the disconnected interior
  private Coordinate disconnectedRingcoord;

  public ConnectedInteriorTester(GeometryGraph geomGraph)
  {
    this.geomGraph = geomGraph;
  }

  public Coordinate getCoordinate() { return disconnectedRingcoord; }

  public boolean isInteriorsConnected()
  {
    // node the edges, in case holes touch the shell
    List splitEdges = new ArrayList();
    geomGraph.computeSplitEdges(splitEdges);

    // form the edges into rings
    PlanarGraph graph = new PlanarGraph(new OverlayNodeFactory());
    graph.addEdges(splitEdges);
    setInteriorEdgesInResult(graph);
    graph.linkResultDirectedEdges();
    List edgeRings = buildEdgeRings(graph.getEdgeEnds());

    /**
     * Mark all the edges for the edgeRings corresponding to the shells
     * of the input polygons.  Note only ONE ring gets marked for each shell.
     */
    visitShellInteriors(geomGraph.getGeometry(), graph);

    /**
     * If there are any unvisited shell edges
     * (i.e. a ring which is not a hole and which has the interior
     * of the parent area on the RHS)
     * this means that one or more holes must have split the interior of the
     * polygon into at least two pieces.  The polygon is thus invalid.
     */
    return ! hasUnvisitedShellEdge(edgeRings);
  }

  private void setInteriorEdgesInResult(PlanarGraph graph)
  {
    for (Iterator it = graph.getEdgeEnds().iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      if (de.getLabel().getLocation(0, Position.RIGHT) == Location.INTERIOR) {
        de.setInResult(true);
      }
    }
  }

  /**
   * Form DirectedEdges in graph into Minimal EdgeRings.
   * (Minimal Edgerings must be used, because only they are guaranteed to provide
   * a correct isHole computation)
   */
  private List buildEdgeRings(Collection dirEdges)
  {
    List edgeRings = new ArrayList();
    for (Iterator it = dirEdges.iterator(); it.hasNext(); ) {
      DirectedEdge de = (DirectedEdge) it.next();
      // if this edge has not yet been processed
      if (de.isInResult()
         && de.getEdgeRing() == null) {
        MaximalEdgeRing er = new MaximalEdgeRing(de, geometryFactory);

        er.linkDirectedEdgesForMinimalEdgeRings();
        List minEdgeRings = er.buildMinimalRings();
        edgeRings.addAll(minEdgeRings);
      }
    }
    return edgeRings;
  }

  /**
   * Mark all the edges for the edgeRings corresponding to the shells
   * of the input polygons.
   * Only ONE ring gets marked for each shell - if there are others which remain unmarked
   * this indicates a disconnected interior.
   */
  private void visitShellInteriors(Geometry g, PlanarGraph graph)
  {
    if (g instanceof Polygon) {
      Polygon p = (Polygon) g;
      visitInteriorRing(p.getExteriorRing(), graph);
    }
    if (g instanceof MultiPolygon) {
      MultiPolygon mp = (MultiPolygon) g;
      for (int i = 0; i < mp.getNumGeometries(); i++) {
        Polygon p = (Polygon) mp.getGeometryN(i);
        visitInteriorRing(p.getExteriorRing(), graph);
      }
    }
  }

  private void visitInteriorRing(LineString ring, PlanarGraph graph)
  {
    Coordinate[] pts = ring.getCoordinates();
    Coordinate pt0 = pts[0];
    /**
     * Find first point in coord list different to initial point.
     * Need special check since the first point may be repeated.
     */
    Coordinate pt1 = findDifferentPoint(pts, pt0);
    Edge e = graph.findEdgeInSameDirection(pt0, pt1);
    DirectedEdge de = (DirectedEdge) graph.findEdgeEnd(e);
    DirectedEdge intDe = null;
    if (de.getLabel().getLocation(0, Position.RIGHT) == Location.INTERIOR) {
      intDe = de;
    }
    else if (de.getSym().getLabel().getLocation(0, Position.RIGHT) == Location.INTERIOR) {
      intDe = de.getSym();
    }
    Assert.isTrue(intDe != null, "unable to find dirEdge with Interior on RHS");

    visitLinkedDirectedEdges(intDe);
  }

  protected void visitLinkedDirectedEdges(DirectedEdge start)
  {
    DirectedEdge startDe = start;
    DirectedEdge de = start;
    do {
      Assert.isTrue(de != null, "found null Directed Edge");
      de.setVisited(true);
      de = de.getNext();
    } while (de != startDe);
  }

  /**
   * Check if any shell ring has an unvisited edge.
   * A shell ring is a ring which is not a hole and which has the interior
   * of the parent area on the RHS.
   * (Note that there may be non-hole rings with the interior on the LHS,
   * since the interior of holes will also be polygonized into CW rings
   * by the linkAllDirectedEdges() step)
   *
   * @return true if there is an unvisited edge in a non-hole ring
   */
  private boolean hasUnvisitedShellEdge(List edgeRings)
  {
    for (int i = 0; i < edgeRings.size(); i++) {
      EdgeRing er = (EdgeRing) edgeRings.get(i);
      // don't check hole rings
      if (er.isHole())
        continue;
      List edges = er.getEdges();
      DirectedEdge de = (DirectedEdge) edges.get(0);
      // don't check CW rings which are holes
      // (MD - this check may now be irrelevant)
      if (de.getLabel().getLocation(0, Position.RIGHT) != Location.INTERIOR) continue;

      /**
       * the edgeRing is CW ring which surrounds the INT of the area, so check all
       * edges have been visited.  If any are unvisited, this is a disconnected part of the interior
       */
      for (int j = 0; j < edges.size(); j++) {
        de = (DirectedEdge) edges.get(j);
//Debug.print("visted? "); Debug.println(de);
        if (! de.isVisited()) {
//Debug.print("not visited "); Debug.println(de);
          disconnectedRingcoord = de.getCoordinate();
          return true;
        }
      }
    }
    return false;
  }
}
