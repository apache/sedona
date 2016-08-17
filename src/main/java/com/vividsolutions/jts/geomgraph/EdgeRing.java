


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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.*;
import com.vividsolutions.jts.io.*;
import com.vividsolutions.jts.util.*;


/**
 * @version 1.7
 */
public abstract class EdgeRing {

  protected DirectedEdge startDe; // the directed edge which starts the list of edges for this EdgeRing
  private int maxNodeDegree = -1;
  private List edges = new ArrayList(); // the DirectedEdges making up this EdgeRing
  private List pts = new ArrayList();
  private Label label = new Label(Location.NONE); // label stores the locations of each geometry on the face surrounded by this ring
  private LinearRing ring;  // the ring created for this EdgeRing
  private boolean isHole;
  private EdgeRing shell;   // if non-null, the ring is a hole and this EdgeRing is its containing shell
  private ArrayList holes = new ArrayList(); // a list of EdgeRings which are holes in this EdgeRing

  protected GeometryFactory geometryFactory;

  public EdgeRing(DirectedEdge start, GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
    computePoints(start);
    computeRing();
  }

  public boolean isIsolated()
  {
    return (label.getGeometryCount() == 1);
  }
  public boolean isHole()
  {
    //computePoints();
    return isHole;
  }

  public Coordinate getCoordinate(int i) { return (Coordinate) pts.get(i);  }
  public LinearRing getLinearRing() { return ring; }
  public Label getLabel() { return label; }
  public boolean isShell() { return shell == null; }
  public EdgeRing getShell() { return shell; }
  public void setShell(EdgeRing shell)
  {
    this.shell = shell;
    if (shell != null) shell.addHole(this);
  }
  public void addHole(EdgeRing ring) { holes.add(ring); }

  public Polygon toPolygon(GeometryFactory geometryFactory)
  {
    LinearRing[] holeLR = new LinearRing[holes.size()];
    for (int i = 0; i < holes.size(); i++) {
      holeLR[i] = ((EdgeRing) holes.get(i)).getLinearRing();
    }
    Polygon poly = geometryFactory.createPolygon(getLinearRing(), holeLR);
    return poly;
  }
  /**
   * Compute a LinearRing from the point list previously collected.
   * Test if the ring is a hole (i.e. if it is CCW) and set the hole flag
   * accordingly.
   */
  public void computeRing()
  {
    if (ring != null) return;   // don't compute more than once
    Coordinate[] coord = new Coordinate[pts.size()];
    for (int i = 0; i < pts.size(); i++) {
      coord[i] = (Coordinate) pts.get(i);
    }
    ring = geometryFactory.createLinearRing(coord);
    isHole = CGAlgorithms.isCCW(ring.getCoordinates());
//Debug.println( (isHole ? "hole - " : "shell - ") + WKTWriter.toLineString(new CoordinateArraySequence(ring.getCoordinates())));
  }
  abstract public DirectedEdge getNext(DirectedEdge de);
  abstract public void setEdgeRing(DirectedEdge de, EdgeRing er);

  /**
   * Returns the list of DirectedEdges that make up this EdgeRing
   */
  public List getEdges() { return edges; }

  /**
   * Collect all the points from the DirectedEdges of this ring into a contiguous list
   */
  protected void computePoints(DirectedEdge start)
  {
//System.out.println("buildRing");
    startDe = start;
    DirectedEdge de = start;
    boolean isFirstEdge = true;
    do {
//      Assert.isTrue(de != null, "found null Directed Edge");
      if (de == null)
        throw new TopologyException("Found null DirectedEdge");
      if (de.getEdgeRing() == this)
        throw new TopologyException("Directed Edge visited twice during ring-building at " + de.getCoordinate());

      edges.add(de);
//Debug.println(de);
//Debug.println(de.getEdge());
      Label label = de.getLabel();
      Assert.isTrue(label.isArea());
      mergeLabel(label);
      addPoints(de.getEdge(), de.isForward(), isFirstEdge);
      isFirstEdge = false;
      setEdgeRing(de, this);
      de = getNext(de);
    } while (de != startDe);
  }

  public int getMaxNodeDegree()
  {
    if (maxNodeDegree < 0) computeMaxNodeDegree();
    return maxNodeDegree;
  }

  private void computeMaxNodeDegree()
  {
    maxNodeDegree = 0;
    DirectedEdge de = startDe;
    do {
      Node node = de.getNode();
      int degree = ((DirectedEdgeStar) node.getEdges()).getOutgoingDegree(this);
      if (degree > maxNodeDegree) maxNodeDegree = degree;
      de = getNext(de);
    } while (de != startDe);
    maxNodeDegree *= 2;
  }


  public void setInResult()
  {
    DirectedEdge de = startDe;
    do {
      de.getEdge().setInResult(true);
      de = de.getNext();
    } while (de != startDe);
  }

  protected void mergeLabel(Label deLabel)
  {
    mergeLabel(deLabel, 0);
    mergeLabel(deLabel, 1);
  }
  /**
   * Merge the RHS label from a DirectedEdge into the label for this EdgeRing.
   * The DirectedEdge label may be null.  This is acceptable - it results
   * from a node which is NOT an intersection node between the Geometries
   * (e.g. the end node of a LinearRing).  In this case the DirectedEdge label
   * does not contribute any information to the overall labelling, and is simply skipped.
   */
  protected void mergeLabel(Label deLabel, int geomIndex)
  {
    int loc = deLabel.getLocation(geomIndex, Position.RIGHT);
    // no information to be had from this label
    if (loc == Location.NONE) return;
    // if there is no current RHS value, set it
    if (label.getLocation(geomIndex) == Location.NONE) {
      label.setLocation(geomIndex, loc);
      return;
    }
  }
  protected void addPoints(Edge edge, boolean isForward, boolean isFirstEdge)
  {
    Coordinate[] edgePts = edge.getCoordinates();
    if (isForward) {
      int startIndex = 1;
      if (isFirstEdge) startIndex = 0;
      for (int i = startIndex; i < edgePts.length; i++) {
        pts.add(edgePts[i]);
      }
    }
    else { // is backward
      int startIndex = edgePts.length - 2;
      if (isFirstEdge) startIndex = edgePts.length - 1;
      for (int i = startIndex; i >= 0; i--) {
        pts.add(edgePts[i]);
      }
    }
  }

  /**
   * This method will cause the ring to be computed.
   * It will also check any holes, if they have been assigned.
   */
  public boolean containsPoint(Coordinate p)
  {
    LinearRing shell = getLinearRing();
    Envelope env = shell.getEnvelopeInternal();
    if (! env.contains(p)) return false;
    if (! CGAlgorithms.isPointInRing(p, shell.getCoordinates()) ) return false;

    for (Iterator i = holes.iterator(); i.hasNext(); ) {
      EdgeRing hole = (EdgeRing) i.next();
      if (hole.containsPoint(p) )
        return false;
    }
    return true;
  }

}
