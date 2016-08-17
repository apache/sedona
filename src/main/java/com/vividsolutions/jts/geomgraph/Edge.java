


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
import java.util.Iterator;
import com.vividsolutions.jts.algorithm.LineIntersector;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.*;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.geomgraph.index.*;


/**
 * @version 1.7
 */
public class Edge
  extends GraphComponent
{

  /**
   * Updates an IM from the label for an edge.
   * Handles edges from both L and A geometries.
   */
  public static void updateIM(Label label, IntersectionMatrix im)
  {
    im.setAtLeastIfValid(label.getLocation(0, Position.ON), label.getLocation(1, Position.ON), 1);
    if (label.isArea()) {
      im.setAtLeastIfValid(label.getLocation(0, Position.LEFT),  label.getLocation(1, Position.LEFT),   2);
      im.setAtLeastIfValid(label.getLocation(0, Position.RIGHT), label.getLocation(1, Position.RIGHT),  2);
    }
  }

  Coordinate[] pts;
  private Envelope env;
  EdgeIntersectionList eiList = new EdgeIntersectionList(this);
  private String name;
  private MonotoneChainEdge mce;
  private boolean isIsolated = true;
  private Depth depth = new Depth();
  private int depthDelta = 0;   // the change in area depth from the R to L side of this edge

  public Edge(Coordinate[] pts, Label label)
  {
    this.pts = pts;
    this.label = label;
  }
  public Edge(Coordinate[] pts)
  {
    this(pts, null);
  }

  public int getNumPoints() { return pts.length; }
  public void setName(String name) { this.name = name; }
  public Coordinate[] getCoordinates()  {    return pts;  }
  public Coordinate getCoordinate(int i)
  {
    return pts[i];
  }
  public Coordinate getCoordinate()
  {
    if (pts.length > 0) return pts[0];
    return null;
  }
  public Envelope getEnvelope()
  {
    // compute envelope lazily
    if (env == null) {
      env = new Envelope();
      for (int i = 0; i < pts.length; i++) {
        env.expandToInclude(pts[i]);
      }
    }
    return env;
  }

  public Depth getDepth() { return depth; }

  /**
   * The depthDelta is the change in depth as an edge is crossed from R to L
   * @return the change in depth as the edge is crossed from R to L
   */
  public int getDepthDelta()  { return depthDelta;  }
  public void setDepthDelta(int depthDelta)  { this.depthDelta = depthDelta;  }

  public int getMaximumSegmentIndex()
  {
    return pts.length - 1;
  }
  public EdgeIntersectionList getEdgeIntersectionList() { return eiList; }

  public MonotoneChainEdge getMonotoneChainEdge()
  {
    if (mce == null) mce = new MonotoneChainEdge(this);
    return mce;
  }

  public boolean isClosed()
  {
    return pts[0].equals(pts[pts.length - 1]);
  }
  /**
   * An Edge is collapsed if it is an Area edge and it consists of
   * two segments which are equal and opposite (eg a zero-width V).
   */
  public boolean isCollapsed()
  {
    if (! label.isArea()) return false;
    if (pts.length != 3) return false;
    if (pts[0].equals(pts[2]) ) return true;
    return false;
  }
  public Edge getCollapsedEdge()
  {
    Coordinate newPts[] = new Coordinate[2];
    newPts[0] = pts[0];
    newPts[1] = pts[1];
    Edge newe = new Edge(newPts, Label.toLineLabel(label));
    return newe;
  }

  public void setIsolated(boolean isIsolated)
  {
    this.isIsolated = isIsolated;
  }
  public boolean isIsolated()
  {
    return isIsolated;
  }

  /**
   * Adds EdgeIntersections for one or both
   * intersections found for a segment of an edge to the edge intersection list.
   */
  public void addIntersections(LineIntersector li, int segmentIndex, int geomIndex)
  {
    for (int i = 0; i < li.getIntersectionNum(); i++) {
      addIntersection(li, segmentIndex, geomIndex, i);
    }
  }
  /**
   * Add an EdgeIntersection for intersection intIndex.
   * An intersection that falls exactly on a vertex of the edge is normalized
   * to use the higher of the two possible segmentIndexes
   */
  public void addIntersection(LineIntersector li, int segmentIndex, int geomIndex, int intIndex)
  {
      Coordinate intPt = new Coordinate(li.getIntersection(intIndex));
      int normalizedSegmentIndex = segmentIndex;
      double dist = li.getEdgeDistance(geomIndex, intIndex);
//Debug.println("edge intpt: " + intPt + " dist: " + dist);
      // normalize the intersection point location
      int nextSegIndex = normalizedSegmentIndex + 1;
      if (nextSegIndex < pts.length) {
        Coordinate nextPt = pts[nextSegIndex];
//Debug.println("next pt: " + nextPt);

        // Normalize segment index if intPt falls on vertex
        // The check for point equality is 2D only - Z values are ignored
        if (intPt.equals2D(nextPt)) {
//Debug.println("normalized distance");
            normalizedSegmentIndex = nextSegIndex;
            dist = 0.0;
        }
      }
      /**
      * Add the intersection point to edge intersection list.
      */
      EdgeIntersection ei = eiList.add(intPt, normalizedSegmentIndex, dist);
//ei.print(System.out);

  }

  /**
   * Update the IM with the contribution for this component.
   * A component only contributes if it has a labelling for both parent geometries
   */
  public void computeIM(IntersectionMatrix im)
  {
    updateIM(label, im);
  }

  /**
   * equals is defined to be:
   * <p>
   * e1 equals e2
   * <b>iff</b>
   * the coordinates of e1 are the same or the reverse of the coordinates in e2
   */
  public boolean equals(Object o)
  {
    if (! (o instanceof Edge)) return false;
    Edge e = (Edge) o;

    if (pts.length != e.pts.length) return false;

    boolean isEqualForward = true;
    boolean isEqualReverse = true;
    int iRev = pts.length;
    for (int i = 0; i < pts.length; i++) {
      if (! pts[i].equals2D(e.pts[i])) {
         isEqualForward = false;
      }
      if (! pts[i].equals2D(e.pts[--iRev])) {
         isEqualReverse = false;
      }
      if (! isEqualForward && ! isEqualReverse) return false;
    }
    return true;
  }

  /**
   * @return true if the coordinate sequences of the Edges are identical
   */
  public boolean isPointwiseEqual(Edge e)
  {
    if (pts.length != e.pts.length) return false;

    for (int i = 0; i < pts.length; i++) {
      if (! pts[i].equals2D(e.pts[i])) {
         return false;
      }
    }
    return true;
  }

  public String toString()
  {
    StringBuffer buf = new StringBuffer();
    buf.append("edge " + name + ": ");
    buf.append("LINESTRING (");
    for (int i = 0; i < pts.length; i++) {
      if (i > 0) buf.append(",");
      buf.append(pts[i].x + " " + pts[i].y);
    }
    buf.append(")  " + label + " " + depthDelta);
    return buf.toString();
  }
  public void print(PrintStream out)
  {
    out.print("edge " + name + ": ");
    out.print("LINESTRING (");
    for (int i = 0; i < pts.length; i++) {
      if (i > 0) out.print(",");
      out.print(pts[i].x + " " + pts[i].y);
    }
    out.print(")  " + label + " " + depthDelta);
  }
  public void printReverse(PrintStream out)
  {
    out.print("edge " + name + ": ");
    for (int i = pts.length - 1; i >= 0; i--) {
      out.print(pts[i] + " ");
    }
    out.println("");
  }

}
