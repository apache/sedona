


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
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.util.*;
import com.vividsolutions.jts.geomgraph.Label;
import com.vividsolutions.jts.geomgraph.Edge;

/**
 * Models the end of an edge incident on a node.
 * EdgeEnds have a direction
 * determined by the direction of the ray from the initial
 * point to the next point.
 * EdgeEnds are comparable under the ordering
 * "a has a greater angle with the x-axis than b".
 * This ordering is used to sort EdgeEnds around a node.
 * @version 1.7
 */
public class EdgeEnd
  implements Comparable
{
  protected Edge edge;  // the parent edge of this edge end
  protected Label label;

  private Node node;          // the node this edge end originates at
  private Coordinate p0, p1;  // points of initial line segment
  private double dx, dy;      // the direction vector for this edge from its starting point
  private int quadrant;

  protected EdgeEnd(Edge edge)
  {
    this.edge = edge;
  }
  public EdgeEnd(Edge edge, Coordinate p0, Coordinate p1) {
    this(edge, p0, p1, null);
  }
  public EdgeEnd(Edge edge, Coordinate p0, Coordinate p1, Label label) {
    this(edge);
    init(p0, p1);
    this.label = label;
  }

  protected void init(Coordinate p0, Coordinate p1)
  {
    this.p0 = p0;
    this.p1 = p1;
    dx = p1.x - p0.x;
    dy = p1.y - p0.y;
    quadrant = Quadrant.quadrant(dx, dy);
    Assert.isTrue(! (dx == 0 && dy == 0), "EdgeEnd with identical endpoints found");
  }

  public Edge getEdge() { return edge; }
  public Label getLabel() { return label; }
  public Coordinate getCoordinate() { return p0; }
  public Coordinate getDirectedCoordinate() { return p1; }
  public int getQuadrant() { return quadrant; }
  public double getDx() { return dx; }
  public double getDy() { return dy; }

  public void setNode(Node node) { this.node = node; }
  public Node getNode() { return node; }

  public int compareTo(Object obj)
  {
      EdgeEnd e = (EdgeEnd) obj;
      return compareDirection(e);
  }
  /**
   * Implements the total order relation:
   * <p>
   *    a has a greater angle with the positive x-axis than b
   * <p>
   * Using the obvious algorithm of simply computing the angle is not robust,
   * since the angle calculation is obviously susceptible to roundoff.
   * A robust algorithm is:
   * - first compare the quadrant.  If the quadrants
   * are different, it it trivial to determine which vector is "greater".
   * - if the vectors lie in the same quadrant, the computeOrientation function
   * can be used to decide the relative orientation of the vectors.
   */
  public int compareDirection(EdgeEnd e)
  {
    if (dx == e.dx && dy == e.dy)
      return 0;
    // if the rays are in different quadrants, determining the ordering is trivial
    if (quadrant > e.quadrant) return 1;
    if (quadrant < e.quadrant) return -1;
    // vectors are in the same quadrant - check relative orientation of direction vectors
    // this is > e if it is CCW of e
    return CGAlgorithms.computeOrientation(e.p0, e.p1, p1);
  }

  public void computeLabel(BoundaryNodeRule boundaryNodeRule)
  {
    // subclasses should override this if they are using labels
  }
  public void print(PrintStream out)
  {
    double angle = Math.atan2(dy, dx);
    String className = getClass().getName();
    int lastDotPos = className.lastIndexOf('.');
    String name = className.substring(lastDotPos + 1);
    out.print("  " + name + ": " + p0 + " - " + p1 + " " + quadrant + ":" + angle + "   " + label);
  }
  public String toString()
  {
    double angle = Math.atan2(dy, dx);
    String className = getClass().getName();
    int lastDotPos = className.lastIndexOf('.');
    String name = className.substring(lastDotPos + 1);
    return "  " + name + ": " + p0 + " - " + p1 + " " + quadrant + ":" + angle + "   " + label;
  }
}
