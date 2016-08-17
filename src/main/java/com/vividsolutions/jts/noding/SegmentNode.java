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
package com.vividsolutions.jts.noding;

import java.io.PrintStream;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Represents an intersection point between two {@link SegmentString}s.
 *
 * @version 1.7
 */
public class SegmentNode
    implements Comparable
{
  private final NodedSegmentString segString;
  public final Coordinate coord;   // the point of intersection
  public final int segmentIndex;   // the index of the containing line segment in the parent edge
  private final int segmentOctant;
  private final boolean isInterior;

  public SegmentNode(NodedSegmentString segString, Coordinate coord, int segmentIndex, int segmentOctant) {
    this.segString = segString;
    this.coord = new Coordinate(coord);
    this.segmentIndex = segmentIndex;
    this.segmentOctant = segmentOctant;
    isInterior = ! coord.equals2D(segString.getCoordinate(segmentIndex));
  }

  /**
   * Gets the {@link Coordinate} giving the location of this node.
   * 
   * @return the coordinate of the node
   */
  public Coordinate getCoordinate() 
  {
    return coord;
  }
  
  public boolean isInterior() { return isInterior; }

  public boolean isEndPoint(int maxSegmentIndex)
  {
    if (segmentIndex == 0 && ! isInterior) return true;
    if (segmentIndex == maxSegmentIndex) return true;
    return false;
  }

  /**
   * @return -1 this SegmentNode is located before the argument location;
   * 0 this SegmentNode is at the argument location;
   * 1 this SegmentNode is located after the argument location
   */
  public int compareTo(Object obj)
  {
    SegmentNode other = (SegmentNode) obj;

    if (segmentIndex < other.segmentIndex) return -1;
    if (segmentIndex > other.segmentIndex) return 1;

    if (coord.equals2D(other.coord)) return 0;

    return SegmentPointComparator.compare(segmentOctant, coord, other.coord);
    //return segment.compareNodePosition(this, other);
  }

  public void print(PrintStream out)
  {
    out.print(coord);
    out.print(" seg # = " + segmentIndex);
  }
}
