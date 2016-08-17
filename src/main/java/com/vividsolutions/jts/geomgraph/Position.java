


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

/**
 * A Position indicates the position of a Location relative to a graph component
 * (Node, Edge, or Area).
 * @version 1.7
 */
public class Position {

  /** An indicator that a Location is <i>on</i> a GraphComponent */
  public static final int ON      = 0;
  /** An indicator that a Location is to the <i>left</i> of a GraphComponent */  
  public static final int LEFT    = 1;
  /** An indicator that a Location is to the <i>right</i> of a GraphComponent */  
  public static final int RIGHT   = 2;
  /**
   * Returns LEFT if the position is RIGHT, RIGHT if the position is LEFT, or the position
   * otherwise.
   */
  public static final int opposite(int position)
  {
    if (position == LEFT) return RIGHT;
    if (position == RIGHT) return LEFT;
    return position;
  }
}
