

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

import com.vividsolutions.jts.geomgraph.Position;
import com.vividsolutions.jts.geom.Location;

/**
 * A Depth object records the topological depth of the sides
 * of an Edge for up to two Geometries.
 * @version 1.7
 */
public class Depth {

  private final static int NULL_VALUE = -1;

  public static int depthAtLocation(int location)
  {
    if (location == Location.EXTERIOR) return 0;
    if (location == Location.INTERIOR) return 1;
    return NULL_VALUE;
  }

  private int[][] depth = new int[2][3];

  public Depth() {
    // initialize depth array to a sentinel value
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        depth[i][j] = NULL_VALUE;
      }
    }
  }

  public int getDepth(int geomIndex, int posIndex)
  {
    return depth[geomIndex][posIndex];
  }
  public void setDepth(int geomIndex, int posIndex, int depthValue)
  {
    depth[geomIndex][posIndex] = depthValue;
  }
  public int getLocation(int geomIndex, int posIndex)
  {
    if (depth[geomIndex][posIndex] <= 0) return Location.EXTERIOR;
    return Location.INTERIOR;
  }
  public void add(int geomIndex, int posIndex, int location)
  {
    if (location == Location.INTERIOR)
      depth[geomIndex][posIndex]++;
  }
  /**
   * A Depth object is null (has never been initialized) if all depths are null.
   */
  public boolean isNull()
  {
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        if (depth[i][j] != NULL_VALUE)
          return false;
      }
    }
    return true;
  }
  public boolean isNull(int geomIndex)
  {
    return depth[geomIndex][1] == NULL_VALUE;
  }
  public boolean isNull(int geomIndex, int posIndex)
  {
    return depth[geomIndex][posIndex] == NULL_VALUE;
  }
  public void add(Label lbl)
  {
    for (int i = 0; i < 2; i++) {
      for (int j = 1; j < 3; j++) {
        int loc = lbl.getLocation(i, j);
        if (loc == Location.EXTERIOR || loc == Location.INTERIOR) {
          // initialize depth if it is null, otherwise add this location value
          if (isNull(i, j)) {
            depth[i][j] = depthAtLocation(loc);
          }
          else
            depth[i][j] += depthAtLocation(loc);
        }
      }
    }
  }
  public int getDelta(int geomIndex)
  {
    return depth[geomIndex][Position.RIGHT] - depth[geomIndex][Position.LEFT];
  }
  /**
   * Normalize the depths for each geometry, if they are non-null.
   * A normalized depth
   * has depth values in the set { 0, 1 }.
   * Normalizing the depths
   * involves reducing the depths by the same amount so that at least
   * one of them is 0.  If the remaining value is > 0, it is set to 1.
   */
  public void normalize()
  {
    for (int i = 0; i < 2; i++) {
      if (! isNull(i)) {
        int minDepth = depth[i][1];
        if (depth[i][2] < minDepth)
          minDepth = depth[i][2];

        if (minDepth < 0) minDepth = 0;
        for (int j = 1; j < 3; j++) {
          int newValue = 0;
          if (depth[i][j] > minDepth)
            newValue = 1;
          depth[i][j] = newValue;
        }
      }
    }
  }

  public String toString()
  {
    return
        "A: " + depth[0][1] + "," + depth[0][2]
      + " B: " + depth[1][1] + "," + depth[1][2];
  }
}
