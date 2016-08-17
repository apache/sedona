
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
package com.vividsolutions.jts.index.quadtree;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

/**
 * A Key is a unique identifier for a node in a quadtree.
 * It contains a lower-left point and a level number. The level number
 * is the power of two for the size of the node envelope
 *
 * @version 1.7
 */
public class Key {

  public static int computeQuadLevel(Envelope env)
  {
    double dx = env.getWidth();
    double dy = env.getHeight();
    double dMax = dx > dy ? dx : dy;
    int level = DoubleBits.exponent(dMax) + 1;
    return level;
  }

  // the fields which make up the key
  private Coordinate pt = new Coordinate();
  private int level = 0;
  // auxiliary data which is derived from the key for use in computation
  private Envelope env = null;

  public Key(Envelope itemEnv)
  {
    computeKey(itemEnv);
  }

  public Coordinate getPoint() { return pt; }
  public int getLevel() { return level; }
  public Envelope getEnvelope() { return env; }

  public Coordinate getCentre()
  {
    return new Coordinate(
      (env.getMinX() + env.getMaxX()) / 2,
      (env.getMinY() + env.getMaxY()) / 2
      );
  }
  /**
   * return a square envelope containing the argument envelope,
   * whose extent is a power of two and which is based at a power of 2
   */
  public void computeKey(Envelope itemEnv)
  {
    level = computeQuadLevel(itemEnv);
    env = new Envelope();
    computeKey(level, itemEnv);
    // MD - would be nice to have a non-iterative form of this algorithm
    while (! env.contains(itemEnv)) {
      level += 1;
      computeKey(level, itemEnv);
    }
  }

  private void computeKey(int level, Envelope itemEnv)
  {
    double quadSize = DoubleBits.powerOf2(level);
    pt.x = Math.floor(itemEnv.getMinX() / quadSize) * quadSize;
    pt.y = Math.floor(itemEnv.getMinY() / quadSize) * quadSize;
    env.init(pt.x, pt.x + quadSize, pt.y, pt.y + quadSize);
  }
}
