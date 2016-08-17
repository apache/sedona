
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
package com.vividsolutions.jts.index.bintree;



import com.vividsolutions.jts.index.quadtree.DoubleBits;

/**
 * A Key is a unique identifier for a node in a tree.
 * It contains a lower-left point and a level number. The level number
 * is the power of two for the size of the node envelope
 *
 * @version 1.7
 */
public class Key {

  public static int computeLevel(Interval interval)
  {
    double dx = interval.getWidth();
    //int level = BinaryPower.exponent(dx) + 1;
    int level = DoubleBits.exponent(dx) + 1;
    return level;
  }


  // the fields which make up the key
  private double pt = 0.0;
  private int level = 0;
  // auxiliary data which is derived from the key for use in computation
  private Interval interval;

  public Key(Interval interval)
  {
    computeKey(interval);
  }

  public double getPoint() { return pt; }
  public int getLevel() { return level; }
  public Interval getInterval() { return interval; }

  /**
   * return a square envelope containing the argument envelope,
   * whose extent is a power of two and which is based at a power of 2
   */
  public void computeKey(Interval itemInterval)
  {
    level = computeLevel(itemInterval);
    interval = new Interval();
    computeInterval(level, itemInterval);
    // MD - would be nice to have a non-iterative form of this algorithm
    while (! interval.contains(itemInterval)) {
      level += 1;
      computeInterval(level, itemInterval);
    }
  }

  private void computeInterval(int level, Interval itemInterval)
  {
    double size = DoubleBits.powerOf2(level);
    //double size = pow2.power(level);
    pt = Math.floor(itemInterval.getMin() / size) * size;
    interval.init(pt, pt + size);
  }
}
