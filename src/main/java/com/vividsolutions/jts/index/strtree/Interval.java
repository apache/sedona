
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
package com.vividsolutions.jts.index.strtree;

import com.vividsolutions.jts.util.*;

/**
 * A contiguous portion of 1D-space. Used internally by SIRtree.
 * @see SIRtree
 *
 * @version 1.7
 */
public class Interval {

  public Interval(Interval other) {
    this(other.min, other.max);
  }

  public Interval(double min, double max) {
    Assert.isTrue(min <= max);
    this.min = min;
    this.max = max;
  }

  private double min;
  private double max;

  public double getCentre() { return (min+max)/2; }

  /**
   * @return this
   */
  public Interval expandToInclude(Interval other) {
    max = Math.max(max, other.max);
    min = Math.min(min, other.min);
    return this;
  }

  public boolean intersects(Interval other) {
    return !(other.min > max || other.max < min);
  }
  public boolean equals(Object o) {
    if (! (o instanceof Interval)) { return false; }
    Interval other = (Interval) o;
    return min == other.min && max == other.max;
  }
}
