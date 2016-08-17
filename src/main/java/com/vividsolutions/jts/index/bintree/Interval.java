
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

/**
 * Represents an (1-dimensional) closed interval on the Real number line.
 *
 * @version 1.7
 */
public class Interval {

  public double min, max;

  public Interval()
  {
    min = 0.0;
    max = 0.0;
  }

  public Interval(double min, double max)
  {
    init(min, max);
  }
  public Interval(Interval interval)
  {
    init(interval.min, interval.max);
  }
  public void init(double min, double max)
  {
    this.min = min;
    this.max = max;
    if (min > max) {
      this.min = max;
      this.max = min;
    }
  }
  public double getMin() { return min; }
  public double getMax() { return max; }
  public double getWidth() { return max - min; }

  public void expandToInclude(Interval interval)
  {
    if (interval.max > max) max = interval.max;
    if (interval.min < min) min = interval.min;
  }
  public boolean overlaps(Interval interval)
  {
    return overlaps(interval.min, interval.max);
  }

  public boolean overlaps(double min, double max)
  {
    if (this.min > max || this.max < min) return false;
    return true;
  }

  public boolean contains(Interval interval)
  {
    return contains(interval.min, interval.max);
  }
  public boolean contains(double min, double max)
  {
    return (min >= this.min && max <= this.max);
  }
  public boolean contains(double p)
  {
    return (p >= this.min && p <= this.max);
  }

  public String toString()
  {
    return "[" + min + ", " + max + "]";
  }
}
