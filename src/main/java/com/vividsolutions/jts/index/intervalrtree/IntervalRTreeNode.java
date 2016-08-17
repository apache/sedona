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
package com.vividsolutions.jts.index.intervalrtree;

import java.util.Comparator;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.*;
import com.vividsolutions.jts.index.*;

public abstract class IntervalRTreeNode 
{
	protected double min = Double.POSITIVE_INFINITY;
	protected double max = Double.NEGATIVE_INFINITY;

	public double getMin() { return min; }
	public double getMax() { return max; }
	
	public abstract void query(double queryMin, double queryMax, ItemVisitor visitor);
	
	protected boolean intersects(double queryMin, double queryMax)
	{
		if (min > queryMax 
				|| max < queryMin)
			return false;
		return true;
	}

	public String toString()
	{
		return WKTWriter.toLineString(new Coordinate(min, 0), new Coordinate(max, 0));
	}
  
  public static class NodeComparator implements Comparator
  {
    public int compare(Object o1, Object o2)
    {
      IntervalRTreeNode n1 = (IntervalRTreeNode) o1;
      IntervalRTreeNode n2 = (IntervalRTreeNode) o2;
      double mid1 = (n1.min + n1.max) / 2;
      double mid2 = (n2.min + n2.max) / 2;
      if (mid1 < mid2) return -1;
      if (mid1 > mid2) return 1;
      return 0;
    }
  }

}
