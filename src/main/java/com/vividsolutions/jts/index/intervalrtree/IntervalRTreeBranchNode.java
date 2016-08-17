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

import com.vividsolutions.jts.index.*;

public class IntervalRTreeBranchNode 
extends IntervalRTreeNode
{
	private IntervalRTreeNode node1;
	private IntervalRTreeNode node2;
	
	public IntervalRTreeBranchNode(IntervalRTreeNode n1, IntervalRTreeNode n2)
	{
		node1 = n1;
		node2 = n2;
		buildExtent(node1, node2);
	}
	
	private void buildExtent(IntervalRTreeNode n1, IntervalRTreeNode n2)
	{
		min = Math.min(n1.min, n2.min);
		max = Math.max(n1.max, n2.max);
	}
	
	public void query(double queryMin, double queryMax, ItemVisitor visitor)
	{
		if (! intersects(queryMin, queryMax)) {
//			System.out.println("Does NOT Overlap branch: " + this);
			return;
		}
//		System.out.println("Overlaps branch: " + this);
		if (node1 != null) node1.query(queryMin, queryMax, visitor);
		if (node2 != null) node2.query(queryMin, queryMax, visitor);
	}
	
}
