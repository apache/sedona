
/*
 * Copyright (c) 2017 Jia Yu.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.apache.sedona.jts.index.strtree;

import java.io.Serializable;
import java.util.Comparator;


/**
 * The Class BoundablePairDistanceComparator. It implements Java comparator and is used 
 * as a parameter to sort the BoundablePair list.
 */
public class BoundablePairDistanceComparator implements Comparator<BoundablePair>, Serializable{
	
	/** The normal order. */
	boolean normalOrder;

	/**
	 * Instantiates a new boundable pair distance comparator.
	 *
	 * @param normalOrder true puts the lowest record at the head of this queue.
	 * This is the natural order. PriorityQueue peek() will get the least element. 
	 */
	public BoundablePairDistanceComparator(boolean normalOrder)
	{
		this.normalOrder = normalOrder;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(BoundablePair p1, BoundablePair p2) {
		double distance1 = p1.getDistance();
		double distance2 = p2.getDistance();
		if(this.normalOrder)
		{
			if (distance1 > distance2) {
				return 1;
			} else if (distance1 == distance2) {
				return 0;
			}
			return -1;
		}
		else
		{
			if (distance1 > distance2) {
				return -1;
			} else if (distance1 == distance2) {
				return 0;
			}
			return 1;
		}

	}
}
