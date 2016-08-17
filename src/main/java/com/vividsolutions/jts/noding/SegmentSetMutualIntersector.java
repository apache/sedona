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

import java.util.*;

/**
 * An intersector for the red-blue intersection problem.
 * In this class of line arrangement problem,
 * two disjoint sets of linestrings are intersected.
 * <p>
 * Implementing classes must provide a way
 * of supplying the base set of segment strings to 
 * test against (e.g. in the constructor, 
 * for straightforward thread-safety).
 * <p>
 * In order to allow optimizing processing, 
 * the following condition is assumed to hold for each set:
 * <ul>
 * <li>the only intersection between any two linestrings occurs at their endpoints.
 * </ul>
 * Implementations can take advantage of this fact to optimize processing
 * (i.e by avoiding testing for intersections between linestrings
 * belonging to the same set).
 *
 * @author Martin Davis
 * @version 1.10
 */
public interface SegmentSetMutualIntersector
{  
  /**
   * Computes the intersections with a given set of {@link SegmentString}s,
   * using the supplied {@link SegmentIntersector}.
   *
   * @param segStrings a collection of {@link SegmentString}s to node
   * @param segInt the intersection detector to either record intersection occurences
   * 			 or add intersection nodes to the input segment strings.
   */
  void process(Collection segStrings, SegmentIntersector segInt);
}