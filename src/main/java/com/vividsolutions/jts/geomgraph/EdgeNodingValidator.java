
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

import java.util.*;
import com.vividsolutions.jts.noding.*;

/**
 * Validates that a collection of {@link Edge}s is correctly noded.
 * Throws an appropriate exception if an noding error is found.
 *
 * @version 1.7
 */
public class EdgeNodingValidator 
{  
	/**
   * Checks whether the supplied {@link Edge}s
   * are correctly noded.  
   * Throws a  {@link TopologyException} if they are not.
   * 
   * @param edges a collection of Edges.
   * @throws TopologyException if the SegmentStrings are not correctly noded
   *
   */
	public static void checkValid(Collection edges)
	{
		EdgeNodingValidator validator = new EdgeNodingValidator(edges);
		validator.checkValid();
	}
	
  public static Collection toSegmentStrings(Collection edges)
  {
    // convert Edges to SegmentStrings
    Collection segStrings = new ArrayList();
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      Edge e = (Edge) i.next();
      segStrings.add(new BasicSegmentString(e.getCoordinates(), e));
    }
    return segStrings;
  }

  private FastNodingValidator nv;

  /**
   * Creates a new validator for the given collection of {@link Edge}s.
   * 
   * @param edges a collection of Edges.
   */
  public EdgeNodingValidator(Collection edges)
  {
    nv = new FastNodingValidator(toSegmentStrings(edges));
  }

  /**
   * Checks whether the supplied edges
   * are correctly noded.  Throws an exception if they are not.
   * 
   * @throws TopologyException if the SegmentStrings are not correctly noded
   *
   */
  public void checkValid()
  {
    nv.checkValid();
  }

}
