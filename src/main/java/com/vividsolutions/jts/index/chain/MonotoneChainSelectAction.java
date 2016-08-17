

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
package com.vividsolutions.jts.index.chain;

import com.vividsolutions.jts.geom.*;
/**
 * The action for the internal iterator for performing
 * envelope select queries on a MonotoneChain
 *
 * @version 1.7
 */
public class MonotoneChainSelectAction
{
  // these envelopes are used during the MonotoneChain search process
  Envelope tempEnv1 = new Envelope();

  LineSegment selectedSegment = new LineSegment();

  /**
   * This method is overridden 
   * to process a segment 
   * in the context of the parent chain.
   * 
   * @param mc the parent chain
   * @param startIndex the index of the start vertex of the segment being processed
   */
  public void select(MonotoneChain mc, int startIndex)
  {
    mc.getLineSegment(startIndex, selectedSegment);
    // call this routine in case select(segmenet) was overridden
    select(selectedSegment);
  }

  /**
   * This is a convenience method which can be overridden to obtain the actual
   * line segment which is selected.
   * 
   * @param seg
   */
  public void select(LineSegment seg)
  {
  }
}
