

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
 * overlap queries on a MonotoneChain
 *
 * @version 1.7
 */
public class MonotoneChainOverlapAction
{
  // these envelopes are used during the MonotoneChain search process
  Envelope tempEnv1 = new Envelope();
  Envelope tempEnv2 = new Envelope();

  protected LineSegment overlapSeg1 = new LineSegment();
  protected LineSegment overlapSeg2 = new LineSegment();

  /**
   * This function can be overridden if the original chains are needed
   *
   * @param start1 the index of the start of the overlapping segment from mc1
   * @param start2 the index of the start of the overlapping segment from mc2
   */
  public void overlap(MonotoneChain mc1, int start1, MonotoneChain mc2, int start2)
  {
    mc1.getLineSegment(start1, overlapSeg1);
    mc2.getLineSegment(start2, overlapSeg2);
    overlap(overlapSeg1, overlapSeg2);
  }

  /**
   * This is a convenience function which can be overridden to obtain the actual
   * line segments which overlap
   * @param seg1
   * @param seg2
   */
  public void overlap(LineSegment seg1, LineSegment seg2)
  {
  }
}
