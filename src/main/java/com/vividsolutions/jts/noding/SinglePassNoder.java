
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
 * Base class for {@link Noder}s which make a single
 * pass to find intersections.
 * This allows using a custom {@link SegmentIntersector}
 * (which for instance may simply identify intersections, rather than
 * insert them).
 *
 * @version 1.7
 */
public abstract class SinglePassNoder
    implements Noder
{

  protected SegmentIntersector segInt;

  public SinglePassNoder() {
  }

  public SinglePassNoder(SegmentIntersector segInt) {
    setSegmentIntersector(segInt);
  }

  /**
   * Sets the SegmentIntersector to use with this noder.
   * A SegmentIntersector will normally add intersection nodes
   * to the input segment strings, but it may not - it may
   * simply record the presence of intersections.
   * However, some Noders may require that intersections be added.
   *
   * @param segInt
   */
  public void setSegmentIntersector(SegmentIntersector segInt)
  {
    this.segInt = segInt;
  }

  /**
   * Computes the noding for a collection of {@link SegmentString}s.
   * Some Noders may add all these nodes to the input SegmentStrings;
   * others may only add some or none at all.
   *
   * @param segStrings a collection of {@link SegmentString}s to node
   */
  public abstract void computeNodes(Collection segStrings);

  /**
   * Returns a {@link Collection} of fully noded {@link SegmentString}s.
   * The SegmentStrings have the same context as their parent.
   *
   * @return a Collection of SegmentStrings
   */
  public abstract Collection getNodedSubstrings();

}
