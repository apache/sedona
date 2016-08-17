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
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.LineIntersector;
import com.vividsolutions.jts.util.Debug;

/**
 * Processes possible intersections detected by a {@link Noder}.
 * The {@link SegmentIntersector} is passed to a {@link Noder}.
 * The {@link SegmentIntersector#processIntersections(SegmentString, int, SegmentString, int)} method is called whenever the {@link Noder}
 * detects that two SegmentStrings <i>might</i> intersect.
 * This class may be used either to find all intersections, or
 * to detect the presence of an intersection.  In the latter case,
 * Noders may choose to short-circuit their computation by calling the
 * {@link #isDone()} method.
 * This class is an example of the <i>Strategy</i> pattern.
 *
 * @version 1.7
 */
public interface SegmentIntersector
{
  /**
   * This method is called by clients
   * of the {@link SegmentIntersector} interface to process
   * intersections for two segments of the {@link SegmentString}s being intersected.
   */
  void processIntersections(
    SegmentString e0,  int segIndex0,
    SegmentString e1,  int segIndex1
     );
  
  /**
   * Reports whether the client of this class
   * needs to continue testing all intersections in an arrangement.
   * 
   * @return true if there is no need to continue testing segments
   */
  boolean isDone();
}
