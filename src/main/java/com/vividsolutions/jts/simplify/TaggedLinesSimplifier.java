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

package com.vividsolutions.jts.simplify;

import java.util.Collection;
import java.util.Iterator;

/**
 * Simplifies a collection of TaggedLineStrings, preserving topology
 * (in the sense that no new intersections are introduced).
 * This class is essentially just a container for the common
 * indexes used by {@link TaggedLineStringSimplifier}.
 */
class TaggedLinesSimplifier
{
  private LineSegmentIndex inputIndex = new LineSegmentIndex();
  private LineSegmentIndex outputIndex = new LineSegmentIndex();
  private double distanceTolerance = 0.0;

  public TaggedLinesSimplifier()
  {

  }

  /**
   * Sets the distance tolerance for the simplification.
   * All vertices in the simplified geometry will be within this
   * distance of the original geometry.
   *
   * @param distanceTolerance the approximation tolerance to use
   */
  public void setDistanceTolerance(double distanceTolerance) {
    this.distanceTolerance = distanceTolerance;
  }

  /**
   * Simplify a collection of TaggedLineStrings
   *
   * @param taggedLines the collection of lines to simplify
   */
  public void simplify(Collection taggedLines) {
    for (Iterator i = taggedLines.iterator(); i.hasNext(); ) {
      inputIndex.add((TaggedLineString) i.next());
    }
    for (Iterator i = taggedLines.iterator(); i.hasNext(); ) {
      TaggedLineStringSimplifier tlss
                    = new TaggedLineStringSimplifier(inputIndex, outputIndex);
      tlss.setDistanceTolerance(distanceTolerance);
      tlss.simplify((TaggedLineString) i.next());
    }
  }

}
