
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
package com.vividsolutions.jts.operation.valid;

import java.util.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.util.*;

/**
 * Tests whether any of a set of {@link LinearRing}s are
 * nested inside another ring in the set, using a simple O(n^2)
 * comparison.
 *
 * @version 1.7
 */
public class SimpleNestedRingTester
{

  private GeometryGraph graph;  // used to find non-node vertices
  private List rings = new ArrayList();
  private Coordinate nestedPt;

  public SimpleNestedRingTester(GeometryGraph graph)
  {
    this.graph = graph;
  }

  public void add(LinearRing ring)
  {
    rings.add(ring);
  }

  public Coordinate getNestedPoint() { return nestedPt; }

  public boolean isNonNested()
  {
    for (int i = 0; i < rings.size(); i++) {
      LinearRing innerRing = (LinearRing) rings.get(i);
      Coordinate[] innerRingPts = innerRing.getCoordinates();

      for (int j = 0; j < rings.size(); j++) {
        LinearRing searchRing = (LinearRing) rings.get(j);
        Coordinate[] searchRingPts = searchRing.getCoordinates();

        if (innerRing == searchRing)
          continue;

        if (! innerRing.getEnvelopeInternal().intersects(searchRing.getEnvelopeInternal()))
          continue;

        Coordinate innerRingPt = IsValidOp.findPtNotNode(innerRingPts, searchRing, graph);
        Assert.isTrue(innerRingPt != null, "Unable to find a ring point not a node of the search ring");
        //Coordinate innerRingPt = innerRingPts[0];

        boolean isInside = CGAlgorithms.isPointInRing(innerRingPt, searchRingPts);
        if (isInside) {
          nestedPt = innerRingPt;
          return false;
        }
      }
    }
    return true;
  }

}
