

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
package com.vividsolutions.jts.operation.overlay;

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geomgraph.*;

/**
 * A ring of {@link DirectedEdge}s which may contain nodes of degree > 2.
 * A <tt>MaximalEdgeRing</tt> may represent two different spatial entities:
 * <ul>
 * <li>a single polygon possibly containing inversions (if the ring is oriented CW)
 * <li>a single hole possibly containing exversions (if the ring is oriented CCW)
 * </ul>
 * If the MaximalEdgeRing represents a polygon,
 * the interior of the polygon is strongly connected.
 * <p>
 * These are the form of rings used to define polygons under some spatial data models.
 * However, under the OGC SFS model, {@link MinimalEdgeRing}s are required.
 * A MaximalEdgeRing can be converted to a list of MinimalEdgeRings using the
 * {@link #buildMinimalRings() } method.
 *
 * @version 1.7
 * @see com.vividsolutions.jts.operation.overlay.MinimalEdgeRing
 */
public class MaximalEdgeRing
  extends EdgeRing
{

  public MaximalEdgeRing(DirectedEdge start, GeometryFactory geometryFactory) {
    super(start, geometryFactory);
  }

  public DirectedEdge getNext(DirectedEdge de)
  {
    return de.getNext();
  }
  public void setEdgeRing(DirectedEdge de, EdgeRing er)
  {
    de.setEdgeRing(er);
  }

  /**
   * For all nodes in this EdgeRing,
   * link the DirectedEdges at the node to form minimalEdgeRings
   */
  public void linkDirectedEdgesForMinimalEdgeRings()
  {
    DirectedEdge de = startDe;
    do {
      Node node = de.getNode();
      ((DirectedEdgeStar) node.getEdges()).linkMinimalDirectedEdges(this);
      de = de.getNext();
    } while (de != startDe);
  }

  public List buildMinimalRings()
  {
    List minEdgeRings = new ArrayList();
    DirectedEdge de = startDe;
    do {
      if (de.getMinEdgeRing() == null) {
        EdgeRing minEr = new MinimalEdgeRing(de, geometryFactory);
        minEdgeRings.add(minEr);
      }
      de = de.getNext();
    } while (de != startDe);
    return minEdgeRings;
  }

}
