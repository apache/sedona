

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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geomgraph.*;

/**
 * A ring of {@link Edge}s with the property that no node
 * has degree greater than 2.  These are the form of rings required
 * to represent polygons under the OGC SFS spatial data model.
 *
 * @version 1.7
 * @see com.vividsolutions.jts.operation.overlay.MaximalEdgeRing
 */
public class MinimalEdgeRing
  extends EdgeRing
{

  public MinimalEdgeRing(DirectedEdge start, GeometryFactory geometryFactory) {
    super(start, geometryFactory);
  }

  public DirectedEdge getNext(DirectedEdge de)
  {
    return de.getNextMin();
  }
  public void setEdgeRing(DirectedEdge de, EdgeRing er)
  {
    de.setMinEdgeRing(er);
  }

}
