

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
package com.vividsolutions.jts.operation;

import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geomgraph.GeometryGraph;

/**
 * The base class for operations that require {@link GeometryGraph}s.
 *
 * @version 1.7
 */
public class GeometryGraphOperation
{
  protected final LineIntersector li = new RobustLineIntersector();
  protected PrecisionModel resultPrecisionModel;

  /**
   * The operation args into an array so they can be accessed by index
   */
  protected GeometryGraph[] arg;  // the arg(s) of the operation

  public GeometryGraphOperation(Geometry g0, Geometry g1)
  {
    this(g0, g1,
         BoundaryNodeRule.OGC_SFS_BOUNDARY_RULE
//         BoundaryNodeRule.ENDPOINT_BOUNDARY_RULE
         );
  }

  public GeometryGraphOperation(Geometry g0, Geometry g1, BoundaryNodeRule boundaryNodeRule)
  {
    // use the most precise model for the result
    if (g0.getPrecisionModel().compareTo(g1.getPrecisionModel()) >= 0)
      setComputationPrecision(g0.getPrecisionModel());
    else
      setComputationPrecision(g1.getPrecisionModel());

    arg = new GeometryGraph[2];
    arg[0] = new GeometryGraph(0, g0, boundaryNodeRule);
    arg[1] = new GeometryGraph(1, g1, boundaryNodeRule);
  }

  public GeometryGraphOperation(Geometry g0) {
    setComputationPrecision(g0.getPrecisionModel());

    arg = new GeometryGraph[1];
    arg[0] = new GeometryGraph(0, g0);;
  }

  public Geometry getArgGeometry(int i) { return arg[i].getGeometry(); }

  protected void setComputationPrecision(PrecisionModel pm)
  {
    resultPrecisionModel = pm;
    li.setPrecisionModel(resultPrecisionModel);
  }
}
