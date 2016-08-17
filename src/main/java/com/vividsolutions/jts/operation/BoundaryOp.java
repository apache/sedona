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

import java.util.*;
import com.vividsolutions.jts.algorithm.BoundaryNodeRule;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geomgraph.GeometryGraph;

/**
 * Computes the boundary of a {@link Geometry}.
 * Allows specifying the {@link BoundaryNodeRule} to be used.
 * This operation will always return a {@link Geometry} of the appropriate
 * dimension for the boundary (even if the input geometry is empty).
 * The boundary of zero-dimensional geometries (Points) is
 * always the empty {@link GeometryCollection}.
 *
 * @author Martin Davis
 * @version 1.7
 */

public class BoundaryOp
{
  public static Geometry getBoundary(Geometry g)
  {
    BoundaryOp bop = new BoundaryOp(g);
    return bop.getBoundary();
  }
  
  public static Geometry getBoundary(Geometry g, BoundaryNodeRule bnRule)
  {
    BoundaryOp bop = new BoundaryOp(g, bnRule);
    return bop.getBoundary();
  }
  
  private Geometry geom;
  private GeometryFactory geomFact;
  private BoundaryNodeRule bnRule;

  public BoundaryOp(Geometry geom)
  {
    this(geom, BoundaryNodeRule.MOD2_BOUNDARY_RULE);
  }

  public BoundaryOp(Geometry geom, BoundaryNodeRule bnRule)
  {
    this.geom = geom;
    geomFact = geom.getFactory();
    this.bnRule = bnRule;
  }

  public Geometry getBoundary()
  {
    if (geom instanceof LineString) return boundaryLineString((LineString) geom);
    if (geom instanceof MultiLineString) return boundaryMultiLineString((MultiLineString) geom);
    return geom.getBoundary();
  }

  private MultiPoint getEmptyMultiPoint()
  {
    return geomFact.createMultiPoint((CoordinateSequence) null);
  }

  private Geometry boundaryMultiLineString(MultiLineString mLine)
  {
    if (geom.isEmpty()) {
      return getEmptyMultiPoint();
    }

    Coordinate[] bdyPts = computeBoundaryCoordinates(mLine);

    // return Point or MultiPoint
    if (bdyPts.length == 1) {
      return geomFact.createPoint(bdyPts[0]);
    }
    // this handles 0 points case as well
    return geomFact.createMultiPoint(bdyPts);
  }

/*
// MD - superseded
  private Coordinate[] computeBoundaryFromGeometryGraph(MultiLineString mLine)
  {
    GeometryGraph g = new GeometryGraph(0, mLine, bnRule);
    Coordinate[] bdyPts = g.getBoundaryPoints();
    return bdyPts;
  }
*/

  private Map endpointMap;

  private Coordinate[] computeBoundaryCoordinates(MultiLineString mLine)
  {
    List bdyPts = new ArrayList();
    endpointMap = new TreeMap();
    for (int i = 0; i < mLine.getNumGeometries(); i++) {
      LineString line = (LineString) mLine.getGeometryN(i);
      if (line.getNumPoints() == 0)
        continue;
      addEndpoint(line.getCoordinateN(0));
      addEndpoint(line.getCoordinateN(line.getNumPoints() - 1));
    }

    for (Iterator it = endpointMap.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry entry = (Map.Entry) it.next();
      Counter counter = (Counter) entry.getValue();
      int valence = counter.count;
      if (bnRule.isInBoundary(valence)) {
        bdyPts.add(entry.getKey());
      }
    }

    return CoordinateArrays.toCoordinateArray(bdyPts);
  }

  private void addEndpoint(Coordinate pt)
  {
    Counter counter = (Counter) endpointMap.get(pt);
    if (counter == null) {
      counter = new Counter();
      endpointMap.put(pt, counter);
    }
    counter.count++;
  }

  private Geometry boundaryLineString(LineString line)
  {
    if (geom.isEmpty()) {
      return getEmptyMultiPoint();
    }

    if (line.isClosed()) {
      // check whether endpoints of valence 2 are on the boundary or not
      boolean closedEndpointOnBoundary = bnRule.isInBoundary(2);
      if (closedEndpointOnBoundary) {
        return line.getStartPoint();
      }
      else {
        return geomFact.createMultiPoint((Coordinate[]) null);
      }
    }
    return geomFact.createMultiPoint(new Point[]{
                                     line.getStartPoint(),
                                     line.getEndPoint()
    });
  }
}

/**
 * Stores an integer count, for use as a Map entry.
 *
 * @author Martin Davis
 * @version 1.7
 */
class Counter
{
  /**
   * The value of the count
   */
  int count;
}
