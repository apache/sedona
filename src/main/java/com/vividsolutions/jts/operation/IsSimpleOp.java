

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
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.util.LinearComponentExtracter;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector;

/**
 * Tests whether a <code>Geometry</code> is simple.
 * In general, the SFS specification of simplicity
 * follows the rule:
 * <ul>
 *    <li> A Geometry is simple if and only if the only self-intersections are at
 *    boundary points.
 * </ul>
 * <p>
 * Simplicity is defined for each {@link Geometry} type as follows:
 * <ul>
 * <li><b>Polygonal</b> geometries are simple by definition, so
 * <code>isSimple</code> trivially returns true.
 * (Note: this means that <tt>isSimple</tt> cannot be used to test 
 * for (invalid) self-intersections in <tt>Polygon</tt>s.  
 * In order to check if a <tt>Polygonal</tt> geometry has self-intersections,
 * use {@link Geometry#isValid()}).
 * <li><b>Linear</b> geometries are simple iff they do <i>not</i> self-intersect at interior points
 * (i.e. points other than boundary points).
 * This is equivalent to saying that no two linear components satisfy the SFS {@link Geometry#touches(Geometry)}
 * predicate. 
 * <li><b>Zero-dimensional (point)</b> geometries are simple if and only if they have no
 * repeated points.
 * <li><b>Empty</b> geometries are <i>always</i> simple, by definition
 * </ul>
 * For {@link Lineal} geometries the evaluation of simplicity  
 * can be customized by supplying a {@link BoundaryNodeRule} 
 * to define how boundary points are determined.
 * The default is the SFS-standard {@link BoundaryNodeRule#MOD2_BOUNDARY_RULE}.
 * Note that under the <tt>Mod-2</tt> rule, closed <tt>LineString</tt>s (rings)
 * will never satisfy the <tt>touches</tt> predicate at their endpoints, since these are
 * interior points, not boundary points. 
 * If it is required to test whether a set of <code>LineString</code>s touch
 * only at their endpoints, use <code>IsSimpleOp</code> with {@link BoundaryNodeRule#ENDPOINT_BOUNDARY_RULE}.
 * For example, this can be used to validate that a set of lines form a topologically valid
 * linear network.
 * 
 * @see BoundaryNodeRule
 *
 * @version 1.7
 */
public class IsSimpleOp
{
  private Geometry inputGeom;
  private boolean isClosedEndpointsInInterior = true;
  private Coordinate nonSimpleLocation = null;

  /**
   * Creates a simplicity checker using the default SFS Mod-2 Boundary Node Rule
   *
   * @deprecated use IsSimpleOp(Geometry)
   */
  public IsSimpleOp() {
  }

  /**
   * Creates a simplicity checker using the default SFS Mod-2 Boundary Node Rule
   *
   * @param geom the geometry to test
   */
  public IsSimpleOp(Geometry geom) {
    this.inputGeom = geom;
  }

  /**
   * Creates a simplicity checker using a given {@link BoundaryNodeRule}
   *
   * @param geom the geometry to test
   * @param boundaryNodeRule the rule to use.
   */
  public IsSimpleOp(Geometry geom, BoundaryNodeRule boundaryNodeRule)
  {
    this.inputGeom = geom;
    isClosedEndpointsInInterior = ! boundaryNodeRule.isInBoundary(2);
  }

  /**
   * Tests whether the geometry is simple.
   *
   * @return true if the geometry is simple
   */
  public boolean isSimple()
  {
    nonSimpleLocation = null;
    return computeSimple(inputGeom);
  }
  
  private boolean computeSimple(Geometry geom)
  {
    nonSimpleLocation = null;
    if (geom.isEmpty()) return true;
    if (geom instanceof LineString) return isSimpleLinearGeometry(geom);
    if (geom instanceof MultiLineString) return isSimpleLinearGeometry(geom);
    if (geom instanceof MultiPoint) return isSimpleMultiPoint((MultiPoint) geom);
    if (geom instanceof Polygonal) return isSimplePolygonal(geom);
    if (geom instanceof GeometryCollection) return isSimpleGeometryCollection(geom);
    // all other geometry types are simple by definition
    return true;
  }

  /**
   * Gets a coordinate for the location where the geometry
   * fails to be simple. 
   * (i.e. where it has a non-boundary self-intersection).
   * {@link #isSimple} must be called before this method is called.
   *
   * @return a coordinate for the location of the non-boundary self-intersection
   * or null if the geometry is simple
   */
  public Coordinate getNonSimpleLocation()
  {
    return nonSimpleLocation;
  }

  /**
   * Reports whether a {@link LineString} is simple.
   *
   * @param geom the lineal geometry to test
   * @return true if the geometry is simple
   * @deprecated use isSimple()
   */
  public boolean isSimple(LineString geom)
  {
    return isSimpleLinearGeometry(geom);
  }

  /**
   * Reports whether a {@link MultiLineString} geometry is simple.
   *
   * @param geom the lineal geometry to test
   * @return true if the geometry is simple
   * @deprecated use isSimple()
   */
  public boolean isSimple(MultiLineString geom)
  {
    return isSimpleLinearGeometry(geom);
  }

  /**
   * A MultiPoint is simple iff it has no repeated points
   * @deprecated use isSimple()
   */
  public boolean isSimple(MultiPoint mp)
  {
    return isSimpleMultiPoint(mp);
  }

  private boolean isSimpleMultiPoint(MultiPoint mp)
  {
    if (mp.isEmpty()) return true;
    Set points = new TreeSet();
    for (int i = 0; i < mp.getNumGeometries(); i++) {
      Point pt = (Point) mp.getGeometryN(i);
      Coordinate p = pt.getCoordinate();
      if (points.contains(p)) {
        nonSimpleLocation = p;
        return false;
      }
      points.add(p);
    }
    return true;
  }

  /**
   * Computes simplicity for polygonal geometries.
   * Polygonal geometries are simple if and only if
   * all of their component rings are simple.
   * 
   * @param geom a Polygonal geometry
   * @return true if the geometry is simple
   */
  private boolean isSimplePolygonal(Geometry geom)
  {
    List rings = LinearComponentExtracter.getLines(geom);
    for (Iterator i = rings.iterator(); i.hasNext(); ) {
      LinearRing ring = (LinearRing) i.next();
      if (! isSimpleLinearGeometry(ring))
        return false;
    }
    return true;
  }
  
  /**
   * Semantics for GeometryCollection is 
   * simple iff all components are simple.
   * 
   * @param geom
   * @return true if the geometry is simple
   */
  private boolean isSimpleGeometryCollection(Geometry geom)
  {
    for (int i = 0; i < geom.getNumGeometries(); i++ ) {
      Geometry comp = geom.getGeometryN(i);
      if (! computeSimple(comp))
        return false;
    }
    return true;
  }
  
  private boolean isSimpleLinearGeometry(Geometry geom)
  {
    if (geom.isEmpty()) return true;
    GeometryGraph graph = new GeometryGraph(0, geom);
    LineIntersector li = new RobustLineIntersector();
    SegmentIntersector si = graph.computeSelfNodes(li, true);
    // if no self-intersection, must be simple
    if (! si.hasIntersection()) return true;
    if (si.hasProperIntersection()) {
      nonSimpleLocation = si.getProperIntersectionPoint();
      return false;
    }
    if (hasNonEndpointIntersection(graph)) return false;
    if (isClosedEndpointsInInterior) {
      if (hasClosedEndpointIntersection(graph)) return false;
    }
    return true;
  }

  /**
   * For all edges, check if there are any intersections which are NOT at an endpoint.
   * The Geometry is not simple if there are intersections not at endpoints.
   */
  private boolean hasNonEndpointIntersection(GeometryGraph graph)
  {
    for (Iterator i = graph.getEdgeIterator(); i.hasNext(); ) {
      Edge e = (Edge) i.next();
      int maxSegmentIndex = e.getMaximumSegmentIndex();
      for (Iterator eiIt = e.getEdgeIntersectionList().iterator(); eiIt.hasNext(); ) {
        EdgeIntersection ei = (EdgeIntersection) eiIt.next();
        if (! ei.isEndPoint(maxSegmentIndex)) {
          nonSimpleLocation = ei.getCoordinate();
          return true;
        }
      }
    }
    return false;
  }

  private static class EndpointInfo {
    Coordinate pt;
    boolean isClosed;
    int degree;

    public EndpointInfo(Coordinate pt)
    {
      this.pt = pt;
      isClosed = false;
      degree = 0;
    }

    public Coordinate getCoordinate() { return pt; }

    public void addEndpoint(boolean isClosed)
    {
      degree++;
      this.isClosed |= isClosed;
    }
  }

  /**
   * Tests that no edge intersection is the endpoint of a closed line.
   * This ensures that closed lines are not touched at their endpoint,
   * which is an interior point according to the Mod-2 rule
   * To check this we compute the degree of each endpoint.
   * The degree of endpoints of closed lines
   * must be exactly 2.
   */
  private boolean hasClosedEndpointIntersection(GeometryGraph graph)
  {
    Map endPoints = new TreeMap();
    for (Iterator i = graph.getEdgeIterator(); i.hasNext(); ) {
      Edge e = (Edge) i.next();
      int maxSegmentIndex = e.getMaximumSegmentIndex();
      boolean isClosed = e.isClosed();
      Coordinate p0 = e.getCoordinate(0);
      addEndpoint(endPoints, p0, isClosed);
      Coordinate p1 = e.getCoordinate(e.getNumPoints() - 1);
      addEndpoint(endPoints, p1, isClosed);
    }

    for (Iterator i = endPoints.values().iterator(); i.hasNext(); ) {
      EndpointInfo eiInfo = (EndpointInfo) i.next();
      if (eiInfo.isClosed && eiInfo.degree != 2) {
        nonSimpleLocation = eiInfo.getCoordinate();
        return true;
      }
    }
    return false;
  }

  /**
   * Add an endpoint to the map, creating an entry for it if none exists
   */
  private void addEndpoint(Map endPoints, Coordinate p, boolean isClosed)
  {
    EndpointInfo eiInfo = (EndpointInfo) endPoints.get(p);
    if (eiInfo == null) {
      eiInfo = new EndpointInfo(p);
      endPoints.put(p, eiInfo);
    }
    eiInfo.addEndpoint(isClosed);
  }

}
