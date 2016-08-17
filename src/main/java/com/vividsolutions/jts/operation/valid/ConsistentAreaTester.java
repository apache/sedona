

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
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector;
import com.vividsolutions.jts.operation.relate.*;
import com.vividsolutions.jts.util.*;

/**
 * Checks that a {@link GeometryGraph} representing an area
 * (a {@link Polygon} or {@link MultiPolygon} )
 * has consistent semantics for area geometries.
 * This check is required for any reasonable polygonal model
 * (including the OGC-SFS model, as well as models which allow ring self-intersection at single points)
 * <p>
 * Checks include:
 * <ul>
 * <li>test for rings which properly intersect
 * (but not for ring self-intersection, or intersections at vertices)
 * <li>test for consistent labelling at all node points
 * (this detects vertex intersections with invalid topology,
 * i.e. where the exterior side of an edge lies in the interior of the area)
 * <li>test for duplicate rings
 * </ul>
 * If an inconsistency is found the location of the problem
 * is recorded and is available to the caller.
 *
 * @version 1.7
 */
public class ConsistentAreaTester {

  private final LineIntersector li = new RobustLineIntersector();
  private GeometryGraph geomGraph;
  private RelateNodeGraph nodeGraph = new RelateNodeGraph();

  // the intersection point found (if any)
  private Coordinate invalidPoint;

  /**
   * Creates a new tester for consistent areas.
   *
   * @param geomGraph the topology graph of the area geometry
   */
  public ConsistentAreaTester(GeometryGraph geomGraph)
  {
    this.geomGraph = geomGraph;
  }

    /**
   * @return the intersection point, or <code>null</code> if none was found
   */
  public Coordinate getInvalidPoint() { return invalidPoint; }

  /**
   * Check all nodes to see if their labels are consistent with area topology.
   *
   * @return <code>true</code> if this area has a consistent node labelling
   */
  public boolean isNodeConsistentArea()
  {
    /**
     * To fully check validity, it is necessary to
     * compute ALL intersections, including self-intersections within a single edge.
     */
    SegmentIntersector intersector = geomGraph.computeSelfNodes(li, true);
    if (intersector.hasProperIntersection()) {
      invalidPoint = intersector.getProperIntersectionPoint();
      return false;
    }

    nodeGraph.build(geomGraph);

    return isNodeEdgeAreaLabelsConsistent();
  }

  /**
   * Check all nodes to see if their labels are consistent.
   * If any are not, return false
   *
   * @return <code>true</code> if the edge area labels are consistent at this node
   */
  private boolean isNodeEdgeAreaLabelsConsistent()
  {
    for (Iterator nodeIt = nodeGraph.getNodeIterator(); nodeIt.hasNext(); ) {
      RelateNode node = (RelateNode) nodeIt.next();
      if (! node.getEdges().isAreaLabelsConsistent(geomGraph)) {
        invalidPoint = (Coordinate) node.getCoordinate().clone();
        return false;
      }
    }
    return true;
  }

  /**
   * Checks for two duplicate rings in an area.
   * Duplicate rings are rings that are topologically equal
   * (that is, which have the same sequence of points up to point order).
   * If the area is topologically consistent (determined by calling the
   * <code>isNodeConsistentArea</code>,
   * duplicate rings can be found by checking for EdgeBundles which contain
   * more than one EdgeEnd.
   * (This is because topologically consistent areas cannot have two rings sharing
   * the same line segment, unless the rings are equal).
   * The start point of one of the equal rings will be placed in
   * invalidPoint.
   *
   * @return true if this area Geometry is topologically consistent but has two duplicate rings
   */
  public boolean hasDuplicateRings()
  {
    for (Iterator nodeIt = nodeGraph.getNodeIterator(); nodeIt.hasNext(); ) {
      RelateNode node = (RelateNode) nodeIt.next();
      for (Iterator i = node.getEdges().iterator(); i.hasNext(); ) {
        EdgeEndBundle eeb = (EdgeEndBundle) i.next();
        if (eeb.getEdgeEnds().size() > 1) {
          invalidPoint = eeb.getEdge().getCoordinate(0);
          return true;
        }
      }
    }
    return false;
  }



}
