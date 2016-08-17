


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
package com.vividsolutions.jts.geomgraph;

import java.util.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.algorithm.locate.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geomgraph.index.*;
import com.vividsolutions.jts.util.*;

/**
 * A GeometryGraph is a graph that models a given Geometry
 * @version 1.7
 */
public class GeometryGraph
  extends PlanarGraph
{
/**
 * This method implements the Boundary Determination Rule
 * for determining whether
 * a component (node or edge) that appears multiple times in elements
 * of a MultiGeometry is in the boundary or the interior of the Geometry
 * <br>
 * The SFS uses the "Mod-2 Rule", which this function implements
 * <br>
 * An alternative (and possibly more intuitive) rule would be
 * the "At Most One Rule":
 *    isInBoundary = (componentCount == 1)
 */
/*
  public static boolean isInBoundary(int boundaryCount)
  {
    // the "Mod-2 Rule"
    return boundaryCount % 2 == 1;
  }
  public static int determineBoundary(int boundaryCount)
  {
    return isInBoundary(boundaryCount) ? Location.BOUNDARY : Location.INTERIOR;
  }
*/

  public static int determineBoundary(BoundaryNodeRule boundaryNodeRule, int boundaryCount)
  {
    return boundaryNodeRule.isInBoundary(boundaryCount)
        ? Location.BOUNDARY : Location.INTERIOR;
  }

  private Geometry parentGeom;

  /**
   * The lineEdgeMap is a map of the linestring components of the
   * parentGeometry to the edges which are derived from them.
   * This is used to efficiently perform findEdge queries
   */
  private Map lineEdgeMap = new HashMap();

  private BoundaryNodeRule boundaryNodeRule = null;

  /**
   * If this flag is true, the Boundary Determination Rule will used when deciding
   * whether nodes are in the boundary or not
   */
  private boolean useBoundaryDeterminationRule = true;
  private int argIndex;  // the index of this geometry as an argument to a spatial function (used for labelling)
  private Collection boundaryNodes;
  private boolean hasTooFewPoints = false;
  private Coordinate invalidPoint = null;

  private PointOnGeometryLocator areaPtLocator = null;
  // for use if geometry is not Polygonal
  private final PointLocator ptLocator = new PointLocator();
  
  private EdgeSetIntersector createEdgeSetIntersector()
  {
  // various options for computing intersections, from slowest to fastest

  //private EdgeSetIntersector esi = new SimpleEdgeSetIntersector();
  //private EdgeSetIntersector esi = new MonotoneChainIntersector();
  //private EdgeSetIntersector esi = new NonReversingChainIntersector();
  //private EdgeSetIntersector esi = new SimpleSweepLineIntersector();
  //private EdgeSetIntersector esi = new MCSweepLineIntersector();

    //return new SimpleEdgeSetIntersector();
    return new SimpleMCSweepLineIntersector();
  }

  public GeometryGraph(int argIndex, Geometry parentGeom)
  {
    this(argIndex, parentGeom,
         BoundaryNodeRule.OGC_SFS_BOUNDARY_RULE
         );
  }

  public GeometryGraph(int argIndex, Geometry parentGeom, BoundaryNodeRule boundaryNodeRule) {
    this.argIndex = argIndex;
    this.parentGeom = parentGeom;
    this.boundaryNodeRule = boundaryNodeRule;
    if (parentGeom != null) {
//      precisionModel = parentGeom.getPrecisionModel();
//      SRID = parentGeom.getSRID();
      add(parentGeom);
    }
  }

  /**
   * This constructor is used by clients that wish to add Edges explicitly,
   * rather than adding a Geometry.  (An example is BufferOp).
   */
  // no longer used
//  public GeometryGraph(int argIndex, PrecisionModel precisionModel, int SRID) {
//    this(argIndex, null);
//    this.precisionModel = precisionModel;
//    this.SRID = SRID;
//  }
//  public PrecisionModel getPrecisionModel()
//  {
//    return precisionModel;
//  }
//  public int getSRID() { return SRID; }

  public boolean hasTooFewPoints() { return hasTooFewPoints; }

  public Coordinate getInvalidPoint() { return invalidPoint; }

  public Geometry getGeometry() { return parentGeom; }

  public BoundaryNodeRule getBoundaryNodeRule() { return boundaryNodeRule; }

  public Collection getBoundaryNodes()
  {
    if (boundaryNodes == null)
      boundaryNodes = nodes.getBoundaryNodes(argIndex);
    return boundaryNodes;
  }

  public Coordinate[] getBoundaryPoints()
  {
    Collection coll = getBoundaryNodes();
    Coordinate[] pts = new Coordinate[coll.size()];
    int i = 0;
    for (Iterator it = coll.iterator(); it.hasNext(); ) {
      Node node = (Node) it.next();
      pts[i++] = (Coordinate) node.getCoordinate().clone();
    }
    return pts;
  }

  public Edge findEdge(LineString line)
  {
    return (Edge) lineEdgeMap.get(line);
  }

  public void computeSplitEdges(List edgelist)
  {
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      Edge e = (Edge) i.next();
      e.eiList.addSplitEdges(edgelist);
    }
  }
  private void add(Geometry g)
  {
    if (g.isEmpty()) return;

    // check if this Geometry should obey the Boundary Determination Rule
    // all collections except MultiPolygons obey the rule
    if (g instanceof MultiPolygon)
      useBoundaryDeterminationRule = false;

    if (g instanceof Polygon)                 addPolygon((Polygon) g);
                        // LineString also handles LinearRings
    else if (g instanceof LineString)         addLineString((LineString) g);
    else if (g instanceof Point)              addPoint((Point) g);
    else if (g instanceof MultiPoint)         addCollection((MultiPoint) g);
    else if (g instanceof MultiLineString)    addCollection((MultiLineString) g);
    else if (g instanceof MultiPolygon)       addCollection((MultiPolygon) g);
    else if (g instanceof GeometryCollection) addCollection((GeometryCollection) g);
    else  throw new UnsupportedOperationException(g.getClass().getName());
  }

  private void addCollection(GeometryCollection gc)
  {
    for (int i = 0; i < gc.getNumGeometries(); i++) {
      Geometry g = gc.getGeometryN(i);
      add(g);
    }
  }
  /**
   * Add a Point to the graph.
   */
  private void addPoint(Point p)
  {
    Coordinate coord = p.getCoordinate();
    insertPoint(argIndex, coord, Location.INTERIOR);
  }
  
  /**
   * Adds a polygon ring to the graph.
   * Empty rings are ignored.
   * 
   * The left and right topological location arguments assume that the ring is oriented CW.
   * If the ring is in the opposite orientation,
   * the left and right locations must be interchanged.
   */
  private void addPolygonRing(LinearRing lr, int cwLeft, int cwRight)
  {
  	// don't bother adding empty holes
  	if (lr.isEmpty()) return;
  	
    Coordinate[] coord = CoordinateArrays.removeRepeatedPoints(lr.getCoordinates());

    if (coord.length < 4) {
      hasTooFewPoints = true;
      invalidPoint = coord[0];
      return;
    }

    int left  = cwLeft;
    int right = cwRight;
    if (CGAlgorithms.isCCW(coord)) {
      left = cwRight;
      right = cwLeft;
    }
    Edge e = new Edge(coord,
                        new Label(argIndex, Location.BOUNDARY, left, right));
    lineEdgeMap.put(lr, e);

    insertEdge(e);
    // insert the endpoint as a node, to mark that it is on the boundary
    insertPoint(argIndex, coord[0], Location.BOUNDARY);
  }

  private void addPolygon(Polygon p)
  {
    addPolygonRing(
            (LinearRing) p.getExteriorRing(),
            Location.EXTERIOR,
            Location.INTERIOR);

    for (int i = 0; i < p.getNumInteriorRing(); i++) {
    	LinearRing hole = (LinearRing) p.getInteriorRingN(i);
    	
      // Holes are topologically labelled opposite to the shell, since
      // the interior of the polygon lies on their opposite side
      // (on the left, if the hole is oriented CW)
      addPolygonRing(
      		hole,
          Location.INTERIOR,
          Location.EXTERIOR);
    }
  }

  private void addLineString(LineString line)
  {
    Coordinate[] coord = CoordinateArrays.removeRepeatedPoints(line.getCoordinates());

    if (coord.length < 2) {
      hasTooFewPoints = true;
      invalidPoint = coord[0];
      return;
    }

    // add the edge for the LineString
    // line edges do not have locations for their left and right sides
    Edge e = new Edge(coord, new Label(argIndex, Location.INTERIOR));
    lineEdgeMap.put(line, e);
    insertEdge(e);
    /**
     * Add the boundary points of the LineString, if any.
     * Even if the LineString is closed, add both points as if they were endpoints.
     * This allows for the case that the node already exists and is a boundary point.
     */
    Assert.isTrue(coord.length >= 2, "found LineString with single point");
    insertBoundaryPoint(argIndex, coord[0]);
    insertBoundaryPoint(argIndex, coord[coord.length - 1]);

  }

  /**
   * Add an Edge computed externally.  The label on the Edge is assumed
   * to be correct.
   */
  public void addEdge(Edge e)
  {
    insertEdge(e);
    Coordinate[] coord = e.getCoordinates();
    // insert the endpoint as a node, to mark that it is on the boundary
    insertPoint(argIndex, coord[0], Location.BOUNDARY);
    insertPoint(argIndex, coord[coord.length - 1], Location.BOUNDARY);
  }

  /**
   * Add a point computed externally.  The point is assumed to be a
   * Point Geometry part, which has a location of INTERIOR.
   */
  public void addPoint(Coordinate pt)
  {
    insertPoint(argIndex, pt, Location.INTERIOR);
  }

  /**
   * Compute self-nodes, taking advantage of the Geometry type to
   * minimize the number of intersection tests.  (E.g. rings are
   * not tested for self-intersection, since they are assumed to be valid).
   * @param li the LineIntersector to use
   * @param computeRingSelfNodes if <false>, intersection checks are optimized to not test rings for self-intersection
   * @return the SegmentIntersector used, containing information about the intersections found
   */
  public SegmentIntersector computeSelfNodes(LineIntersector li, boolean computeRingSelfNodes)
  {
    SegmentIntersector si = new SegmentIntersector(li, true, false);
    EdgeSetIntersector esi = createEdgeSetIntersector();
    // optimized test for Polygons and Rings
    if (! computeRingSelfNodes
        && (parentGeom instanceof LinearRing
        || parentGeom instanceof Polygon
        || parentGeom instanceof MultiPolygon)) {
      esi.computeIntersections(edges, si, false);
    }
    else {
      esi.computeIntersections(edges, si, true);
    }
//System.out.println("SegmentIntersector # tests = " + si.numTests);
    addSelfIntersectionNodes(argIndex);
    return si;
  }

  public SegmentIntersector computeEdgeIntersections(
    GeometryGraph g,
    LineIntersector li,
    boolean includeProper)
  {
    SegmentIntersector si = new SegmentIntersector(li, includeProper, true);
    si.setBoundaryNodes(this.getBoundaryNodes(), g.getBoundaryNodes());

    EdgeSetIntersector esi = createEdgeSetIntersector();
    esi.computeIntersections(edges, g.edges, si);
/*
for (Iterator i = g.edges.iterator(); i.hasNext();) {
Edge e = (Edge) i.next();
Debug.print(e.getEdgeIntersectionList());
}
*/
    return si;
  }

  private void insertPoint(int argIndex, Coordinate coord, int onLocation)
  {
    Node n = nodes.addNode(coord);
    Label lbl = n.getLabel();
    if (lbl == null) {
      n.label = new Label(argIndex, onLocation);
    }
    else
      lbl.setLocation(argIndex, onLocation);
  }

  /**
   * Adds candidate boundary points using the current {@link BoundaryNodeRule}.
   * This is used to add the boundary
   * points of dim-1 geometries (Curves/MultiCurves).
   */
  private void insertBoundaryPoint(int argIndex, Coordinate coord)
  {
    Node n = nodes.addNode(coord);
    // nodes always have labels
    Label lbl = n.getLabel();
    // the new point to insert is on a boundary
    int boundaryCount = 1;
    // determine the current location for the point (if any)
    int loc = Location.NONE;
    loc = lbl.getLocation(argIndex, Position.ON);
    if (loc == Location.BOUNDARY) boundaryCount++;

    // determine the boundary status of the point according to the Boundary Determination Rule
    int newLoc = determineBoundary(boundaryNodeRule, boundaryCount);
    lbl.setLocation(argIndex, newLoc);
  }

  private void addSelfIntersectionNodes(int argIndex)
  {
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      Edge e = (Edge) i.next();
      int eLoc = e.getLabel().getLocation(argIndex);
      for (Iterator eiIt = e.eiList.iterator(); eiIt.hasNext(); ) {
        EdgeIntersection ei = (EdgeIntersection) eiIt.next();
        addSelfIntersectionNode(argIndex, ei.coord, eLoc);
      }
    }
  }
  /**
   * Add a node for a self-intersection.
   * If the node is a potential boundary node (e.g. came from an edge which
   * is a boundary) then insert it as a potential boundary node.
   * Otherwise, just add it as a regular node.
   */
  private void addSelfIntersectionNode(int argIndex, Coordinate coord, int loc)
  {
    // if this node is already a boundary node, don't change it
    if (isBoundaryNode(argIndex, coord)) return;
    if (loc == Location.BOUNDARY && useBoundaryDeterminationRule)
        insertBoundaryPoint(argIndex, coord);
    else
      insertPoint(argIndex, coord, loc);
  }

  // MD - experimental for now
  /**
   * Determines the {@link Location} of the given {@link Coordinate}
   * in this geometry.
   * 
   * @param p the point to test
   * @return the location of the point in the geometry
   */
  public int locate(Coordinate pt)
  {
  	if (parentGeom instanceof Polygonal && parentGeom.getNumGeometries() > 50) {
  		// lazily init point locator
  		if (areaPtLocator == null) {
  			areaPtLocator = new IndexedPointInAreaLocator(parentGeom);
  		}
  		return areaPtLocator.locate(pt);
  	}
  	return ptLocator.locate(pt, parentGeom);
  }
}
