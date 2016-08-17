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

package com.vividsolutions.jts.triangulate.quadedge;

import java.util.*;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * A class that contains the {@link QuadEdge}s representing a planar
 * subdivision that models a triangulation. 
 * The subdivision is constructed using the
 * quadedge algebra defined in the classs {@link QuadEdge}. 
 * All metric calculations
 * are done in the {@link Vertex} class.
 * In addition to a triangulation, subdivisions
 * support extraction of Voronoi diagrams.
 * This is easily accomplished, since the Voronoi diagram is the dual
 * of the Delaunay triangulation.
 * <p>
 * Subdivisions can be provided with a tolerance value. Inserted vertices which
 * are closer than this value to vertices already in the subdivision will be
 * ignored. Using a suitable tolerance value can prevent robustness failures
 * from happening during Delaunay triangulation.
 * <p>
 * Subdivisions maintain a <b>frame</b> triangle around the client-created
 * edges. The frame is used to provide a bounded "container" for all edges
 * within a TIN. Normally the frame edges, frame connecting edges, and frame
 * triangles are not included in client processing.
 * 
 * @author David Skea
 * @author Martin Davis
 */
public class QuadEdgeSubdivision {
	/**
	 * Gets the edges for the triangle to the left of the given {@link QuadEdge}.
	 * 
	 * @param startQE
	 * @param triEdge
	 * 
	 * @throws IllegalArgumentException
	 *           if the edges do not form a triangle
	 */
	public static void getTriangleEdges(QuadEdge startQE, QuadEdge[] triEdge) {
		triEdge[0] = startQE;
		triEdge[1] = triEdge[0].lNext();
		triEdge[2] = triEdge[1].lNext();
		if (triEdge[2].lNext() != triEdge[0])
			throw new IllegalArgumentException("Edges do not form a triangle");
	}

	private final static double EDGE_COINCIDENCE_TOL_FACTOR = 1000;

	// debugging only - preserve current subdiv statically
	// private static QuadEdgeSubdivision currentSubdiv;

	// used for edge extraction to ensure edge uniqueness
	private int visitedKey = 0;
//	private Set quadEdges = new HashSet();
	private List quadEdges = new ArrayList();
	private QuadEdge startingEdge;
	private double tolerance;
	private double edgeCoincidenceTolerance;
	private Vertex[] frameVertex = new Vertex[3];
	private Envelope frameEnv;
	private QuadEdgeLocator locator = null;

	/**
	 * Creates a new instance of a quad-edge subdivision based on a frame triangle
	 * that encloses a supplied bounding box. A new super-bounding box that
	 * contains the triangle is computed and stored.
	 * 
	 * @param env
	 *          the bouding box to surround
	 * @param tolerance
	 *          the tolerance value for determining if two sites are equal
	 */
	public QuadEdgeSubdivision(Envelope env, double tolerance) {
		// currentSubdiv = this;
		this.tolerance = tolerance;
		edgeCoincidenceTolerance = tolerance / EDGE_COINCIDENCE_TOL_FACTOR;

		createFrame(env);
		
		startingEdge = initSubdiv();
		locator = new LastFoundQuadEdgeLocator(this);
	}

	private void createFrame(Envelope env)
	{
		double deltaX = env.getWidth();
		double deltaY = env.getHeight();
		double offset = 0.0;
		if (deltaX > deltaY) {
			offset = deltaX * 10.0;
		} else {
			offset = deltaY * 10.0;
		}

		frameVertex[0] = new Vertex((env.getMaxX() + env.getMinX()) / 2.0, env
				.getMaxY()
				+ offset);
		frameVertex[1] = new Vertex(env.getMinX() - offset, env.getMinY() - offset);
		frameVertex[2] = new Vertex(env.getMaxX() + offset, env.getMinY() - offset);

		frameEnv = new Envelope(frameVertex[0].getCoordinate(), frameVertex[1]
				.getCoordinate());
		frameEnv.expandToInclude(frameVertex[2].getCoordinate());
	}
	
	private QuadEdge initSubdiv()
	{
		// build initial subdivision from frame
		QuadEdge ea = makeEdge(frameVertex[0], frameVertex[1]);
		QuadEdge eb = makeEdge(frameVertex[1], frameVertex[2]);
		QuadEdge.splice(ea.sym(), eb);
		QuadEdge ec = makeEdge(frameVertex[2], frameVertex[0]);
		QuadEdge.splice(eb.sym(), ec);
		QuadEdge.splice(ec.sym(), ea);
		return ea;
	}
	
	/**
	 * Gets the vertex-equality tolerance value
	 * used in this subdivision
	 * 
	 * @return the tolerance value
	 */
	public double getTolerance() {
		return tolerance;
	}

	/**
	 * Gets the envelope of the Subdivision (including the frame).
	 * 
	 * @return the envelope
	 */
	public Envelope getEnvelope() {
		return new Envelope(frameEnv);
	}

	/**
	 * Gets the collection of base {@link QuadEdge}s (one for every pair of
	 * vertices which is connected).
	 * 
	 * @return a collection of QuadEdges
	 */
	public Collection getEdges() {
		return quadEdges;
	}

	/**
	 * Sets the {@link QuadEdgeLocator} to use for locating containing triangles
	 * in this subdivision.
	 * 
	 * @param locator
	 *          a QuadEdgeLocator
	 */
	public void setLocator(QuadEdgeLocator locator) {
		this.locator = locator;
	}

	/**
	 * Creates a new quadedge, recording it in the edges list.
	 * 
	 * @param o
	 * @param d
	 * @return a new quadedge
	 */
	public QuadEdge makeEdge(Vertex o, Vertex d) {
		QuadEdge q = QuadEdge.makeEdge(o, d);
		quadEdges.add(q);
		return q;
	}

	/**
	 * Creates a new QuadEdge connecting the destination of a to the origin of b,
	 * in such a way that all three have the same left face after the connection
	 * is complete. The quadedge is recorded in the edges list.
	 * 
	 * @param a
	 * @param b
	 * @return a quadedge
	 */
	public QuadEdge connect(QuadEdge a, QuadEdge b) {
		QuadEdge q = QuadEdge.connect(a, b);
		quadEdges.add(q);
		return q;
	}

	/**
	 * Deletes a quadedge from the subdivision. Linked quadedges are updated to
	 * reflect the deletion.
	 * 
	 * @param e
	 *          the quadedge to delete
	 */
	public void delete(QuadEdge e) {
		QuadEdge.splice(e, e.oPrev());
		QuadEdge.splice(e.sym(), e.sym().oPrev());

		QuadEdge eSym = e.sym();
		QuadEdge eRot = e.rot();
		QuadEdge eRotSym = e.rot().sym();

		// this is inefficient on an ArrayList, but this method should be called infrequently
		quadEdges.remove(e);
		quadEdges.remove(eSym);
		quadEdges.remove(eRot);
		quadEdges.remove(eRotSym);

		e.delete();
		eSym.delete();
		eRot.delete();
		eRotSym.delete();
	}

	/**
	 * Locates an edge of a triangle which contains a location 
	 * specified by a Vertex v. 
	 * The edge returned has the
	 * property that either v is on e, or e is an edge of a triangle containing v.
	 * The search starts from startEdge amd proceeds on the general direction of v.
	 * <p>
	 * This locate algorithm relies on the subdivision being Delaunay. For
	 * non-Delaunay subdivisions, this may loop for ever.
	 * 
	 * @param v the location to search for
	 * @param startEdge an edge of the subdivision to start searching at
	 * @returns a QuadEdge which contains v, or is on the edge of a triangle containing v
	 * @throws LocateFailureException
	 *           if the location algorithm fails to converge in a reasonable
	 *           number of iterations
	 */
	public QuadEdge locateFromEdge(Vertex v, QuadEdge startEdge) {
		int iter = 0;
		int maxIter = quadEdges.size();

		QuadEdge e = startEdge;

		while (true) {
			iter++;

			/**
			 * So far it has always been the case that failure to locate indicates an
			 * invalid subdivision. So just fail completely. (An alternative would be
			 * to perform an exhaustive search for the containing triangle, but this
			 * would mask errors in the subdivision topology)
			 * 
			 * This can also happen if two vertices are located very close together,
			 * since the orientation predicates may experience precision failures.
			 */
			if (iter > maxIter) {
				throw new LocateFailureException(e.toLineSegment());
				// String msg = "Locate failed to converge (at edge: " + e + ").
				// Possible causes include invalid Subdivision topology or very close
				// sites";
				// System.err.println(msg);
				// dumpTriangles();
			}

			if ((v.equals(e.orig())) || (v.equals(e.dest()))) {
				break;
			} else if (v.rightOf(e)) {
				e = e.sym();
			} else if (!v.rightOf(e.oNext())) {
				e = e.oNext();
			} else if (!v.rightOf(e.dPrev())) {
				e = e.dPrev();
			} else {
				// on edge or in triangle containing edge
				break;
			}
		}
		// System.out.println("Locate count: " + iter);
		return e;
	}

	/**
	 * Finds a quadedge of a triangle containing a location 
	 * specified by a {@link Vertex}, if one exists.
	 * 
	 * @param v the vertex to locate
	 * @return a quadedge on the edge of a triangle which touches or contains the location
	 * or null if no such triangle exists
	 */
	public QuadEdge locate(Vertex v) {
		return locator.locate(v);
	}

	/**
	 * Finds a quadedge of a triangle containing a location
	 * specified by a {@link Coordinate}, if one exists.
	 * 
	 * @param p the Coordinate to locate
	 * @return a quadedge on the edge of a triangle which touches or contains the location
	 * or null if no such triangle exists
	 */
	public QuadEdge locate(Coordinate p) {
		return locator.locate(new Vertex(p));
	}

	/**
	 * Locates the edge between the given vertices, if it exists in the
	 * subdivision.
	 * 
	 * @param p0 a coordinate
	 * @param p1 another coordinate
	 * @return the edge joining the coordinates, if present
	 * or null if no such edge exists
	 */
	public QuadEdge locate(Coordinate p0, Coordinate p1) {
		// find an edge containing one of the points
		QuadEdge e = locator.locate(new Vertex(p0));
		if (e == null)
			return null;

		// normalize so that p0 is origin of base edge
		QuadEdge base = e;
		if (e.dest().getCoordinate().equals2D(p0))
			base = e.sym();
		// check all edges around origin of base edge
		QuadEdge locEdge = base;
		do {
			if (locEdge.dest().getCoordinate().equals2D(p1))
				return locEdge;
			locEdge = locEdge.oNext();
		} while (locEdge != base);
		return null;
	}

	/**
	 * Inserts a new site into the Subdivision, connecting it to the vertices of
	 * the containing triangle (or quadrilateral, if the split point falls on an
	 * existing edge).
	 * <p>
	 * This method does NOT maintain the Delaunay condition. If desired, this must
	 * be checked and enforced by the caller.
	 * <p>
	 * This method does NOT check if the inserted vertex falls on an edge. This
	 * must be checked by the caller, since this situation may cause erroneous
	 * triangulation
	 * 
	 * @param v
	 *          the vertex to insert
	 * @return a new quad edge terminating in v
	 */
	public QuadEdge insertSite(Vertex v) {
		QuadEdge e = locate(v);

		if ((v.equals(e.orig(), tolerance)) || (v.equals(e.dest(), tolerance))) {
			return e; // point already in subdivision.
		}

		// Connect the new point to the vertices of the containing
		// triangle (or quadrilateral, if the new point fell on an
		// existing edge.)
		QuadEdge base = makeEdge(e.orig(), v);
		QuadEdge.splice(base, e);
		QuadEdge startEdge = base;
		do {
			base = connect(e, base.sym());
			e = base.oPrev();
		} while (e.lNext() != startEdge);

		return startEdge;
	}

	/**
	 * Tests whether a QuadEdge is an edge incident on a frame triangle vertex.
	 * 
	 * @param e
	 *          the edge to test
	 * @return true if the edge is connected to the frame triangle
	 */
	public boolean isFrameEdge(QuadEdge e) {
		if (isFrameVertex(e.orig()) || isFrameVertex(e.dest()))
			return true;
		return false;
	}

	/**
	 * Tests whether a QuadEdge is an edge on the border of the frame facets and
	 * the internal facets. E.g. an edge which does not itself touch a frame
	 * vertex, but which touches an edge which does.
	 * 
	 * @param e
	 *          the edge to test
	 * @return true if the edge is on the border of the frame
	 */
	public boolean isFrameBorderEdge(QuadEdge e) {
		// MD debugging
		QuadEdge[] leftTri = new QuadEdge[3];
		getTriangleEdges(e, leftTri);
		// System.out.println(new QuadEdgeTriangle(leftTri).toString());
		QuadEdge[] rightTri = new QuadEdge[3];
		getTriangleEdges(e.sym(), rightTri);
		// System.out.println(new QuadEdgeTriangle(rightTri).toString());

		// check other vertex of triangle to left of edge
		Vertex vLeftTriOther = e.lNext().dest();
		if (isFrameVertex(vLeftTriOther))
			return true;
		// check other vertex of triangle to right of edge
		Vertex vRightTriOther = e.sym().lNext().dest();
		if (isFrameVertex(vRightTriOther))
			return true;

		return false;
	}

	/**
	 * Tests whether a vertex is a vertex of the outer triangle.
	 * 
	 * @param v
	 *          the vertex to test
	 * @return true if the vertex is an outer triangle vertex
	 */
	public boolean isFrameVertex(Vertex v) {
		if (v.equals(frameVertex[0]))
			return true;
		if (v.equals(frameVertex[1]))
			return true;
		if (v.equals(frameVertex[2]))
			return true;
		return false;
	}

	private LineSegment seg = new LineSegment();

	/**
	 * Tests whether a {@link Coordinate} lies on a {@link QuadEdge}, up to a
	 * tolerance determined by the subdivision tolerance.
	 * 
	 * @param e
	 *          a QuadEdge
	 * @param p
	 *          a point
	 * @return true if the vertex lies on the edge
	 */
	public boolean isOnEdge(QuadEdge e, Coordinate p) {
		seg.setCoordinates(e.orig().getCoordinate(), e.dest().getCoordinate());
		double dist = seg.distance(p);
		// heuristic (hack?)
		return dist < edgeCoincidenceTolerance;
	}

	/**
	 * Tests whether a {@link Vertex} is the start or end vertex of a
	 * {@link QuadEdge}, up to the subdivision tolerance distance.
	 * 
	 * @param e
	 * @param v
	 * @return true if the vertex is a endpoint of the edge
	 */
	public boolean isVertexOfEdge(QuadEdge e, Vertex v) {
		if ((v.equals(e.orig(), tolerance)) || (v.equals(e.dest(), tolerance))) {
			return true;
		}
		return false;
	}

  /**
   * Gets the unique {@link Vertex}es in the subdivision,
   * including the frame vertices if desired.
   * 
	 * @param includeFrame
	 *          true if the frame vertices should be included
   * @return a collection of the subdivision vertices
   * 
   * @see #getVertexUniqueEdges
   */
  public Collection getVertices(boolean includeFrame) 
  {
    Set vertices = new HashSet();
    for (Iterator i = quadEdges.iterator(); i.hasNext();) {
      QuadEdge qe = (QuadEdge) i.next();
      Vertex v = qe.orig();
      //System.out.println(v);
      if (includeFrame || ! isFrameVertex(v))
        vertices.add(v);
      
      /**
       * Inspect the sym edge as well, since it is
       * possible that a vertex is only at the 
       * dest of all tracked quadedges.
       */
      Vertex vd = qe.dest();
      //System.out.println(vd);
      if (includeFrame || ! isFrameVertex(vd))
        vertices.add(vd);
    }
    return vertices;
  }

  /**
   * Gets a collection of {@link QuadEdge}s whose origin
   * vertices are a unique set which includes
   * all vertices in the subdivision. 
   * The frame vertices can be included if required.
   * <p>
   * This is useful for algorithms which require traversing the 
   * subdivision starting at all vertices.
   * Returning a quadedge for each vertex
   * is more efficient than 
   * the alternative of finding the actual vertices
   * using {@link #getVertices} and then locating 
   * quadedges attached to them.
   * 
   * @param includeFrame true if the frame vertices should be included
   * @return a collection of QuadEdge with the vertices of the subdivision as their origins
   */
  public List getVertexUniqueEdges(boolean includeFrame) 
  {
  	List edges = new ArrayList();
    Set visitedVertices = new HashSet();
    for (Iterator i = quadEdges.iterator(); i.hasNext();) {
      QuadEdge qe = (QuadEdge) i.next();
      Vertex v = qe.orig();
      //System.out.println(v);
      if (! visitedVertices.contains(v)) {
      	visitedVertices.add(v);
        if (includeFrame || ! isFrameVertex(v)) {
        	edges.add(qe);
        }
      }
      
      /**
       * Inspect the sym edge as well, since it is
       * possible that a vertex is only at the 
       * dest of all tracked quadedges.
       */
      QuadEdge qd = qe.sym();
      Vertex vd = qd.orig();
      //System.out.println(vd);
      if (! visitedVertices.contains(vd)) {
      	visitedVertices.add(vd);
        if (includeFrame || ! isFrameVertex(vd)) {
        	edges.add(qd);
        }
      }
    }
    return edges;
  }

	/**
	 * Gets all primary quadedges in the subdivision. 
   * A primary edge is a {@link QuadEdge}
	 * which occupies the 0'th position in its array of associated quadedges. 
	 * These provide the unique geometric edges of the triangulation.
	 * 
	 * @param includeFrame true if the frame edges are to be included
	 * @return a List of QuadEdges
	 */
	public List getPrimaryEdges(boolean includeFrame) {
		visitedKey++;

		List edges = new ArrayList();
		Stack edgeStack = new Stack();
		edgeStack.push(startingEdge);
		
		Set visitedEdges = new HashSet();

		while (!edgeStack.empty()) {
			QuadEdge edge = (QuadEdge) edgeStack.pop();
			if (! visitedEdges.contains(edge)) {
				QuadEdge priQE = edge.getPrimary();

				if (includeFrame || ! isFrameEdge(priQE))
					edges.add(priQE);

				edgeStack.push(edge.oNext());
				edgeStack.push(edge.sym().oNext());
				
				visitedEdges.add(edge);
				visitedEdges.add(edge.sym());
			}
		}
		return edges;
	}
  
  /**
   * A TriangleVisitor which computes and sets the 
   * circumcentre as the origin of the dual 
   * edges originating in each triangle.
   * 
   * @author mbdavis
   *
   */
	private static class TriangleCircumcentreVisitor implements TriangleVisitor 
	{
		public TriangleCircumcentreVisitor() {
		}

		public void visit(QuadEdge[] triEdges) 
		{
			Coordinate a = triEdges[0].orig().getCoordinate();
			Coordinate b = triEdges[1].orig().getCoordinate();
			Coordinate c = triEdges[2].orig().getCoordinate();
			
			// TODO: choose the most accurate circumcentre based on the edges
      Coordinate cc = Triangle.circumcentre(a, b, c);
			Vertex ccVertex = new Vertex(cc);
			// save the circumcentre as the origin for the dual edges originating in this triangle
			for (int i = 0; i < 3; i++) {
				triEdges[i].rot().setOrig(ccVertex);
			}
		}
	}

	/*****************************************************************************
	 * Visitors
	 ****************************************************************************/

	public void visitTriangles(TriangleVisitor triVisitor,
			boolean includeFrame) {
		visitedKey++;

		// visited flag is used to record visited edges of triangles
		// setVisitedAll(false);
		Stack edgeStack = new Stack();
		edgeStack.push(startingEdge);

		Set visitedEdges = new HashSet();
		
		while (!edgeStack.empty()) {
			QuadEdge edge = (QuadEdge) edgeStack.pop();
			if (! visitedEdges.contains(edge)) {
				QuadEdge[] triEdges = fetchTriangleToVisit(edge, edgeStack,
						includeFrame, visitedEdges);
				if (triEdges != null)
					triVisitor.visit(triEdges);
			}
		}
	}

	/**
	 * The quadedges forming a single triangle.
   * Only one visitor is allowed to be active at a
	 * time, so this is safe.
	 */
	private QuadEdge[] triEdges = new QuadEdge[3];

	/**
	 * Stores the edges for a visited triangle. Also pushes sym (neighbour) edges
	 * on stack to visit later.
	 * 
	 * @param edge
	 * @param edgeStack
	 * @param includeFrame
	 * @return the visited triangle edges
	 * or null if the triangle should not be visited (for instance, if it is
	 *         outer)
	 */
	private QuadEdge[] fetchTriangleToVisit(QuadEdge edge, Stack edgeStack,
			boolean includeFrame, Set visitedEdges) {
		QuadEdge curr = edge;
		int edgeCount = 0;
		boolean isFrame = false;
		do {
			triEdges[edgeCount] = curr;

			if (isFrameEdge(curr))
				isFrame = true;
			
			// push sym edges to visit next
			QuadEdge sym = curr.sym();
			if (! visitedEdges.contains(sym))
				edgeStack.push(sym);
			
			// mark this edge as visited
			visitedEdges.add(curr);
			
			edgeCount++;
			curr = curr.lNext();
		} while (curr != edge);

		if (isFrame && !includeFrame)
			return null;
		return triEdges;
	}

	/**
	 * Gets a list of the triangles
	 * in the subdivision, specified as
	 * an array of the primary quadedges around the triangle.
	 * 
	 * @param includeFrame
	 *          true if the frame triangles should be included
	 * @return a List of QuadEdge[3] arrays
	 */
	public List getTriangleEdges(boolean includeFrame) {
		TriangleEdgesListVisitor visitor = new TriangleEdgesListVisitor();
		visitTriangles(visitor, includeFrame);
		return visitor.getTriangleEdges();
	}

	private static class TriangleEdgesListVisitor implements TriangleVisitor {
		private List triList = new ArrayList();

		public void visit(QuadEdge[] triEdges) {
			triList.add(triEdges.clone());
		}

		public List getTriangleEdges() {
			return triList;
		}
	}

	/**
	 * Gets a list of the triangles in the subdivision,
	 * specified as an array of the triangle {@link Vertex}es.
	 * 
	 * @param includeFrame
	 *          true if the frame triangles should be included
	 * @return a List of Vertex[3] arrays
	 */
	public List getTriangleVertices(boolean includeFrame) {
		TriangleVertexListVisitor visitor = new TriangleVertexListVisitor();
		visitTriangles(visitor, includeFrame);
		return visitor.getTriangleVertices();
	}

	private static class TriangleVertexListVisitor implements TriangleVisitor {
		private List triList = new ArrayList();

		public void visit(QuadEdge[] triEdges) {
			triList.add(new Vertex[] { triEdges[0].orig(), triEdges[1].orig(),
					triEdges[2].orig() });
		}

		public List getTriangleVertices() {
			return triList;
		}
	}

	/**
	 * Gets the coordinates for each triangle in the subdivision as an array.
	 * 
	 * @param includeFrame
	 *          true if the frame triangles should be included
	 * @return a list of Coordinate[4] representing each triangle
	 */
	public List getTriangleCoordinates(boolean includeFrame) {
		TriangleCoordinatesVisitor visitor = new TriangleCoordinatesVisitor();
		visitTriangles(visitor, includeFrame);
		return visitor.getTriangles();
	}

	private static class TriangleCoordinatesVisitor implements TriangleVisitor {
		private CoordinateList coordList = new CoordinateList();

		private List triCoords = new ArrayList();

		public TriangleCoordinatesVisitor() {
		}

		public void visit(QuadEdge[] triEdges) {
			coordList.clear();
			for (int i = 0; i < 3; i++) {
				Vertex v = triEdges[i].orig();
				coordList.add(v.getCoordinate());
			}
			if (coordList.size() > 0) {
				coordList.closeRing();
				Coordinate[] pts = coordList.toCoordinateArray();
				if (pts.length != 4) {
					//checkTriangleSize(pts);
					return;
				}

				triCoords.add(pts);
			}
		}

		private void checkTriangleSize(Coordinate[] pts)
		{
			String loc = "";
			if (pts.length >= 2)
				loc = WKTWriter.toLineString(pts[0], pts[1]);
			else {
				if (pts.length >= 1)
					loc = WKTWriter.toPoint(pts[0]);
			}
			// Assert.isTrue(pts.length == 4, "Too few points for visited triangle at " + loc);
			//com.vividsolutions.jts.util.Debug.println("too few points for triangle at " + loc);
		}
		
		public List getTriangles() {
			return triCoords;
		}
	}

	/**
	 * Gets the geometry for the edges in the subdivision as a {@link MultiLineString}
	 * containing 2-point lines.
	 * 
	 * @param geomFact the GeometryFactory to use
	 * @return a MultiLineString
	 */
	public Geometry getEdges(GeometryFactory geomFact) {
		List quadEdges = getPrimaryEdges(false);
		LineString[] edges = new LineString[quadEdges.size()];
		int i = 0;
		for (Iterator it = quadEdges.iterator(); it.hasNext();) {
			QuadEdge qe = (QuadEdge) it.next();
			edges[i++] = geomFact.createLineString(new Coordinate[] {
					qe.orig().getCoordinate(), qe.dest().getCoordinate() });
		}
		return geomFact.createMultiLineString(edges);
	}

	/**
	 * Gets the geometry for the triangles in a triangulated subdivision as a {@link GeometryCollection}
	 * of triangular {@link Polygon}s.
	 * 
	 * @param geomFact the GeometryFactory to use
	 * @return a GeometryCollection of triangular Polygons
	 */
	public Geometry getTriangles(GeometryFactory geomFact) {
		List triPtsList = getTriangleCoordinates(false);
		Polygon[] tris = new Polygon[triPtsList.size()];
		int i = 0;
		for (Iterator it = triPtsList.iterator(); it.hasNext();) {
			Coordinate[] triPt = (Coordinate[]) it.next();
			tris[i++] = geomFact
					.createPolygon(geomFact.createLinearRing(triPt), null);
		}
		return geomFact.createGeometryCollection(tris);
	}

	/**
	 * Gets the cells in the Voronoi diagram for this triangulation.
	 * The cells are returned as a {@link GeometryCollection} of {@link Polygon}s
   * <p>
   * The userData of each polygon is set to be the {@link Coordinate}
   * of the cell site.  This allows easily associating external 
   * data associated with the sites to the cells.
	 * 
	 * @param geomFact a geometry factory
	 * @return a GeometryCollection of Polygons
	 */
  public Geometry getVoronoiDiagram(GeometryFactory geomFact)
  {
    List vorCells = getVoronoiCellPolygons(geomFact);
    return geomFact.createGeometryCollection(GeometryFactory.toGeometryArray(vorCells));   
  }
  
	/**
	 * Gets a List of {@link Polygon}s for the Voronoi cells 
	 * of this triangulation.
   * <p>
   * The userData of each polygon is set to be the {@link Coordinate}
   * of the cell site.  This allows easily associating external 
   * data associated with the sites to the cells.
	 * 
	 * @param geomFact a geometry factory
	 * @return a List of Polygons
	 */
  public List getVoronoiCellPolygons(GeometryFactory geomFact)
  {
  	/*
  	 * Compute circumcentres of triangles as vertices for dual edges.
  	 * Precomputing the circumcentres is more efficient, 
  	 * and more importantly ensures that the computed centres
  	 * are consistent across the Voronoi cells.
  	 */ 
  	visitTriangles(new TriangleCircumcentreVisitor(), true);
  	
    List cells = new ArrayList();
    Collection edges = getVertexUniqueEdges(false);
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
    	QuadEdge qe = (QuadEdge) i.next();
      cells.add(getVoronoiCellPolygon(qe, geomFact));
    }
    return cells;
  }
  
  /**
   * Gets the Voronoi cell around a site specified
   * by the origin of a QuadEdge.
   * <p>
   * The userData of the polygon is set to be the {@link Coordinate}
   * of the site.  This allows attaching external 
   * data associated with the site to this cell polygon.
   * 
   * @param qe a quadedge originating at the cell site
   * @param geomFact a factory for building the polygon
   * @return a polygon indicating the cell extent
   */
  public Polygon getVoronoiCellPolygon(QuadEdge qe, GeometryFactory geomFact)
  {
    List cellPts = new ArrayList();
    QuadEdge startQE = qe;
    do {
//    	Coordinate cc = circumcentre(qe);
    	// use previously computed circumcentre
    	Coordinate cc = qe.rot().orig().getCoordinate();
      cellPts.add(cc);
      
      // move to next triangle CW around vertex
      qe = qe.oPrev();
    } while (qe != startQE);
    
    CoordinateList coordList = new CoordinateList();
    coordList.addAll(cellPts, false);
    coordList.closeRing();
    
    if (coordList.size() < 4) {
      System.out.println(coordList);
      coordList.add(coordList.get(coordList.size()-1), true);
    }
    
    Coordinate[] pts = coordList.toCoordinateArray();
    Polygon cellPoly = geomFact.createPolygon(geomFact.createLinearRing(pts), null);
    
    Vertex v = startQE.orig();
    cellPoly.setUserData(v.getCoordinate());
    return cellPoly;
  }
  
}