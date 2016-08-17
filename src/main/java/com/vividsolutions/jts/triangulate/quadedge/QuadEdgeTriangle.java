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

import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Models a triangle formed from {@link QuadEdge}s in a {@link QuadEdgeSubdivision}
 * which forms a triangulation. The class provides methods to access the
 * topological and geometric properties of the triangle and its neighbours in
 * the triangulation. Triangle vertices are ordered in CCW orientation in the
 * structure.
 * <p>
 * QuadEdgeTriangles support having an external data attribute attached to them.
 * Alternatively, this class can be subclassed and attributes can 
 * be defined in the subclass.  Subclasses will need to define 
 * their own <tt>BuilderVisitor</tt> class
 * and <tt>createOn</tt> method.
 * 
 * @author Martin Davis
 * @version 1.0
 */
public class QuadEdgeTriangle 
{
	/**
	 * Creates {@link QuadEdgeTriangle}s for all facets of a 
	 * {@link QuadEdgeSubdivision} representing a triangulation.
	 * The <tt>data</tt> attributes of the {@link QuadEdge}s in the subdivision
	 * will be set to point to the triangle which contains that edge.
	 * This allows tracing the neighbour triangles of any given triangle.
	 * 
	 * @param subdiv
	 * 				the QuadEdgeSubdivision to create the triangles on.
	 * @return a List of the created QuadEdgeTriangles
	 */
	public static List createOn(QuadEdgeSubdivision subdiv)
	{
		QuadEdgeTriangleBuilderVisitor visitor = new QuadEdgeTriangleBuilderVisitor();
		subdiv.visitTriangles(visitor, false);
		return visitor.getTriangles();
	}

	/**
	 * Tests whether the point pt is contained in the triangle defined by 3
	 * {@link Vertex}es.
	 * 
	 * @param tri
	 *          an array containing at least 3 Vertexes
	 * @param pt
	 *          the point to test
	 * @return true if the point is contained in the triangle
	 */
	public static boolean contains(Vertex[] tri, Coordinate pt) {
		Coordinate[] ring = new Coordinate[] { tri[0].getCoordinate(),
				tri[1].getCoordinate(), tri[2].getCoordinate(), tri[0].getCoordinate() };
		return CGAlgorithms.isPointInRing(pt, ring);
	}

	/**
	 * Tests whether the point pt is contained in the triangle defined by 3
	 * {@link QuadEdge}es.
	 * 
	 * @param tri
	 *          an array containing at least 3 QuadEdges
	 * @param pt
	 *          the point to test
	 * @return true if the point is contained in the triangle
	 */
	public static boolean contains(QuadEdge[] tri, Coordinate pt) {
		Coordinate[] ring = new Coordinate[] { tri[0].orig().getCoordinate(),
				tri[1].orig().getCoordinate(), tri[2].orig().getCoordinate(),
				tri[0].orig().getCoordinate() };
		return CGAlgorithms.isPointInRing(pt, ring);
	}

	public static Geometry toPolygon(Vertex[] v) {
		Coordinate[] ringPts = new Coordinate[] { v[0].getCoordinate(),
				v[1].getCoordinate(), v[2].getCoordinate(), v[0].getCoordinate() };
		GeometryFactory fact = new GeometryFactory();
		LinearRing ring = fact.createLinearRing(ringPts);
		Polygon tri = fact.createPolygon(ring, null);
		return tri;
	}

	public static Geometry toPolygon(QuadEdge[] e) {
		Coordinate[] ringPts = new Coordinate[] { e[0].orig().getCoordinate(),
				e[1].orig().getCoordinate(), e[2].orig().getCoordinate(),
				e[0].orig().getCoordinate() };
		GeometryFactory fact = new GeometryFactory();
		LinearRing ring = fact.createLinearRing(ringPts);
		Polygon tri = fact.createPolygon(ring, null);
		return tri;
	}

	/**
	 * Finds the next index around the triangle. Index may be an edge or vertex
	 * index.
	 * 
	 * @param index
	 * @return the next index
	 */
	public static int nextIndex(int index) {
		return index = (index + 1) % 3;
	}

	private QuadEdge[] edge;
	private Object data;

	/**
	 * Creates a new triangle from the given edges.
	 * 
	 * @param edge an array of the edges of the triangle in CCW order
	 */
	public QuadEdgeTriangle(QuadEdge[] edge) {
		this.edge = (QuadEdge[]) edge.clone();
		// link the quadedges back to this triangle
    for (int i = 0; i < 3; i++) {
      edge[i].setData(this);
    }
	}

  /**
   * Sets the external data value for this triangle.
   * 
   * @param data an object containing external data
   */
  public void setData(Object data) {
      this.data = data;
  }
  
  /**
   * Gets the external data value for this triangle.
   * 
   * @return the data object
   */
  public Object getData() {
      return data;
  }

	public void kill() {
		edge = null;
	}

	public boolean isLive() {
		return edge != null;
	}

	public QuadEdge[] getEdges() {
		return edge;
	}

	public QuadEdge getEdge(int i) {
		return edge[i];
	}

	public Vertex getVertex(int i) {
		return edge[i].orig();
	}

	/**
	 * Gets the vertices for this triangle.
	 * 
	 * @return a new array containing the triangle vertices
	 */
	public Vertex[] getVertices() {
		Vertex[] vert = new Vertex[3];
		for (int i = 0; i < 3; i++) {
			vert[i] = getVertex(i);
		}
		return vert;
	}

	public Coordinate getCoordinate(int i) {
		return edge[i].orig().getCoordinate();
	}

	/**
	 * Gets the index for the given edge of this triangle
	 * 
	 * @param e
	 *          a QuadEdge
	 * @return the index of the edge in this triangle
	 * or -1 if the edge is not an edge of this triangle
	 */
	public int getEdgeIndex(QuadEdge e) {
		for (int i = 0; i < 3; i++) {
			if (edge[i] == e)
				return i;
		}
		return -1;
	}

	/**
	 * Gets the index for the edge that starts at vertex v.
	 * 
	 * @param v
	 *          the vertex to find the edge for
	 * @return the index of the edge starting at the vertex
	 * or -1 if the vertex is not in the triangle
	 */
	public int getEdgeIndex(Vertex v) {
		for (int i = 0; i < 3; i++) {
			if (edge[i].orig() == v)
				return i;
		}
		return -1;
	}

	public void getEdgeSegment(int i, LineSegment seg) {
		seg.p0 = edge[i].orig().getCoordinate();
		int nexti = (i + 1) % 3;
		seg.p1 = edge[nexti].orig().getCoordinate();
	}

	public Coordinate[] getCoordinates() {
		Coordinate[] pts = new Coordinate[4];
		for (int i = 0; i < 3; i++) {
			pts[i] = edge[i].orig().getCoordinate();
		}
		pts[3] = new Coordinate(pts[0]);
		return pts;
	}

	public boolean contains(Coordinate pt) {
		Coordinate[] ring = getCoordinates();
		return CGAlgorithms.isPointInRing(pt, ring);
	}

	public Polygon getGeometry(GeometryFactory fact) {
		LinearRing ring = fact.createLinearRing(getCoordinates());
		Polygon tri = fact.createPolygon(ring, null);
		return tri;
	}

	public String toString() {
		return getGeometry(new GeometryFactory()).toString();
	}

	/**
	 * Tests whether this triangle is adjacent to the outside of the subdivision.
	 * 
	 * @return true if the triangle is adjacent to the subdivision exterior
	 */
	public boolean isBorder() {
		for (int i = 0; i < 3; i++) {
			if (getAdjacentTriangleAcrossEdge(i) == null)
				return true;
		}
		return false;
	}

	public boolean isBorder(int i) {
		return getAdjacentTriangleAcrossEdge(i) == null;
	}

	public QuadEdgeTriangle getAdjacentTriangleAcrossEdge(int edgeIndex) {
		return (QuadEdgeTriangle) getEdge(edgeIndex).sym().getData();
	}

	public int getAdjacentTriangleEdgeIndex(int i) {
		return getAdjacentTriangleAcrossEdge(i).getEdgeIndex(getEdge(i).sym());
	}

	/**
	 * Gets the triangles which are adjacent (include) to a 
	 * given vertex of this triangle.
	 * 
	 * @param vertexIndex the vertex to query
	 * @return a list of the vertex-adjacent triangles
	 */
	public List getTrianglesAdjacentToVertex(int vertexIndex) {
		// Assert: isVertex
		List adjTris = new ArrayList();

		QuadEdge start = getEdge(vertexIndex);
		QuadEdge qe = start;
		do {
			QuadEdgeTriangle adjTri = (QuadEdgeTriangle) qe.getData();
			if (adjTri != null) {
				adjTris.add(adjTri);
			}
			qe = qe.oNext();
		} while (qe != start);

		return adjTris;

	}

	/**
	 * Gets the neighbours of this triangle. If there is no neighbour triangle,
	 * the array element is <code>null</code>
	 * 
	 * @return an array containing the 3 neighbours of this triangle
	 */
	public QuadEdgeTriangle[] getNeighbours() {
		QuadEdgeTriangle[] neigh = new QuadEdgeTriangle[3];
		for (int i = 0; i < 3; i++) {
			neigh[i] = (QuadEdgeTriangle) getEdge(i).sym().getData();
		}
		return neigh;
	}

	private static class QuadEdgeTriangleBuilderVisitor implements TriangleVisitor {
		private List triangles = new ArrayList();

		public QuadEdgeTriangleBuilderVisitor() {
		}

		public void visit(QuadEdge[] edges) {
			triangles.add(new QuadEdgeTriangle(edges));
		}

		public List getTriangles() {
			return triangles;
		}
	}
}


