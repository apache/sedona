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

package com.vividsolutions.jts.triangulate;

import java.util.Collection;
import java.util.Iterator;

import com.vividsolutions.jts.triangulate.quadedge.LocateFailureException;
import com.vividsolutions.jts.triangulate.quadedge.QuadEdge;
import com.vividsolutions.jts.triangulate.quadedge.QuadEdgeSubdivision;
import com.vividsolutions.jts.triangulate.quadedge.Vertex;

/**
 * Computes a Delauanay Triangulation of a set of {@link Vertex}es, using an
 * incrementatal insertion algorithm.
 * 
 * @author Martin Davis
 * @version 1.0
 */
public class IncrementalDelaunayTriangulator 
{
	private QuadEdgeSubdivision subdiv;
	private boolean isUsingTolerance = false;

	/**
	 * Creates a new triangulator using the given {@link QuadEdgeSubdivision}.
	 * The triangulator uses the tolerance of the supplied subdivision.
	 * 
	 * @param subdiv
	 *          a subdivision in which to build the TIN
	 */
	public IncrementalDelaunayTriangulator(QuadEdgeSubdivision subdiv) {
		this.subdiv = subdiv;
		isUsingTolerance = subdiv.getTolerance() > 0.0;
		
	}

	/**
	 * Inserts all sites in a collection. The inserted vertices <b>MUST</b> be
	 * unique up to the provided tolerance value. (i.e. no two vertices should be
	 * closer than the provided tolerance value). They do not have to be rounded
	 * to the tolerance grid, however.
	 * 
	 * @param vertices a Collection of Vertex
	 * 
   * @throws LocateFailureException if the location algorithm fails to converge in a reasonable number of iterations
	 */
	public void insertSites(Collection vertices) {
		for (Iterator i = vertices.iterator(); i.hasNext();) {
			Vertex v = (Vertex) i.next();
			insertSite(v);
		}
	}

	/**
	 * Inserts a new point into a subdivision representing a Delaunay
	 * triangulation, and fixes the affected edges so that the result is still a
	 * Delaunay triangulation.
	 * <p>
	 * 
	 * @return a quadedge containing the inserted vertex
	 */
	public QuadEdge insertSite(Vertex v) {

		/**
		 * This code is based on Guibas and Stolfi (1985), with minor modifications
		 * and a bug fix from Dani Lischinski (Graphic Gems 1993). (The modification
		 * I believe is the test for the inserted site falling exactly on an
		 * existing edge. Without this test zero-width triangles have been observed
		 * to be created)
		 */
		QuadEdge e = subdiv.locate(v);

		if (subdiv.isVertexOfEdge(e, v)) {
			// point is already in subdivision.
			return e; 
		} 
		else if (subdiv.isOnEdge(e, v.getCoordinate())) {
			// the point lies exactly on an edge, so delete the edge 
			// (it will be replaced by a pair of edges which have the point as a vertex)
			e = e.oPrev();
			subdiv.delete(e.oNext());
		}

		/**
		 * Connect the new point to the vertices of the containing triangle 
		 * (or quadrilateral, if the new point fell on an existing edge.)
		 */
		QuadEdge base = subdiv.makeEdge(e.orig(), v);
		QuadEdge.splice(base, e);
		QuadEdge startEdge = base;
		do {
			base = subdiv.connect(e, base.sym());
			e = base.oPrev();
		} while (e.lNext() != startEdge);

		// Examine suspect edges to ensure that the Delaunay condition
		// is satisfied.
		do {
			QuadEdge t = e.oPrev();
			if (t.dest().rightOf(e) && v.isInCircle(e.orig(), t.dest(), e.dest())) {
				QuadEdge.swap(e);
				e = e.oPrev();
			} else if (e.oNext() == startEdge) {
				return base; // no more suspect edges.
			} else {
				e = e.oNext().lPrev();
			}
		} while (true);
	}

}