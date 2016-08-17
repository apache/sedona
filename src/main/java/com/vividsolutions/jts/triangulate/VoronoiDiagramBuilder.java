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

import java.util.*;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.triangulate.quadedge.*;

/**
 * A utility class which creates Voronoi Diagrams
 * from collections of points.
 * The diagram is returned as a {@link GeometryCollection} of {@link Polygon}s
 * representing the faces of the Voronoi diagram.
 * The faces are clipped to the larger of:
 * <ul>
 * <li> an envelope supplied by {@link #setClipEnvelope(Envelope)}
 * <li> an envelope determined by the input sites
 * </ul>
 * The <tt>userData</tt> attribute of each face <tt>Polygon</tt> is set to 
 * the <tt>Coordinate</tt>  of the corresponding input site.
 * This allows using a <tt>Map</tt> to link faces to data associated with sites.
 * 
 * @author Martin Davis
 *
 */
public class VoronoiDiagramBuilder 
{
	private Collection siteCoords;
	private double tolerance = 0.0;
	private QuadEdgeSubdivision subdiv = null;
	private Envelope clipEnv = null;
	private Envelope diagramEnv = null; 
	
	/**
	 * Creates a new Voronoi diagram builder.
	 *
	 */
	public VoronoiDiagramBuilder()
	{
	}
	
	/**
	 * Sets the sites (point or vertices) which will be diagrammed.
	 * All vertices of the given geometry will be used as sites.
	 * 
	 * @param geom the geometry from which the sites will be extracted.
	 */
	public void setSites(Geometry geom)
	{
		// remove any duplicate points (they will cause the triangulation to fail)
		siteCoords = DelaunayTriangulationBuilder.extractUniqueCoordinates(geom);
	}
	
	/**
	 * Sets the sites (point or vertices) which will be diagrammed
	 * from a collection of {@link Coordinate}s.
	 * 
	 * @param coords a collection of Coordinates.
	 */
	public void setSites(Collection coords)
	{
		// remove any duplicate points (they will cause the triangulation to fail)
		siteCoords = DelaunayTriangulationBuilder.unique(CoordinateArrays.toCoordinateArray(coords));
	}
	
	/**
	 * Sets the envelope to clip the diagram to.
	 * The diagram will be clipped to the larger
	 * of this envelope or an envelope surrounding the sites.
	 * 
	 * @param clipEnv the clip envelope.
	 */
	public void setClipEnvelope(Envelope clipEnv)
	{
		this.clipEnv = clipEnv;
	}
	/**
	 * Sets the snapping tolerance which will be used
	 * to improved the robustness of the triangulation computation.
	 * A tolerance of 0.0 specifies that no snapping will take place.
	 * 
	 * @param tolerance the tolerance distance to use
	 */
	public void setTolerance(double tolerance)
	{
		this.tolerance = tolerance;
	}
	
	private void create()
	{
		if (subdiv != null) return;
		
		Envelope siteEnv = DelaunayTriangulationBuilder.envelope(siteCoords);
		diagramEnv = siteEnv;
		// add a buffer around the final envelope
		double expandBy = Math.max(diagramEnv.getWidth(), diagramEnv.getHeight());
		diagramEnv.expandBy(expandBy);
		if (clipEnv != null)
			diagramEnv.expandToInclude(clipEnv);
		
		List vertices = DelaunayTriangulationBuilder.toVertices(siteCoords);
		subdiv = new QuadEdgeSubdivision(siteEnv, tolerance);
		IncrementalDelaunayTriangulator triangulator = new IncrementalDelaunayTriangulator(subdiv);
		triangulator.insertSites(vertices);
	}
	
	/**
	 * Gets the {@link QuadEdgeSubdivision} which models the computed diagram.
	 * 
	 * @return the subdivision containing the triangulation
	 */
	public QuadEdgeSubdivision getSubdivision()
	{
		create();
		return subdiv;
	}
	
	/**
	 * Gets the faces of the computed diagram as a {@link GeometryCollection} 
	 * of {@link Polygon}s, clipped as specified.
	 * <p>
	 * The <tt>userData</tt> attribute of each face <tt>Polygon</tt> is set to 
	 * the <tt>Coordinate</tt>  of the corresponding input site.
	 * This allows using a <tt>Map</tt> to link faces to data associated with sites.
	 * 
	 * @param geomFact the geometry factory to use to create the output
	 * @return a <tt>GeometryCollection</tt> containg the face <tt>Polgyon</tt>s of the diagram
	 */
	public Geometry getDiagram(GeometryFactory geomFact)
	{
		create();
		Geometry polys = subdiv.getVoronoiDiagram(geomFact);
		
		// clip polys to diagramEnv
		return clipGeometryCollection(polys, diagramEnv);
	}
	
	private static Geometry clipGeometryCollection(Geometry geom, Envelope clipEnv)
	{
		Geometry clipPoly = geom.getFactory().toGeometry(clipEnv);
		List clipped = new ArrayList();
		for (int i = 0; i < geom.getNumGeometries(); i++) {
			Geometry g = geom.getGeometryN(i);
			Geometry result = null;
			// don't clip unless necessary
			if (clipEnv.contains(g.getEnvelopeInternal()))
					result = g;
			else if (clipEnv.intersects(g.getEnvelopeInternal())) {
				result = clipPoly.intersection(g);
				// keep vertex key info
				result.setUserData(g.getUserData());
			}

			if (result != null && ! result.isEmpty()) {
				clipped.add(result);
			}
		}
		return geom.getFactory().createGeometryCollection(GeometryFactory.toGeometryArray(clipped));
	}
}
