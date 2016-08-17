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
 * A utility class which creates Delaunay Trianglulations
 * from collections of points and extract the resulting 
 * triangulation edges or triangles as geometries. 
 * 
 * @author Martin Davis
 *
 */
public class DelaunayTriangulationBuilder 
{
	/**
	 * Extracts the unique {@link Coordinate}s from the given {@link Geometry}.
	 * @param geom the geometry to extract from
	 * @return a List of the unique Coordinates
	 */
	public static CoordinateList extractUniqueCoordinates(Geometry geom)
	{
		if (geom == null)
			return new CoordinateList();
		
		Coordinate[] coords = geom.getCoordinates();
		return unique(coords);
	}
	
	public static CoordinateList unique(Coordinate[] coords)
	{
	  Coordinate[] coordsCopy = CoordinateArrays.copyDeep(coords);
		Arrays.sort(coordsCopy);
		CoordinateList coordList = new CoordinateList(coordsCopy, false);
		return coordList;
	}
	
	/**
	 * Converts all {@link Coordinate}s in a collection to {@link Vertex}es.
	 * @param coords the coordinates to convert
	 * @return a List of Vertex objects
	 */
	public static List toVertices(Collection coords)
	{
		List verts = new ArrayList();
		for (Iterator i = coords.iterator(); i.hasNext(); ) {
			Coordinate coord = (Coordinate) i.next();
			verts.add(new Vertex(coord));
		}
		return verts;
	}

	/**
	 * Computes the {@link Envelope} of a collection of {@link Coordinate}s.
	 * 
	 * @param coords a List of Coordinates
	 * @return the envelope of the set of coordinates
	 */
	public static Envelope envelope(Collection coords)
	{
		Envelope env = new Envelope();
		for (Iterator i = coords.iterator(); i.hasNext(); ) {
			Coordinate coord = (Coordinate) i.next();
			env.expandToInclude(coord);
		}
		return env;
	}
	
	private Collection siteCoords;
	private double tolerance = 0.0;
	private QuadEdgeSubdivision subdiv = null;
	
	/**
	 * Creates a new triangulation builder.
	 *
	 */
	public DelaunayTriangulationBuilder()
	{
	}
	
	/**
	 * Sets the sites (vertices) which will be triangulated.
	 * All vertices of the given geometry will be used as sites.
	 * 
	 * @param geom the geometry from which the sites will be extracted.
	 */
	public void setSites(Geometry geom)
	{
		// remove any duplicate points (they will cause the triangulation to fail)
		siteCoords = extractUniqueCoordinates(geom);
	}
	
	/**
	 * Sets the sites (vertices) which will be triangulated
	 * from a collection of {@link Coordinate}s.
	 * 
	 * @param coords a collection of Coordinates.
	 */
	public void setSites(Collection coords)
	{
		// remove any duplicate points (they will cause the triangulation to fail)
		siteCoords = unique(CoordinateArrays.toCoordinateArray(coords));
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
		
		Envelope siteEnv = envelope(siteCoords);
		List vertices = toVertices(siteCoords);
		subdiv = new QuadEdgeSubdivision(siteEnv, tolerance);
		IncrementalDelaunayTriangulator triangulator = new IncrementalDelaunayTriangulator(subdiv);
		triangulator.insertSites(vertices);
	}
	
	/**
	 * Gets the {@link QuadEdgeSubdivision} which models the computed triangulation.
	 * 
	 * @return the subdivision containing the triangulation
	 */
	public QuadEdgeSubdivision getSubdivision()
	{
		create();
		return subdiv;
	}
	
	/**
	 * Gets the edges of the computed triangulation as a {@link MultiLineString}.
	 * 
	 * @param geomFact the geometry factory to use to create the output
	 * @return the edges of the triangulation
	 */
	public Geometry getEdges(GeometryFactory geomFact)
	{
		create();
		return subdiv.getEdges(geomFact);
	}
	
	/**
	 * Gets the faces of the computed triangulation as a {@link GeometryCollection} 
	 * of {@link Polygon}.
	 * 
	 * @param geomFact the geometry factory to use to create the output
	 * @return the faces of the triangulation
	 */
	public Geometry getTriangles(GeometryFactory geomFact)
	{
		create();
		return subdiv.getTriangles(geomFact);
	}
}
