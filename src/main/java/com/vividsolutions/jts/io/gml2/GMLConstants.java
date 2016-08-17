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
package com.vividsolutions.jts.io.gml2;

/**
 * Various constant strings associated with GML format.
 */
final public class GMLConstants{
	
	  // Namespace constants
	  public static final String GML_NAMESPACE = "http://www.opengis.net/gml";
	  public static final String GML_PREFIX = "gml";

	  // Source Coordinate System
	  public static final String GML_ATTR_SRSNAME = "srsName";

	  // GML associative types
	  public static final String GML_GEOMETRY_MEMBER = "geometryMember";
	  public static final String GML_POINT_MEMBER = "pointMember";
	  public static final String GML_POLYGON_MEMBER = "polygonMember";
	  public static final String GML_LINESTRING_MEMBER = "lineStringMember";
	  public static final String GML_OUTER_BOUNDARY_IS = "outerBoundaryIs";
	  public static final String GML_INNER_BOUNDARY_IS = "innerBoundaryIs";

	  // Primitive Geometries
	  public static final String GML_POINT = "Point";
	  public static final String GML_LINESTRING = "LineString";
	  public static final String GML_LINEARRING = "LinearRing";
	  public static final String GML_POLYGON = "Polygon";
	  public static final String GML_BOX = "Box";

	  // Aggregate Geometries
	  public static final String GML_MULTI_GEOMETRY = "MultiGeometry";
	  public static final String GML_MULTI_POINT = "MultiPoint";
	  public static final String GML_MULTI_LINESTRING = "MultiLineString";
	  public static final String GML_MULTI_POLYGON = "MultiPolygon";

	  // Coordinates
	  public static final String GML_COORDINATES = "coordinates";
	  public static final String GML_COORD = "coord";
	  public static final String GML_COORD_X = "X";
	  public static final String GML_COORD_Y = "Y";
	  public static final String GML_COORD_Z = "Z";
}