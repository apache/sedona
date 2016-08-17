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

import java.io.*;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.Assert;

/**
 * Writes {@link Geometry}s as XML fragments in GML2 format.
 * Allows specifying the XML prefix, namespace and srsName
 * of the emitted GML.
 * Also allows adding custom root elements
 * to support GML extensions such as KML. 
 * With appropriate settings for prefix (none) and custom root elements
 * this class can be used to write out geometry in KML format.
 * <p>
 * An example of the output that can be generated is:
 * 
 * <pre>
 * <gml:LineString xmlns:gml='http://www.opengis.net/gml' srsName='foo'>
 *   <gml:coordinates>
 *     6.03,8.17 7.697,6.959 8.333,5.0 7.697,3.041 6.03,1.83 3.97,1.83 2.303,3.041 1.667,5.0 2.303,6.959 3.97,8.17 
 *   </gml:coordinates>
 * </gml:LineString>
 * </pre>
 * 
 * <p>
 * This class does not rely on any external XML libraries. 
 *
 * @author David Zwiers, Vivid Solutions 
 * @author Martin Davis 
 */
public class GMLWriter {
	private final String INDENT = "  ";

	private int startingIndentIndex = 0;

	private int maxCoordinatesPerLine = 10;

	private boolean emitNamespace = false;

	private boolean isRootTag = false;

	private String prefix = GMLConstants.GML_PREFIX;
	private String namespace = GMLConstants.GML_NAMESPACE;
	private String srsName = null;
	
	private String[] customElements = null;
	
	/**
	 * Creates a writer which outputs GML with default settings.
	 * The defaults are:
	 * <ul>
	 * <li>the namespace prefix is <tt>gml:<//t>
	 * <li>no namespace prefix declaration is written
	 * <li>no <tt>srsName</tt> attribute is written
	 * </ul>
	 */
	public GMLWriter() {
	}

	/**
	 * Creates a writer which may emit the GML namespace prefix 
	 * declaration in the geometry root element.
	 * 
	 * @param emitNamespace trueif the GML namespace prefix declaration should be written
	 *  in the geometry root element
	 */
	public GMLWriter(boolean emitNamespace) {
		this.setNamespace(emitNamespace);
	}

	/**
	 * Specifies the namespace prefix to write on each GML tag. 
	 * A null or blank prefix may be used to indicate no prefix.
	 * <p>
	 * The default is to write <tt>gml:</tt> as the namespace prefix.
	 * 
	 * @param prefix the namespace prefix to use (<tt>null</tt> or blank if none)
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * Sets the value of the <tt>srsName</tt> attribute 
	 * to be written into the root geometry tag.
	 * If the value is <tt>null</tt> or blank no srsName attribute will be written.
	 * The provided value must be a valid XML attribute value 
	 * - it will not be XML-escaped.
	 * <p>
	 * The default is not to write the <tt>srsName</tt> attribute.
	 * 
	 * @param srsName the srsName attribute value
	 */
	public void setSrsName(String srsName) {
		this.srsName = srsName;
	}

	/**
	 * Determines whether a GML namespace declaration will be written in the
	 * opening tag of geometries.  Useful in XML-aware environments which 
	 * parse the geometries before use, such as XSLT.
	 * 
	 * @param emitNamespace true if the GML namespace prefix declaration 
	 * should be written in the root geometry element
	 */
	public void setNamespace(boolean emitNamespace) {
		this.emitNamespace = emitNamespace;
	}

	/**
	 * Specifies a list of custom elements
	 * which are written after the opening tag
	 * of the root element.
	 * The text contained in the string sequence should form valid XML markup.
	 * The specified strings are written one per line immediately after
	 * the root geometry tag line.
	 * <p>
	 * For instance, this is useful for adding KML-specific geometry parameters
	 * such as <tt>&lt;extrude&gt;</tt>
	 * 
	 * @param customElements a list of the custom element strings, or null if none
	 */
	public void setCustomElements(String[] customElements) {
		this.customElements = customElements;
	}

	/**
	 * Sets the starting column index for pretty printing
	 * 
	 * @param indent
	 */
	public void setStartingIndentIndex(int indent) {
		if (indent < 0)
			indent = 0;
		startingIndentIndex = indent;
	}

	/**
	 * Sets the number of coordinates printed per line. 
	 * 
	 * @param num
	 */
	public void setMaxCoordinatesPerLine(int num) {
		if (num < 1)
			throw new IndexOutOfBoundsException(
					"Invalid coordinate count per line, must be > 0");
		maxCoordinatesPerLine = num;
	}

	/**
	 * Writes a {@link Geometry} in GML2 format to a String.
	 * 
	 * @param geom
	 * @return String GML2 Encoded Geometry
	 */
	public String write(Geometry geom) 
	{
		StringWriter writer = new StringWriter();
		try {
			write(geom, writer);
		}
    catch (IOException ex) {
      Assert.shouldNeverReachHere();
    }
		return writer.toString();
	}

	/**
	 * Writes a {@link Geometry} in GML2 format into a {@link Writer}.
	 * 
	 * @param geom Geometry to encode
	 * @param writer Stream to encode to.
	 * @throws IOException 
	 */
	public void write(Geometry geom, Writer writer) throws IOException {
		write(geom, writer, startingIndentIndex);
	}

	private void write(Geometry geom, Writer writer, int level)
			throws IOException 
			{
		isRootTag = true;
		if (geom instanceof Point) {
			writePoint((Point) geom, writer, level);
		} else if (geom instanceof LineString) {
			writeLineString((LineString) geom, writer, level);
		} else if (geom instanceof Polygon) {
			writePolygon((Polygon) geom, writer, level);
		} else if (geom instanceof MultiPoint) {
			writeMultiPoint((MultiPoint) geom, writer, level);
		} else if (geom instanceof MultiLineString) {
			writeMultiLineString((MultiLineString) geom, writer, level);
		} else if (geom instanceof MultiPolygon) {
			writeMultiPolygon((MultiPolygon) geom, writer, level);
		} else if (geom instanceof GeometryCollection) {
			writeGeometryCollection((GeometryCollection) geom, writer,
					startingIndentIndex);
		} else {
			throw new IllegalArgumentException("Unhandled geometry type: "
					+ geom.getGeometryType());
		}
		writer.flush();
	}

	// <gml:Point><gml:coordinates>1195156.78946687,382069.533723461</gml:coordinates></gml:Point>
	private void writePoint(Point p, Writer writer, int level) throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_POINT, p, writer);

		write(new Coordinate[] { p.getCoordinate() }, writer, level + 1);

		startLine(level, writer);
		endGeomTag(GMLConstants.GML_POINT, writer);
	}

	//<gml:LineString><gml:coordinates>1195123.37289257,381985.763974674 1195120.22369473,381964.660533343 1195118.14929823,381942.597718511</gml:coordinates></gml:LineString>
	private void writeLineString(LineString ls, Writer writer, int level)
			throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_LINESTRING, ls, writer);

		write(ls.getCoordinates(), writer, level + 1);

		startLine(level, writer);
		endGeomTag(GMLConstants.GML_LINESTRING, writer);
	}

	//<gml:LinearRing><gml:coordinates>1226890.26761027,1466433.47430292 1226880.59239079,1466427.03208053...></coordinates></gml:LinearRing>
	private void writeLinearRing(LinearRing lr, Writer writer, int level)
			throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_LINEARRING, lr, writer);

		write(lr.getCoordinates(), writer, level + 1);

		startLine(level, writer);
		endGeomTag(GMLConstants.GML_LINEARRING, writer);
	}

	private void writePolygon(Polygon p, Writer writer, int level)
			throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_POLYGON, p, writer);

		startLine(level + 1, writer);
		startGeomTag(GMLConstants.GML_OUTER_BOUNDARY_IS, null, writer);

		writeLinearRing((LinearRing) p.getExteriorRing(), writer, level + 2);

		startLine(level + 1, writer);
		endGeomTag(GMLConstants.GML_OUTER_BOUNDARY_IS, writer);

		for (int t = 0; t < p.getNumInteriorRing(); t++) {
			startLine(level + 1, writer);
			startGeomTag(GMLConstants.GML_INNER_BOUNDARY_IS, null, writer);

			writeLinearRing((LinearRing) p.getInteriorRingN(t), writer, level + 2);

			startLine(level + 1, writer);
			endGeomTag(GMLConstants.GML_INNER_BOUNDARY_IS, writer);
		}

		startLine(level, writer);
		endGeomTag(GMLConstants.GML_POLYGON, writer);
	}

	private void writeMultiPoint(MultiPoint mp, Writer writer, int level)
			throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_MULTI_POINT, mp, writer);

		for (int t = 0; t < mp.getNumGeometries(); t++) {
			startLine(level + 1, writer);
			startGeomTag(GMLConstants.GML_POINT_MEMBER, null, writer);

			writePoint((Point) mp.getGeometryN(t), writer, level + 2);

			startLine(level + 1, writer);
			endGeomTag(GMLConstants.GML_POINT_MEMBER, writer);
		}
		startLine(level, writer);
		endGeomTag(GMLConstants.GML_MULTI_POINT, writer);
	}

	private void writeMultiLineString(MultiLineString mls, Writer writer,
			int level) throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_MULTI_LINESTRING, mls, writer);

		for (int t = 0; t < mls.getNumGeometries(); t++) {
			startLine(level + 1, writer);
			startGeomTag(GMLConstants.GML_LINESTRING_MEMBER, null, writer);

			writeLineString((LineString) mls.getGeometryN(t), writer, level + 2);

			startLine(level + 1, writer);
			endGeomTag(GMLConstants.GML_LINESTRING_MEMBER, writer);
		}
		startLine(level, writer);
		endGeomTag(GMLConstants.GML_MULTI_LINESTRING, writer);
	}

	private void writeMultiPolygon(MultiPolygon mp, Writer writer, int level)
			throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_MULTI_POLYGON, mp, writer);

		for (int t = 0; t < mp.getNumGeometries(); t++) {
			startLine(level + 1, writer);
			startGeomTag(GMLConstants.GML_POLYGON_MEMBER, null, writer);

			writePolygon((Polygon) mp.getGeometryN(t), writer, level + 2);

			startLine(level + 1, writer);
			endGeomTag(GMLConstants.GML_POLYGON_MEMBER, writer);
		}
		startLine(level, writer);
		endGeomTag(GMLConstants.GML_MULTI_POLYGON, writer);
	}

	private void writeGeometryCollection(GeometryCollection gc, Writer writer,
			int level) throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_MULTI_GEOMETRY, gc, writer);

		for (int t = 0; t < gc.getNumGeometries(); t++) {
			startLine(level + 1, writer);
			startGeomTag(GMLConstants.GML_GEOMETRY_MEMBER, null, writer);

			write(gc.getGeometryN(t), writer, level + 2);

			startLine(level + 1, writer);
			endGeomTag(GMLConstants.GML_GEOMETRY_MEMBER, writer);
		}
		startLine(level, writer);
		endGeomTag(GMLConstants.GML_MULTI_GEOMETRY, writer);
	}

	private static final String coordinateSeparator = ",";

	private static final String tupleSeparator = " ";

	/**
	 * Takes a list of coordinates and converts it to GML.<br>
	 * 2d and 3d aware.
	 * 
	 * @param coords array of coordinates
	 * @throws IOException 
	 */
	private void write(Coordinate[] coords, Writer writer, int level)
			throws IOException {
		startLine(level, writer);
		startGeomTag(GMLConstants.GML_COORDINATES, null, writer);

		int dim = 2;

		if (coords.length > 0) {
			if (!(Double.isNaN(coords[0].z)))
				dim = 3;
		}

		boolean isNewLine = true;
		for (int i = 0; i < coords.length; i++) {
			if (isNewLine) {
				startLine(level + 1, writer);
				isNewLine = false;
			}
			if (dim == 2) {
				writer.write("" + coords[i].x);
				writer.write(coordinateSeparator);
				writer.write("" + coords[i].y);
			} else if (dim == 3) {
				writer.write("" + coords[i].x);
				writer.write(coordinateSeparator);
				writer.write("" + coords[i].y);
				writer.write(coordinateSeparator);
				writer.write("" + coords[i].z);
			}
			writer.write(tupleSeparator);

			// break output lines to prevent them from getting too long
			if ((i + 1) % maxCoordinatesPerLine == 0 && i < coords.length - 1) {
				writer.write("\n");
				isNewLine = true;
			}
		}
		if (!isNewLine)
			writer.write("\n");

		startLine(level, writer);
		endGeomTag(GMLConstants.GML_COORDINATES, writer);
	}

	private void startLine(int level, Writer writer) throws IOException {
		for (int i = 0; i < level; i++)
			writer.write(INDENT);
	}

	private void startGeomTag(String geometryName, Geometry g, Writer writer)
			throws IOException {
		writer.write("<"
				+ ((prefix == null || "".equals(prefix)) ? "" : prefix + ":"));
		writer.write(geometryName);
		writeAttributes(g, writer);
		writer.write(">\n");
		writeCustomElements(g, writer);
		isRootTag = false;
	}

	private void writeAttributes(Geometry geom, Writer writer) throws IOException {
		if (geom == null)
			return;
		if (! isRootTag)
			return;
		
		if (emitNamespace) {
			writer.write(" xmlns"
					+ ((prefix == null || "".equals(prefix)) ? "" : ":"+prefix )
					+ "='" + namespace + "'");
		}
		if (srsName != null && srsName.length() > 0) {
			writer.write(" " + GMLConstants.GML_ATTR_SRSNAME + "='" + srsName + "'");
			// MD - obsoleted
//			writer.write(geom.getSRID() + "");
		}
	}

	private void writeCustomElements(Geometry geom, Writer writer) throws IOException {
		if (geom == null)			return;
		if (! isRootTag)			return;
		if (customElements == null) return;
		
		for (int i = 0; i < customElements.length; i++) {
			writer.write(customElements[i]);
			writer.write("\n");
		}
	}
	
	private void endGeomTag(String geometryName, Writer writer)
			throws IOException {
		writer.write("</" + prefix());
		writer.write(geometryName);
		writer.write(">\n");
	}
	
	private String prefix()
	{
		if (prefix == null || prefix.length() == 0)
			return "";
		return prefix + ":";
	}
}
