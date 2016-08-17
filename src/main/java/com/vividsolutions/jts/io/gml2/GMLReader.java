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

import javax.xml.parsers.*;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.vividsolutions.jts.geom.*;

/**
 * Reads a GML2 geometry from an XML fragment into a {@link Geometry}.
 * <p>
 * An example of the GML2 format handled is:
 * <pre>
 *   <LineString>
 *  	<coordinates>24824.045318333192,38536.15071012041
 *  		26157.378651666528,37567.42733944659 26666.666,36000.0
 *  		26157.378651666528,34432.57266055341
 *  		24824.045318333192,33463.84928987959
 *  		23175.954681666804,33463.84928987959
 *  		21842.621348333472,34432.57266055341 21333.333,36000.0
 *  		21842.621348333472,37567.42733944659
 *  		23175.954681666808,38536.15071012041 </coordinates>
 *  </LineString>
 * </pre>
 *
 * The reader ignores namespace prefixes, 
 * and disables both the validation and namespace options on the <tt>SAXParser</tt>.
 * This class requires the presence of a SAX Parser available via the 
 * {@link javax.xml.parsers.SAXParserFactory#newInstance()}
 * method.
 * <p>
 * A specification of the GML XML format 
 * can be found at the OGC web site: <a href='http://www.opengeospatial.org/'>http://www.opengeospatial.org/</a>.
 * <p>
 * It is the caller's responsibility to ensure that the supplied {@link PrecisionModel}
 * matches the precision of the incoming data.
 * If a lower precision for the data is required, a subsequent
 * process must be run on the data to reduce its precision.
 * <p>
 * To parse and build geometry directly from a SAX stream, see {@link GMLHandler}.
 *
 * @author David Zwiers, Vivid Solutions.
 * 
 * @see GMLHandler
 */
public class GMLReader 
{

	/**
	 * Reads a GML2 Geometry from a <tt>String</tt> into a single {@link Geometry}
	 *
	 * If a collection of geometries is found, a {@link GeometryCollection} is returned.
	 *
	 * @param gml The GML String to parse
	 * @param geometryFactory When null, a default will be used.
	 * @return the resulting JTS Geometry
	 * 
	 * @throws ParserConfigurationException
	 * @throws IOException
	 * @throws SAXException
	 *
	 * @see #read(Reader, GeometryFactory)
	 */
	public Geometry read(String gml, GeometryFactory geometryFactory) throws SAXException, IOException, ParserConfigurationException{
		return read(new StringReader(gml),geometryFactory);
	}

	/**
	 * Reads a GML2 Geometry from a {@link Reader} into a single {@link Geometry}
	 *
	 * If a collection of Geometries is found, a {@link GeometryCollection} is returned.
	 *
	 * @param reader The input source
	 * @param geometryFactory When null, a default will be used.
	 * @return The resulting JTS Geometry
	 * @throws SAXException
	 * @throws IOException
	 */
	public Geometry read(Reader reader, GeometryFactory geometryFactory) throws SAXException, IOException, ParserConfigurationException{
		SAXParserFactory fact = SAXParserFactory.newInstance();

		fact.setNamespaceAware(false);
		fact.setValidating(false);

		SAXParser parser = fact.newSAXParser();

		if(geometryFactory == null)
			geometryFactory = new GeometryFactory();

		GMLHandler gh = new GMLHandler(geometryFactory,null);
		parser.parse(new InputSource(reader), (DefaultHandler)gh);

		return gh.getGeometry();
	}

}
