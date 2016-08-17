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

import java.util.*;
import java.util.regex.Pattern;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.gml2.GMLHandler.Handler;
import com.vividsolutions.jts.util.StringUtil;

/**
 * Container for GML2 Geometry parsing strategies which can be represented in JTS.
 *
 * @author David Zwiers, Vivid Solutions.
 */
public class GeometryStrategies{

	/**
	 * This set of strategies is not expected to be used directly outside of this distribution.
	 * 
	 * The implementation of this class are intended to be used as static function points in C. These strategies should be associated with an element when the element begins. The strategy is utilized at the end of the element to create an object of value to the user. 
	 * 
	 * In this case all the objects are either java.lang.* or JTS Geometry objects
	 *
	 * @author David Zwiers, Vivid Solutions.
	 */
	static interface ParseStrategy{
		/**
		 * @param arg Value to interpret
		 * @param gf GeometryFactory
		 * @return The interpreted value
		 * @throws SAXException 
		 */
		Object parse(Handler arg, GeometryFactory gf) throws SAXException;
	}
	
	private static HashMap strategies = loadStrategies();
	private static HashMap loadStrategies(){
		HashMap strats = new HashMap();
		
		// point
		strats.put(GMLConstants.GML_POINT.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()!=1)
					throw new SAXException("Cannot create a point without exactly one coordinate");

				int srid = getSrid(arg.attrs,gf.getSRID());

				Object c = arg.children.get(0);
				Point p = null;
				if(c instanceof Coordinate){
					p = gf.createPoint((Coordinate)c);
				}else{
					p = gf.createPoint((CoordinateSequence)c);
				}
				if(p.getSRID()!=srid)
					p.setSRID(srid);
				
				return p;
			}
		});
		
		// linestring
		strats.put(GMLConstants.GML_LINESTRING.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()<1)
					throw new SAXException("Cannot create a linestring without atleast two coordinates or one coordinate sequence");

				int srid = getSrid(arg.attrs,gf.getSRID());
				
				LineString ls = null;
				if(arg.children.size() == 1){
					// coord set
					try{
						CoordinateSequence cs = (CoordinateSequence) arg.children.get(0);
						ls = gf.createLineString(cs);
					}catch(ClassCastException e){
						throw new SAXException("Cannot create a linestring without atleast two coordinates or one coordinate sequence",e);
					}
				}else{
					try{
						Coordinate[] coords = (Coordinate[]) arg.children.toArray(new Coordinate[arg.children.size()]);
						ls = gf.createLineString(coords);
					}catch(ClassCastException e){
						throw new SAXException("Cannot create a linestring without atleast two coordinates or one coordinate sequence",e);
					}
				}
				
				if(ls.getSRID()!=srid)
					ls.setSRID(srid);
				
				return ls;
			}
		});
		
		// linearring
		strats.put(GMLConstants.GML_LINEARRING.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()!=1 && arg.children.size()<4)
					throw new SAXException("Cannot create a linear ring without atleast four coordinates or one coordinate sequence");

				int srid = getSrid(arg.attrs,gf.getSRID());
				
				LinearRing ls = null;
				if(arg.children.size() == 1){
					// coord set
					try{
						CoordinateSequence cs = (CoordinateSequence) arg.children.get(0);
						ls = gf.createLinearRing(cs);
					}catch(ClassCastException e){
						throw new SAXException("Cannot create a linear ring without atleast four coordinates or one coordinate sequence",e);
					}
				}else{
					try{
						Coordinate[] coords = (Coordinate[]) arg.children.toArray(new Coordinate[arg.children.size()]);
						ls = gf.createLinearRing(coords);
					}catch(ClassCastException e){
						throw new SAXException("Cannot create a linear ring without atleast four coordinates or one coordinate sequence",e);
					}
				}
				
				if(ls.getSRID()!=srid)
					ls.setSRID(srid);
				
				return ls;
			}
		});
		
		// polygon
		strats.put(GMLConstants.GML_POLYGON.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()<1)
					throw new SAXException("Cannot create a polygon without atleast one linear ring");

				int srid = getSrid(arg.attrs,gf.getSRID());
				
				LinearRing outer = (LinearRing) arg.children.get(0); // will be the first
				List t = arg.children.size()>1?arg.children.subList(1,arg.children.size()):null;
				LinearRing[] inner = t==null?null:(LinearRing[]) t.toArray(new LinearRing[t.size()]);
				
				Polygon p = gf.createPolygon(outer,inner);
				
				if(p.getSRID()!=srid)
					p.setSRID(srid);
				
				return p;
			}
		});
		
		// box
		strats.put(GMLConstants.GML_BOX.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()<1 || arg.children.size()>2)
					throw new SAXException("Cannot create a box without either two coords or one coordinate sequence");

//				int srid = getSrid(arg.attrs,gf.getSRID());
				
				Envelope box = null;
				if(arg.children.size() == 1){
					CoordinateSequence cs = (CoordinateSequence) arg.children.get(0);
					box = cs.expandEnvelope(new Envelope());
				}else{
					box = new Envelope((Coordinate)arg.children.get(0),(Coordinate)arg.children.get(1));
				}
				
				return box;
			}
		});
		
		// multi-point
		strats.put(GMLConstants.GML_MULTI_POINT.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()<1)
					throw new SAXException("Cannot create a multi-point without atleast one point");

				int srid = getSrid(arg.attrs,gf.getSRID());
				
				Point[] pts = (Point[]) arg.children.toArray(new Point[arg.children.size()]);
				
				MultiPoint mp = gf.createMultiPoint(pts);
				
				if(mp.getSRID()!=srid)
					mp.setSRID(srid);
				
				return mp;
			}
		});
		
		// multi-linestring
		strats.put(GMLConstants.GML_MULTI_LINESTRING.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()<1)
					throw new SAXException("Cannot create a multi-linestring without atleast one linestring");

				int srid = getSrid(arg.attrs,gf.getSRID());
				
				LineString[] lns = (LineString[]) arg.children.toArray(new LineString[arg.children.size()]);
				
				MultiLineString mp = gf.createMultiLineString(lns);
				
				if(mp.getSRID()!=srid)
					mp.setSRID(srid);
				
				return mp;
			}
		});
		
		// multi-poly
		strats.put(GMLConstants.GML_MULTI_POLYGON.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()<1)
					throw new SAXException("Cannot create a multi-polygon without atleast one polygon");

				int srid = getSrid(arg.attrs,gf.getSRID());
				
				Polygon[] plys = (Polygon[]) arg.children.toArray(new Polygon[arg.children.size()]);
				
				MultiPolygon mp = gf.createMultiPolygon(plys);
				
				if(mp.getSRID()!=srid)
					mp.setSRID(srid);
				
				return mp;
			}
		});
		
		// multi-geom
		strats.put(GMLConstants.GML_MULTI_GEOMETRY.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence
				
				if(arg.children.size()<1)
					throw new SAXException("Cannot create a multi-polygon without atleast one geometry");
				
				Geometry[] geoms = (Geometry[]) arg.children.toArray(new Geometry[arg.children.size()]);
				
				GeometryCollection gc = gf.createGeometryCollection(geoms);
								
				return gc;
			}
		});
		
		// coordinates
		strats.put(GMLConstants.GML_COORDINATES.toLowerCase(),new ParseStrategy(){

			private WeakHashMap patterns = new WeakHashMap();
			
			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence

				if(arg.text == null || "".equals(arg.text))
					throw new SAXException("Cannot create a coordinate sequence without text to parse");
				
				String decimal = ".";
				String coordSeperator = ",";
				String toupleSeperator = " ";
				
				// get overides from coordinates
				if(arg.attrs.getIndex("decimal")>=0)
					decimal = arg.attrs.getValue("decimal");
				else if(arg.attrs.getIndex(GMLConstants.GML_NAMESPACE,"decimal")>=0)
					decimal = arg.attrs.getValue(GMLConstants.GML_NAMESPACE,"decimal");

				if(arg.attrs.getIndex("cs")>=0)
					coordSeperator = arg.attrs.getValue("cs");
				else if(arg.attrs.getIndex(GMLConstants.GML_NAMESPACE,"cs")>=0)
					coordSeperator = arg.attrs.getValue(GMLConstants.GML_NAMESPACE,"cs");

				if(arg.attrs.getIndex("ts")>=0)
					toupleSeperator = arg.attrs.getValue("ts");
				else if(arg.attrs.getIndex(GMLConstants.GML_NAMESPACE,"ts")>=0)
					toupleSeperator = arg.attrs.getValue(GMLConstants.GML_NAMESPACE,"ts");
				
				// now to start parse
				String t = arg.text.toString();
				t = t.replaceAll("\\s"," ");
				
				Pattern ptn = (Pattern) patterns.get(toupleSeperator);
				if(ptn == null){
					String ts = new String(toupleSeperator);
					if(ts.indexOf('\\')>-1){
							// need to escape it
							ts = ts.replaceAll("\\","\\\\");
					}
					if(ts.indexOf('.')>-1){
						// need to escape it
						ts = ts.replaceAll("\\.","\\\\.");
					}
					ptn = Pattern.compile(ts);
					patterns.put(toupleSeperator,ptn);
				}
				String[] touples = ptn.split(t.trim());//  t.trim().split(toupleSeperator);
				
				if(touples.length == 0)
					throw new SAXException("Cannot create a coordinate sequence without a touple to parse");
				
				// we may have null touples, so calculate the num first
				int numNonNullTouples = 0;
				for(int i=0;i<touples.length;i++){
					if(touples[i] !=null && !"".equals(touples[i].trim())){
						if(i!=numNonNullTouples){
							touples[numNonNullTouples] = touples[i]; // always shift left
						}
						numNonNullTouples++;
					}
				}
				for(int i=numNonNullTouples;i<touples.length;i++)
					touples[i] = null;
				
				// null touples now at end of array
				if(numNonNullTouples == 0)
					throw new SAXException("Cannot create a coordinate sequence without a non-null touple to parse");
				
				int dim = StringUtil.split(touples[0], coordSeperator).length;
				CoordinateSequence cs = gf.getCoordinateSequenceFactory().create(numNonNullTouples,dim);
				dim = cs.getDimension(); // max dim
				
				boolean replaceDec = !".".equals(decimal);
				
				for(int i=0;i<numNonNullTouples;i++){
					// for each touple, split, parse, add

					ptn = (Pattern) patterns.get(coordSeperator);
					if(ptn == null){
						String ts = new String(coordSeperator);
						if(ts.indexOf('\\')>-1){
								// need to escape it
							ts = ts.replaceAll("\\","\\\\");
						}
						if(ts.indexOf('.')>-1){
							// need to escape it
							ts = ts.replaceAll("\\.","\\\\.");
						}
						ptn = Pattern.compile(ts);
						patterns.put(coordSeperator,ptn);
					}
					String[] coords = ptn.split(touples[i]);//  touples[i].split(coordSeperator);
					
					int dimIndex = 0;
					for(int j=0;j<coords.length && j<dim;j++){
						if(coords[j] != null && !"".equals(coords[j].trim())){
							double ordinate = Double.parseDouble(replaceDec?coords[j].replaceAll(decimal,"."):coords[j]);
							cs.setOrdinate(i,dimIndex++,ordinate);
						}
					}
						// fill remaining dim
					for(;dimIndex<dim;)cs.setOrdinate(i,dimIndex++,Double.NaN);
				}
				
				return cs;
			}
		});
		
		// coord
		strats.put(GMLConstants.GML_COORD.toLowerCase(),new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				// one child, either a coord
				// or a coordinate sequence

				if(arg.children.size()<1)
					throw new SAXException("Cannot create a coordinate without atleast one axis");
				if(arg.children.size()>3)
					throw new SAXException("Cannot create a coordinate with more than 3 axis");
				
				Double[] axis = (Double[]) arg.children.toArray(new Double[arg.children.size()]);
				Coordinate c = new Coordinate();
				c.x = axis[0].doubleValue();
				if(axis.length>1)
					c.y = axis[1].doubleValue();
				if(axis.length>2)
					c.z = axis[2].doubleValue();
				
				return c;
			}
		});
		
		ParseStrategy coord_child = new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				if(arg.text == null)
					return null;
				return new Double((arg.text.toString()));
			}
		};
		
		// coord-x
		strats.put(GMLConstants.GML_COORD_X.toLowerCase(),coord_child);
		
		// coord-y
		strats.put(GMLConstants.GML_COORD_Y.toLowerCase(),coord_child);
		
		// coord-z
		strats.put(GMLConstants.GML_COORD_Z.toLowerCase(),coord_child);
		
		ParseStrategy member = new ParseStrategy(){

			public Object parse(Handler arg, GeometryFactory gf) throws SAXException {
				if(arg.children.size()!=1)
					throw new SAXException("Geometry Members may only contain one geometry.");
				
				// type checking will occur in the parent geom collection.
				// may wish to add this in the future
				
				return arg.children.get(0);
			}
		};
		// outerBoundary - linear ring member
		strats.put(GMLConstants.GML_OUTER_BOUNDARY_IS.toLowerCase(),member);
		
		// innerBoundary - linear ring member
		strats.put(GMLConstants.GML_INNER_BOUNDARY_IS.toLowerCase(),member);
		
		// point member
		strats.put(GMLConstants.GML_POINT_MEMBER.toLowerCase(),member);
		
		// line string member
		strats.put(GMLConstants.GML_LINESTRING_MEMBER.toLowerCase(),member);
		
		// polygon member
		strats.put(GMLConstants.GML_POLYGON_MEMBER.toLowerCase(),member);
		
		return strats;
	}
	
	static int getSrid(Attributes attrs, int defaultValue){
		String srs = null;
		if(attrs.getIndex(GMLConstants.GML_ATTR_SRSNAME)>=0)
			srs = attrs.getValue(GMLConstants.GML_ATTR_SRSNAME);
		else if(attrs.getIndex(GMLConstants.GML_NAMESPACE,GMLConstants.GML_ATTR_SRSNAME)>=0)
			srs = attrs.getValue(GMLConstants.GML_NAMESPACE,GMLConstants.GML_ATTR_SRSNAME);
		
		if(srs != null){
			srs = srs.trim();
			if(srs != null && !"".equals(srs)){
				try{
					return Integer.parseInt(srs);
				}catch(NumberFormatException e){
					// rip out the end, uri's are used here sometimes
					int index = srs.lastIndexOf('#');
					if(index > -1)
						srs = srs.substring(index);
					try{
						return Integer.parseInt(srs);
					}catch(NumberFormatException e2){
						// ignore
					}
				}
			}
		}
		
		return defaultValue;
	}
	
	/**
	 * @param uri Not currently used, included for future work
	 * @param localName Used to look up an appropriate parse strategy
	 * @return The ParseStrategy which should be employed
	 * 
	 * @see ParseStrategy
	 */
	public static ParseStrategy findStrategy(String uri,String localName){
		return localName == null?null:(ParseStrategy) strategies.get(localName.toLowerCase());
	}
}
