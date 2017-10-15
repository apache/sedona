/**
 * FILE: CRSTransformation.java
 * PATH: org.datasyslab.geospark.utils.CRSTransformation.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Geometry;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class CRSTransformation.
 */
public class CRSTransformation {

	/**
	 * Transform.
	 *
	 * @param sourceEpsgCRSCode the source epsg CRS code
	 * @param targetEpsgCRSCode the target epsg CRS code
	 * @param sourceObject the source object
	 * @return the point
	 */
	public static Point Transform(String sourceEpsgCRSCode, String targetEpsgCRSCode, Point sourceObject)
	{
		try {
	    	CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
			CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
			final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
			return (Point)JTS.transform(sourceObject,transform);
			} catch (FactoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (MismatchedDimensionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (TransformException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
	}
	
	/**
	 * Transform.
	 *
	 * @param sourceEpsgCRSCode the source epsg CRS code
	 * @param targetEpsgCRSCode the target epsg CRS code
	 * @param sourceObject the source object
	 * @return the polygon
	 */
	public static Polygon Transform(String sourceEpsgCRSCode, String targetEpsgCRSCode, Polygon sourceObject)
	{
		try {
	    	CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
			CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
			final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
			return (Polygon)JTS.transform(sourceObject,transform);
			} catch (FactoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (MismatchedDimensionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (TransformException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
	}
	
	/**
	 * Transform.
	 *
	 * @param sourceEpsgCRSCode the source epsg CRS code
	 * @param targetEpsgCRSCode the target epsg CRS code
	 * @param sourceObject the source object
	 * @return the envelope
	 */
	public static Envelope Transform(String sourceEpsgCRSCode, String targetEpsgCRSCode, Envelope sourceObject)
	{
		try {
	    	CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
			CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
			final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
			return JTS.transform(sourceObject,transform);
			} catch (FactoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (MismatchedDimensionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (TransformException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
	}

	public static <T extends Geometry> T Transform(String sourceEpsgCRSCode, String targetEpsgCRSCode, T sourceObject)
	{
		try {
			CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
			CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
			final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
			return (T)JTS.transform(sourceObject,transform);
		} catch (FactoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (MismatchedDimensionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (TransformException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
