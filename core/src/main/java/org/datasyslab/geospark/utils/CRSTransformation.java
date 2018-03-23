/*
 * FILE: CRSTransformation
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

// TODO: Auto-generated Javadoc

/**
 * The Class CRSTransformation.
 */
public class CRSTransformation
{

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
            return (Point) JTS.transform(sourceObject, transform);
        }
        catch (FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (MismatchedDimensionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (TransformException e) {
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
            return (Polygon) JTS.transform(sourceObject, transform);
        }
        catch (FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (MismatchedDimensionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (TransformException e) {
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
            return JTS.transform(sourceObject, transform);
        }
        catch (FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (MismatchedDimensionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (TransformException e) {
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
            return (T) JTS.transform(sourceObject, transform);
        }
        catch (FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (MismatchedDimensionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        catch (TransformException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }
}
