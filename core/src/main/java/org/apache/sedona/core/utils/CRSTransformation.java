/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.utils;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
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
