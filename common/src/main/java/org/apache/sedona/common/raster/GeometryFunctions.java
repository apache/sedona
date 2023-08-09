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

package org.apache.sedona.common.raster;

import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.Envelope2D;
import org.locationtech.jts.geom.*;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.geom.Point2D;

public class GeometryFunctions {

    /***
     * Returns the convex hull of the input raster
     * @param raster
     * @return Geometry: convex hull of the input raster
     * @throws FactoryException
     * @throws TransformException
     */
    public static Geometry convexHull(GridCoverage2D raster) throws FactoryException, TransformException {
        int width = RasterAccessors.getWidth(raster), height = RasterAccessors.getHeight(raster);
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), RasterAccessors.srid(raster));
        double upperLeftX = RasterAccessors.getUpperLeftX(raster), upperLeftY = RasterAccessors.getUpperLeftY(raster);
        //First and last coord (Upper left)
        Coordinate coordOne = new Coordinate(upperLeftX, upperLeftY);

        //start clockwise rotation

        //upper right
        Point2D point = RasterUtils.getWorldCornerCoordinates(raster, width + 1, 1);
        Coordinate coordTwo = new Coordinate(point.getX(), point.getY());

        //lower right
        point = RasterUtils.getWorldCornerCoordinates(raster, width + 1, height + 1);
        Coordinate coordThree = new Coordinate(point.getX(), point.getY());

        //lower left
        point = RasterUtils.getWorldCornerCoordinates(raster, 1, height + 1);
        Coordinate coordFour = new Coordinate(point.getX(), point.getY());

        return geometryFactory.createPolygon(new Coordinate[] {coordOne, coordTwo, coordThree, coordFour, coordOne});
    }

    public static Geometry envelope(GridCoverage2D raster) throws FactoryException {
        Envelope2D envelope2D = raster.getEnvelope2D();

        Envelope envelope = new Envelope(envelope2D.getMinX(), envelope2D.getMaxX(), envelope2D.getMinY(), envelope2D.getMaxY());
        int srid = RasterAccessors.srid(raster);
        return new GeometryFactory(new PrecisionModel(), srid).toGeometry(envelope);
    }
}