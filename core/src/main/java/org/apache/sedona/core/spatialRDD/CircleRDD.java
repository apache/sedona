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

package org.apache.sedona.core.spatialRDD;

import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

// TODO: Auto-generated Javadoc

/**
 * The Class CircleRDD.
 */
public class CircleRDD
        extends SpatialRDD<Circle>
{

    /**
     * Instantiates a new circle RDD.
     *
     * @param circleRDD the circle RDD
     */
    public CircleRDD(JavaRDD<Circle> circleRDD)
    {
        this.rawSpatialRDD = circleRDD;
    }

    /**
     * Instantiates a new circle RDD.
     *
     * @param circleRDD the circle RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCRSCode the target epsg CRS code
     */
    public CircleRDD(JavaRDD<Circle> circleRDD, String sourceEpsgCRSCode, String targetEpsgCRSCode)
    {
        this.rawSpatialRDD = circleRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCRSCode);
    }

    /**
     * Instantiates a new circle RDD.
     *
     * @param spatialRDD the spatial RDD
     * @param Radius the radius
     */
    public CircleRDD(SpatialRDD spatialRDD, Double Radius)
    {
        final Double radius = Radius;
        this.rawSpatialRDD = spatialRDD.rawSpatialRDD.map(new Function<Object, Object>()
        {

            public Object call(Object v1)
            {

                return new Circle((Geometry) v1, radius);
            }
        });
        this.CRStransformation = spatialRDD.CRStransformation;
        this.sourceEpsgCode = spatialRDD.sourceEpsgCode;
        this.targetEpgsgCode = spatialRDD.targetEpgsgCode;
    }

    /**
     * Gets the center point as spatial RDD.
     *
     * @return the center point as spatial RDD
     */
    public PointRDD getCenterPointAsSpatialRDD()
    {
        return new PointRDD(this.rawSpatialRDD.map(new Function<Circle, Point>()
        {

            public Point call(Circle circle)
            {
                return (Point) circle.getCenterGeometry();
            }
        }));
    }

    /**
     * Gets the center polygon as spatial RDD.
     *
     * @return the center polygon as spatial RDD
     */
    public PolygonRDD getCenterPolygonAsSpatialRDD()
    {
        return new PolygonRDD(this.rawSpatialRDD.map(new Function<Circle, Polygon>()
        {

            public Polygon call(Circle circle)
            {

                return (Polygon) circle.getCenterGeometry();
            }
        }));
    }

    /**
     * Gets the center line string RDD as spatial RDD.
     *
     * @return the center line string RDD as spatial RDD
     */
    public LineStringRDD getCenterLineStringRDDAsSpatialRDD()
    {
        return new LineStringRDD(this.rawSpatialRDD.map(new Function<Circle, LineString>()
        {

            public LineString call(Circle circle)
            {

                return (LineString) circle.getCenterGeometry();
            }
        }));
    }

    /**
     * Gets the center rectangle RDD as spatial RDD.
     *
     * @return the center rectangle RDD as spatial RDD
     */
    public RectangleRDD getCenterRectangleRDDAsSpatialRDD()
    {
        return new RectangleRDD(this.rawSpatialRDD.map(new Function<Circle, Polygon>()
        {

            public Polygon call(Circle circle)
            {

                return (Polygon) circle.getCenterGeometry();
            }
        }));
    }
}
