/*
 * FILE: CircleRDD
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
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.geometryObjects.Circle;

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
