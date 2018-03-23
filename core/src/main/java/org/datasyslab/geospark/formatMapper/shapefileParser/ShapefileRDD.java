/*
 * FILE: ShapefileRDD
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
package org.datasyslab.geospark.formatMapper.shapefileParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.datasyslab.geospark.formatMapper.shapefileParser.boundary.BoundBox;
import org.datasyslab.geospark.formatMapper.shapefileParser.boundary.BoundaryInputFormat;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeInputFormat;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class ShapefileRDD.
 */
@Deprecated
public class ShapefileRDD
        implements Serializable
{

    /**
     * shape collection.
     */
    private JavaRDD<Geometry> shapeRDD = null;

    /**
     * The geometry factory.
     */
    public static GeometryFactory geometryFactory = new GeometryFactory();

    /**
     * bounding box.
     */
    private BoundBox boundBox = null;

    /**
     * ShapefileRDD.
     *
     * @param sparkContext the spark context
     * @param filePath the file path
     */
    public ShapefileRDD(JavaSparkContext sparkContext, String filePath)
    {
        boundBox = new BoundBox();
        JavaPairRDD<ShapeKey, PrimitiveShape> shapePrimitiveRdd = sparkContext.newAPIHadoopFile(
                filePath,
                ShapeInputFormat.class,
                ShapeKey.class,
                PrimitiveShape.class,
                sparkContext.hadoopConfiguration()
        );
        shapeRDD = shapePrimitiveRdd.map(PrimitiveToShape);
    }

    /**
     * The Constant PrimitiveToShape.
     */
    private static final Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry> PrimitiveToShape
            = new Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry>()
    {
        public Geometry call(Tuple2<ShapeKey, PrimitiveShape> primitiveTuple)
        {
            Geometry shape = null;
            // parse bytes to shape
            try {
                shape = primitiveTuple._2().getShape(geometryFactory);
            }
            catch (TypeUnknownException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                return shape;
            }
        }
    };

    /**
     * The Print shape.
     */
    private final VoidFunction<Geometry> PrintShape = new VoidFunction<Geometry>()
    {
        public void call(Geometry shape)
                throws Exception
        {
            System.out.println(shape.toText());
        }
    };

    /**
     * Gets the shape RDD.
     *
     * @return the shape RDD
     */
    public JavaRDD<Geometry> getShapeRDD()
    {
        return this.shapeRDD;
    }

    /**
     * Gets the point RDD.
     *
     * @return the point RDD
     */
    public JavaRDD<Point> getPointRDD()
    {
        return shapeRDD.flatMap(new FlatMapFunction<Geometry, Point>()
        {
            @Override
            public Iterator<Point> call(Geometry spatialObject)
                    throws Exception
            {
                List<Point> result = new ArrayList<Point>();
                if (spatialObject instanceof MultiPoint) {
                    MultiPoint multiObjects = (MultiPoint) spatialObject;
                    for (int i = 0; i < multiObjects.getNumGeometries(); i++) {
                        Point oneObject = (Point) multiObjects.getGeometryN(i);
                        oneObject.setUserData(multiObjects.getUserData());
                        result.add(oneObject);
                    }
                }
                else if (spatialObject instanceof Point) {
                    result.add((Point) spatialObject);
                }
                else {
                    throw new Exception("[ShapefileRDD][getPointRDD] the object type is not Point or MultiPoint type. It is " + spatialObject.getGeometryType());
                }
                return result.iterator();
            }
        });
    }

    /**
     * Gets the polygon RDD.
     *
     * @return the polygon RDD
     */
    public JavaRDD<Polygon> getPolygonRDD()
    {
        return shapeRDD.flatMap(new FlatMapFunction<Geometry, Polygon>()
        {
            @Override
            public Iterator<Polygon> call(Geometry spatialObject)
                    throws Exception
            {
                List<Polygon> result = new ArrayList<Polygon>();
                if (spatialObject instanceof MultiPolygon) {
                    MultiPolygon multiObjects = (MultiPolygon) spatialObject;
                    for (int i = 0; i < multiObjects.getNumGeometries(); i++) {
                        Polygon oneObject = (Polygon) multiObjects.getGeometryN(i);
                        oneObject.setUserData(multiObjects.getUserData());
                        result.add(oneObject);
                    }
                }
                else if (spatialObject instanceof Polygon) {
                    result.add((Polygon) spatialObject);
                }
                else {
                    throw new Exception("[ShapefileRDD][getPolygonRDD] the object type is not Polygon or MultiPolygon type. It is " + spatialObject.getGeometryType());
                }
                return result.iterator();
            }
        });
    }

    /**
     * Gets the line string RDD.
     *
     * @return the line string RDD
     */
    public JavaRDD<LineString> getLineStringRDD()
    {
        return shapeRDD.flatMap(new FlatMapFunction<Geometry, LineString>()
        {
            @Override
            public Iterator<LineString> call(Geometry spatialObject)
                    throws Exception
            {
                List<LineString> result = new ArrayList<LineString>();
                if (spatialObject instanceof MultiLineString) {
                    MultiLineString multiObjects = (MultiLineString) spatialObject;
                    for (int i = 0; i < multiObjects.getNumGeometries(); i++) {
                        LineString oneObject = (LineString) multiObjects.getGeometryN(i);
                        oneObject.setUserData(multiObjects.getUserData());
                        result.add(oneObject);
                    }
                }
                else if (spatialObject instanceof LineString) {
                    result.add((LineString) spatialObject);
                }
                else {
                    throw new Exception("[ShapefileRDD][getLineStringRDD] the object type is not LineString or MultiLineString type. It is " + spatialObject.getGeometryType());
                }
                return result.iterator();
            }
        });
    }

    /**
     * Count.
     *
     * @return the long
     */
    public long count()
    {
        return shapeRDD.count();
    }

    /**
     * read and merge bound boxes of all shapefiles user input, if there is no, leave BoundBox null;
     */
    public BoundBox getBoundBox(JavaSparkContext sc, String inputPath)
    {
        // read bound boxes into memory
        JavaPairRDD<Long, BoundBox> bounds = sc.newAPIHadoopFile(
                inputPath,
                BoundaryInputFormat.class,
                Long.class,
                BoundBox.class,
                sc.hadoopConfiguration()
        );
        // merge all into one
        bounds = bounds.reduceByKey(new Function2<BoundBox, BoundBox, BoundBox>()
        {
            @Override
            public BoundBox call(BoundBox box1, BoundBox box2)
                    throws Exception
            {
                return BoundBox.mergeBoundBox(box1, box2);
            }
        });
        // if there is a result assign it to variable : boundBox
        if (bounds.count() > 0) {
            return new BoundBox(bounds.collect().get(0)._2());
        }
        else { return null; }
    }
}