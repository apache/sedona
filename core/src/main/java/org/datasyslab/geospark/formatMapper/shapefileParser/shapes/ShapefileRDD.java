/**
 * FILE: ShapefileRDD.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapefileRDD.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShapefileRDD implements Serializable{

    /** shape collection */
    private JavaRDD<Geometry> shapeRDD = null;

    public static GeometryFactory geometryFactory;

    /** field descriptor of  */

    public ShapefileRDD(JavaSparkContext sparkContext, String filePath){
        geometryFactory = new GeometryFactory();
        JavaPairRDD<ShapeKey, PrimitiveShape> shapePrimitiveRdd = sparkContext.newAPIHadoopFile(
                filePath,
                ShapeInputFormat.class,
                ShapeKey.class,
                PrimitiveShape.class,
                sparkContext.hadoopConfiguration()
        );
        shapeRDD = shapePrimitiveRdd.map(PrimitiveToShape);
    }

    private static final Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry> PrimitiveToShape
            = new Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry>(){
        public Geometry call(Tuple2<ShapeKey, PrimitiveShape> primitiveTuple) {
            Geometry shape = null;
            // parse bytes to shape
            try{
                shape = primitiveTuple._2().getShape(geometryFactory);
                if(primitiveTuple._2().getPrimitiveAttribute() != null){
                    String attributes = primitiveTuple._2().generateAttributes();
                    attributes = primitiveTuple._1().getIndex() + "\t" + attributes;
                    shape.setUserData(attributes);
                }
            }catch (TypeUnknownException e){
                e.printStackTrace();
            }catch (IOException e){
                e.printStackTrace();
            }finally {
                return shape;
            }

        }
    };

    private final VoidFunction<Geometry> PrintShape = new VoidFunction<Geometry>() {
        public void call(Geometry shape) throws Exception {
            System.out.println(shape.toText());
        }
    };


    public JavaRDD<Geometry> getShapeRDD()
    {
        return this.shapeRDD;
    }

    public JavaRDD<Object> getSpatialRDD() {
        return shapeRDD.flatMap(new FlatMapFunction<Geometry, Object>()
        {
			@Override
			public Iterator<Object> call(Geometry spatialObject) throws Exception {
                List<Object> result = new ArrayList<Object>();
                if(spatialObject instanceof MultiPoint)
			    {
                    MultiPoint multiObjects = (MultiPoint)spatialObject;
                    for (int i=0;i<multiObjects.getNumGeometries();i++)
                    {
                        result.add(multiObjects.getGeometryN(i));
                    }
                }
                if(spatialObject instanceof MultiLineString)
                {
                    MultiLineString multiObjects = (MultiLineString)spatialObject;
                    for (int i=0;i<multiObjects.getNumGeometries();i++)
                    {
                        result.add(multiObjects.getGeometryN(i));
                    }
                }
                if (spatialObject instanceof MultiPolygon)
                {
                    MultiPolygon multiObjects = (MultiPolygon)spatialObject;
                    for (int i=0;i<multiObjects.getNumGeometries();i++)
                    {
                        result.add(multiObjects.getGeometryN(i));
                    }
                }
				return result.iterator();
			}
        	
        });
    }

}
