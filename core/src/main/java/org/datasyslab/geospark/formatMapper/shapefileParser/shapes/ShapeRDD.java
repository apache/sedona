package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.DbfParseUtil;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.Serializable;

/**
 * Created by zongsizhang on 5/23/17.
 */
public class ShapeRDD implements Serializable{

    JavaRDD<Geometry> shapeRDD;

    public static GeometryFactory geometryFactory;

    public ShapeRDD(String filePath, JavaSparkContext sparkContext){
        geometryFactory = new GeometryFactory();
        JavaPairRDD<ShapeKey, PrimitiveShape> shapePrimitiveRdd = sparkContext.newAPIHadoopFile(
                filePath,
                ShapeInputFormat.class,
                ShapeKey.class,
                PrimitiveShape.class,
                new Configuration()
        );
        shapeRDD = shapePrimitiveRdd.map(PrimitiveToShape);
    }

    private static final Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry> PrimitiveToShape
            = new Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry>(){
        public Geometry call(Tuple2<ShapeKey, PrimitiveShape> primitiveTuple) throws Exception {
            Geometry shape = null;
            // parse bytes to shape
            shape = primitiveTuple._2().getShape(geometryFactory);
            // parse bytes to attributes
            if(primitiveTuple._2().getPrimitiveAttribute() != null){
                DataInputStream dbfInputStream = new DataInputStream(
                        new ByteArrayInputStream(primitiveTuple._2().getPrimitiveAttribute().getBytes()));
                shape.setUserData(DbfParseUtil.primitiveToAttributes(dbfInputStream));
            }
            return shape;
        }
    };

    private static final VoidFunction<Geometry> PrintShape = new VoidFunction<Geometry>() {
        public void call(Geometry shape) throws Exception {
            System.out.println(shape.toText());
        }
    };



    public JavaRDD<Geometry> getShapeWritableRDD() {
        return shapeRDD;
    }

}
