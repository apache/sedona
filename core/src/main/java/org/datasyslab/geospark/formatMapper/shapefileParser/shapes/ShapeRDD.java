package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by zongsizhang on 5/23/17.
 */
public class ShapeRDD implements Serializable{

    /** shape collection */
    private JavaRDD<Geometry> shapeRDD = null;

    public static GeometryFactory geometryFactory;

    /** field descriptor of  */

    public ShapeRDD(String filePath, JavaSparkContext sparkContext){
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



    public JavaRDD<Geometry> getShapeWritableRDD() {
        return shapeRDD;
    }

}
