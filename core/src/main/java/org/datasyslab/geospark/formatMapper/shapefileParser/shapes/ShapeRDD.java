package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShpParseUtil;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.Serializable;

/**
 * Created by zongsizhang on 5/23/17.
 */
public class ShapeRDD implements Serializable{

    JavaRDD<Geometry> shapeRDD;

    JavaPairRDD<ShapeKey, BytesWritable> primitiveShapeRDD;

    public ShapeRDD(String filePath, JavaSparkContext sparkContext){
        JavaPairRDD<ShapeKey, PrimitiveShapeWritable> shapePrimitiveRdd = sparkContext.newAPIHadoopFile(
                filePath,
                ShapeInputFormat.class,
                ShapeKey.class,
                PrimitiveShapeWritable.class,
                new Configuration()
        );
        shapeRDD = shapePrimitiveRdd.map(PrimitiveToShape);
        //shapeRDD.foreach(PrintShape);
    }

    public static final Function<Tuple2<ShapeKey, PrimitiveShapeWritable>, Geometry> PrimitiveToShape
            = new Function<Tuple2<ShapeKey, PrimitiveShapeWritable>, Geometry>(){
        public Geometry call(Tuple2<ShapeKey, PrimitiveShapeWritable> primitiveTuple) throws Exception {
            Geometry shape = null;
            shape = ShpParseUtil.primitiveToShape(primitiveTuple._2().getPrimitiveRecord().getBytes());
            if(primitiveTuple._2().getPrimitiveAttribute() != null){
                DataInputStream dbfInputStream = new DataInputStream(
                        new ByteArrayInputStream(primitiveTuple._2().getPrimitiveAttribute().getBytes()));
                shape.setUserData(DbfParseUtil.primitiveToAttributes(dbfInputStream));
            }
            return shape;
        }
    };

    public static final VoidFunction<Geometry> PrintShape = new VoidFunction<Geometry>() {
        public void call(Geometry shape) throws Exception {
            System.out.println(shape.toText());
        }
    };

    public JavaRDD<Geometry> getShapeWritableRDD() {
        return shapeRDD;
    }

}
