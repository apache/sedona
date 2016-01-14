package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jinxuanwu on 1/13/16.
 */
public class KNNQueryTest {
    private static JavaSparkContext sc;
    private static SparkConf conf;
    private static ArrayList<Point> pointList = new ArrayList<Point>();
    private static Point queryPoint;
    @BeforeClass
    public static void onceExecutedBeforeAll(){
        SparkConf conf = new SparkConf().setAppName("JoinTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        GeometryFactory geometryFactory = new GeometryFactory();
        for(int i = 1; i < 10; i++) {
            Point point = geometryFactory.createPoint(new Coordinate(0.1 * i, 0.1 * i));
            pointList.add(point);
        }
        queryPoint = geometryFactory.createPoint(new Coordinate(0, 0));
    }

    @AfterClass
    public static void teardown(){
        sc.stop();
    }

    @Test
    public void testSpatialKnnQuery() throws Exception {
        JavaRDD<Point> pointJavaRDD = sc.parallelize(pointList);
        PointRDD pointRDD = new PointRDD(pointJavaRDD);

        List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, sc.broadcast(queryPoint), 1);

        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getCoordinate().x, 0.1, 0.001);
        assertEquals(result.get(0).getCoordinate().y, 0.1, 0.001);



    }
}