package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;


/**
 * Created by jinxuanwu on 1/13/16.
 */
public class RangeQueryTest {
    private static JavaSparkContext sc;
    private static SparkConf sparkConf;
    private static ArrayList<Point> pointList = new ArrayList<Point>();
    private static ArrayList<Envelope> rectangleList = new ArrayList<Envelope>();
    private static ArrayList<Polygon> polygonList = new ArrayList<Polygon>();
    private static Envelope queryEnvelope;
    private static Polygon queryPolygon;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("JoinTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        GeometryFactory geometryFactory = new GeometryFactory();

        Point point = geometryFactory.createPoint(new Coordinate(0.0, 0.0));
        pointList.add(point);

        Envelope envelope = new Envelope(0.0, 1.0, 0.0, 1.0);
        rectangleList.add(envelope);

        Polygon polygon = (Polygon) geometryFactory.toGeometry(envelope);
        polygonList.add(polygon);

        queryEnvelope = new Envelope(0.0, 2.0, 0.0, 2.0);

        queryPolygon = (Polygon) geometryFactory.toGeometry(queryEnvelope);
    }

    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    @Test
    public void testSpatialRangeQuery() throws Exception {
        JavaRDD<Point> pointRDD = sc.parallelize(pointList);
        PointRDD targetRDD = new PointRDD(pointRDD);
        int resultSize = RangeQuery.SpatialRangeQuery(targetRDD, queryEnvelope, 0).getRawPointRDD().collect().size();
        assertEquals(resultSize, 1);
    }

    @Test
    public void testSpatialRangeQuery1() throws Exception {
        JavaRDD<Envelope> rectangleRDD = sc.parallelize(rectangleList);
        RectangleRDD targetRDD = new RectangleRDD(rectangleRDD);
        int resultSize = RangeQuery.SpatialRangeQuery(targetRDD, queryEnvelope, 0).getRawRectangleRDD().collect().size();
        assertEquals(resultSize, 1);
    }

    @Test
    public void testSpatialRangeQuery2() throws Exception {
        JavaRDD<Polygon> polygonRDD = sc.parallelize(polygonList);
        PolygonRDD targetRDD = new PolygonRDD(polygonRDD);
        int resultSize = RangeQuery.SpatialRangeQuery(targetRDD, queryEnvelope, 0).getRawPolygonRDD().collect().size();
    }
    //todo: Leave space for polygon query window
    @Test
    public void testSpatialRangeQuery3() throws Exception {

    }

    @Test
    public void testSpatialRangeQuery4() throws Exception {

    }

    @Test
    public void testSpatialRangeQuery5() throws Exception {

    }
}