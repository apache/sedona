package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import scala.Tuple2;


public class PointJoinTest {
    public static JavaSparkContext sc;
    static Properties prop;
    static InputStream input;
    static String InputLocation;
    static Integer offset;
    static String splitter;
    static String gridType;
    static String indexType;
    static Integer numPartitions;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("PointJoin").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PointJoinTest.class.getClassLoader().getResourceAsStream("point.test.properties");
        InputLocation = "file://"+PointJoinTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        offset = 0;
        splitter = "";
        gridType = "";
        indexType = "";
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);

            //InputLocation = prop.getProperty("inputLocation");
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = prop.getProperty("splitter");
            gridType = prop.getProperty("gridType");
            indexType = prop.getProperty("indexType");
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    @Test
    public void testSpatialJoinQuery() throws Exception {

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        JoinQuery joinQuery = new JoinQuery(sc,pointRDD,rectangleRDD); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result = joinQuery.SpatialJoinQuery(pointRDD,rectangleRDD).collect();

        System.out.println(result.size());
    }

    @Test(expected = NullPointerException.class)
    public void testSpatialJoinQueryUsingIndexException() throws Exception {
        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, numPartitions);
        
        JoinQuery joinQuery = new JoinQuery(sc,pointRDD,rectangleRDD);
        
        //This should throw exception since the previous constructor doesn't build a grided RDD.
        List<Tuple2<Envelope, HashSet<Point>>> result = joinQuery.SpatialJoinQueryUsingIndex(pointRDD,rectangleRDD).collect();

    }

    @Test
    public void testSpatialJoinQueryUsingIndex() throws Exception {

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        pointRDD.buildIndex("strtree");

        JoinQuery joinQuery = new JoinQuery(sc,pointRDD,rectangleRDD);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = joinQuery.SpatialJoinQueryUsingIndex(pointRDD,rectangleRDD).collect();

        System.out.println(result.size());

    }



}