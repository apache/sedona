package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import scala.Tuple2;

/**
 * Created by jinxuanwu on 1/5/16.
 */
public class NewJoinQueryTest {
    public static JavaSparkContext sc;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("JoinTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    @Test
    public void testSpatialJoinQuery() throws Exception {
        //Create rectangeRDD
        Properties prop = new Properties();
        InputStream input = getClass().getClassLoader().getResourceAsStream("point.test.properties");
        String InputLocation = "file://"+NewJoinQueryTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        Integer offset = 0;
        String splitter = "";
        String gridType = "";
        String indexType = "";
        Integer numPartitions = 0;

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

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        List<Tuple2<Envelope, List<Point>>> result = NewJoinQuery.SpatialJoinQueryWithOutIndex(sc, pointRDD, rectangleRDD, true).collect();

        System.out.println(result.size());
    }

    @Test(expected = NullPointerException.class)
    public void testSpatialJoinQueryUsingIndexException() throws Exception {
        Properties prop = new Properties();
        InputStream input = getClass().getClassLoader().getResourceAsStream("point.test.properties");
        String InputLocation = "";
        Integer offset = 0;
        String splitter = "";
        String gridType = "";
        String indexType = "";
        Integer numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            InputLocation = prop.getProperty("inputLocation");
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

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        //This should throw exception since the previous constructor doesn't build a grided RDD.
        List<Tuple2<Envelope, List<Point>>> result = NewJoinQuery.SpatialJoinQueryUsingIndex(sc, pointRDD, rectangleRDD, "R-TREE").collect();

    }

    @Test
    public void testSpatialJoinQueryUsingIndex() throws Exception {
        Properties prop = new Properties();
        InputStream input = getClass().getClassLoader().getResourceAsStream("point.test.properties");
        String InputLocation = "";
        Integer offset = 0;
        String splitter = "";
        String gridType = "";
        String indexType = "";
        Integer numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            InputLocation = prop.getProperty("inputLocation");
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

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        pointRDD.buildIndex("R-Tree");

        List<Tuple2<Envelope, List<Point>>> result = NewJoinQuery.SpatialJoinQueryUsingIndex(sc, pointRDD, rectangleRDD, "R-Tree").collect();

        System.out.println(result.size());

    }


}