package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import org.apache.commons.lang.IllegalClassException;
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

import static org.junit.Assert.*;

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

    @Test
    public void testSpatialJoinQuery() throws Exception {
        //Create rectangeRDD
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

        List<Tuple2<Envelope, List<Point>>> result = NewJoinQuery.SpatialJoinQuery(sc, pointRDD, rectangleRDD, true).collect();

        //Print result out;
        //结果是不是有问题啊, 为什么会有空的envelope? JTS 的contains 应该是不包括在边界上把..
        for(Tuple2<Envelope, List<Point>> t:result){
            System.out.print(t._1() + ":");
            for(Point p:t._2()) {
                System.out.print(p + ", ");
            }
            System.out.print("\n");
        }


        System.out.println(result.size());
    }

    @Test (expected=NullPointerException.class)
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
//        for(Tuple2<Envelope, List<Point>> t:result){
//            System.out.print(t._1() + ":");
//            for(Point p:t._2()) {
//                System.out.print(p + ", ");
//            }
//            System.out.print("\n");
//        }
    }


    @AfterClass
    public static void TearDown() {
        sc.stop();
    }


}