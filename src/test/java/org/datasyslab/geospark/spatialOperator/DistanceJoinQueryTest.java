package org.datasyslab.geospark.spatialOperator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;

import scala.Tuple2;


public class DistanceJoinQueryTest {
    public static JavaSparkContext sc;
    static Properties prop;
    static InputStream input;
    static String InputLocation;
    static Integer offset;
    static FileDataSplitter splitter;
    static GridType gridType;
    static IndexType indexType;
    static Integer numPartitions;
    static double distance;
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("JoinTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        prop = new Properties();
        input = DistanceJoinQueryTest.class.getClassLoader().getResourceAsStream("point.test.properties");
        InputLocation = "file://"+DistanceJoinQueryTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        offset = 2;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);

            //InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+DistanceJoinQueryTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.valueOf(prop.getProperty("splitter").toUpperCase());
            gridType = GridType.valueOf(prop.getProperty("gridType").toUpperCase());
            indexType = IndexType.valueOf(prop.getProperty("indexType").toUpperCase());
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            distance=Double.parseDouble(prop.getProperty("distance"));
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

    @Test
    public void testDistancelJoinQuery() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        PointRDD pointRDD2 = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        List<Tuple2<Point, HashSet<Point>>> result = DistanceJoin.SpatialJoinQueryWithoutIndex(sc, pointRDD, pointRDD2, distance).collect();

        assertEquals(pointRDD.getRawPointRDD().distinct().count(), pointRDD.getRawPointRDD().distinct().count());

    }

    @Test
    public void testDistancelJoinQueryWithIndex() throws Exception {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        PointRDD pointRDD2 = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        pointRDD.buildIndex(IndexType.RTREE);
        List<Tuple2<Point, List<Point>>> result = DistanceJoin.SpatialJoinQueryUsingIndex(sc, pointRDD, pointRDD2, distance).collect();

        assertEquals(pointRDD.getRawPointRDD().distinct().count(), pointRDD.getRawPointRDD().distinct().count());

    }


    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}
