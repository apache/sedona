package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Point;

import org.apache.commons.lang.IllegalClassException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by jinxuanwu on 1/3/16.
 */
public class PointRDDTest implements Serializable{
    public static JavaSparkContext sc;
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("JobTileMatchWithAcronymExpension").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    @Test
    public void testGetSampleNumbers() throws Exception {
        assertEquals(0, RDDSampleUtils.getSampleNumbers(2, 10));
        assertEquals(10, RDDSampleUtils.getSampleNumbers(2, 100));
        assertEquals(10, RDDSampleUtils.getSampleNumbers(2, 101));
        assertEquals(10, RDDSampleUtils.getSampleNumbers(2, 104));
        assertEquals(10, RDDSampleUtils.getSampleNumbers(2, 110));
        assertEquals(12, RDDSampleUtils.getSampleNumbers(2, 120));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(2, 1010));
        assertEquals(110, RDDSampleUtils.getSampleNumbers(2, 1110));
        assertEquals(100, RDDSampleUtils.getSampleNumbers(10, 1000));
        assertEquals(99, RDDSampleUtils.getSampleNumbers(9, 1000));
        assertEquals(110, RDDSampleUtils.getSampleNumbers(10, 1100));
    }



    @Test
    public void testConstructor() throws Exception {
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

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        //todo: Set this to debug level
        for (Double d : pointRDD.grid) {
            System.out.println(d);
        }


        //todo: Move this into log4j.
        Map<Integer, Object> map = pointRDD.gridPointRDD.countByKey();
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / pointRDD.totalNumberOfRecords;
            System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }

    @Test(expected=IllegalClassException.class)
    public void testBuildIndexWithoutSetGrid() throws Exception {
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

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, numPartitions);
        pointRDD.buildIndex("R-Tree");
    }


    @Test
    public void testBuildIndex() throws Exception {
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

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        pointRDD.buildIndex("R-Tree");
        List<Point> result = pointRDD.indexedRDD.take(1).get(0)._2().query(pointRDD.boundaryEnvelope);
    }

    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}