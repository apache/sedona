package org.datasyslab.geospark.spatialRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by jinxuanwu on 1/5/16.
 */
public class RectangleRDDTest {
    public static JavaSparkContext sc;
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("JobTileMatchWithAcronymExpension").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
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

            RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        //todo: Set this to debug level
        for (Double d : rectangleRDD.grid) {
            System.out.println(d);
        }


        //todo: Move this into log4j.
        Map<Integer, Object> map = rectangleRDD.gridRectangleRDD.countByKey();
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / rectangleRDD.totalNumberofRecords;
            System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }

    @Test
    public void testDouble(){
        int result;
        Double d1 = -81.126591;
        Double d2 = Double.parseDouble("-81.126591");
        result = Double.compare(d1.doubleValue(), d2.doubleValue());
    }

    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}