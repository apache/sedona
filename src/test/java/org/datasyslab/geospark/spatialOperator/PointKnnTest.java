package org.datasyslab.geospark.spatialOperator;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import io.netty.handler.codec.http.HttpContentEncoder.Result;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PointRDDTest;
import org.datasyslab.geospark.spatialRDD.RectangleRDDTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;


public class PointKnnTest {
    public static JavaSparkContext sc;
    static Properties prop;
    static InputStream input;
    static String InputLocation;
    static Integer offset;
    static String splitter;
    static String indexType;
    static Integer numPartitions;
    static int loopTimes;
    static Point queryPoint;
    @BeforeClass
    public static void onceExecutedBeforeAll(){
        SparkConf conf = new SparkConf().setAppName("PointKnn").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PointKnnTest.class.getClassLoader().getResourceAsStream("point.test.properties");

        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
        InputLocation = "file://"+PointKnnTest.class.getClassLoader().getResource("primaryroads.csv").getPath();

        offset = 0;
        splitter = "";
        indexType = "";
        numPartitions = 0;
        GeometryFactory fact=new GeometryFactory();
        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+PointKnnTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = prop.getProperty("splitter");
            indexType = prop.getProperty("indexType");
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            loopTimes=5;
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
        queryPoint = fact.createPoint(new Coordinate(-84.01, 34.01));
    }

    @AfterClass
    public static void teardown(){
        sc.stop();
    }

    @Test
    public void testSpatialKnnQuery() throws Exception {
    	PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter);

    	for(int i=0;i<loopTimes;i++)
    	{
    		List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, 5);
    		assert result.size()>-1;
    		assert result.get(0).getUserData().toString()!=null;
    		//System.out.println(result.get(0).getUserData().toString());
    	}
    	
    }
    @Test
    public void testSpatialKnnQueryUsingIndex() throws Exception {
    	PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter);
    	pointRDD.buildIndex("rtree");
    	for(int i=0;i<loopTimes;i++)
    	{
    		List<Point> result = KNNQuery.SpatialKnnQueryUsingIndex(pointRDD, queryPoint, 5);
    		assert result.size()>-1;
    		assert result.get(0).getUserData().toString()!=null;
    		//System.out.println(result.get(0).getUserData().toString());
    	}

    }

}