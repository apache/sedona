package org.datasyslab.geospark.spatialOperator;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Envelope;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDDTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import scala.Tuple2;

public class RectangleJoinTest {
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
        SparkConf conf = new SparkConf().setAppName("RectangleJoin").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = RectangleJoinTest.class.getClassLoader().getResourceAsStream("rectangle.test.properties");
        InputLocation = "file://"+RectangleJoinTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        offset = 0;
        splitter = "";
        gridType = "";
        indexType = "";
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);

            //InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+RectangleJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
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

        RectangleRDD objectRDD = new RectangleRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        JoinQuery joinQuery = new JoinQuery(sc,objectRDD,rectangleRDD); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result = joinQuery.SpatialJoinQuery(objectRDD,rectangleRDD).collect();
        assert result.get(0)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }

    @Test(expected = NullPointerException.class)
    public void testSpatialJoinQueryUsingIndexException() throws Exception {
        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        RectangleRDD objectRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);
        
        JoinQuery joinQuery = new JoinQuery(sc,objectRDD,rectangleRDD);
        
        //This should throw exception since the previous constructor doesn't build a grided RDD.
        List<Tuple2<Envelope, HashSet<Envelope>>> result = joinQuery.SpatialJoinQueryUsingIndex(objectRDD,rectangleRDD).collect();

    }

    @Test
    public void testSpatialJoinQueryUsingIndex() throws Exception {

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocation, offset, splitter, numPartitions);

        RectangleRDD objectRDD = new RectangleRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        objectRDD.buildIndex("strtree");

        JoinQuery joinQuery = new JoinQuery(sc,objectRDD,rectangleRDD);
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result = joinQuery.SpatialJoinQueryUsingIndex(objectRDD,rectangleRDD).collect();
        assert result.get(0)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }

    }
    @Test
    public void testJoinCorrectness() throws Exception {

        RectangleRDD rectangleRDD1 = new RectangleRDD(sc, InputLocation, offset, splitter);

        RectangleRDD objectRDD1 = new RectangleRDD(sc, InputLocation, offset, splitter, gridType, 20);

        JoinQuery joinQuery1 = new JoinQuery(sc,objectRDD1,rectangleRDD1); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result1 = joinQuery1.SpatialJoinQuery(objectRDD1,rectangleRDD1).collect();
        
        
        RectangleRDD rectangleRDD2 = new RectangleRDD(sc, InputLocation, offset, splitter);
        
        RectangleRDD objectRDD2 = new RectangleRDD(sc, InputLocation, offset, splitter, gridType, 30);

        JoinQuery joinQuery2 = new JoinQuery(sc,objectRDD2,rectangleRDD2); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result2 = joinQuery2.SpatialJoinQuery(objectRDD2,rectangleRDD2).collect();
        
        
        RectangleRDD rectangleRDD3 = new RectangleRDD(sc, InputLocation, offset, splitter);
        
        RectangleRDD objectRDD3 = new RectangleRDD(sc, InputLocation, offset, splitter, gridType, 40);

        JoinQuery joinQuery3 = new JoinQuery(sc,objectRDD3,rectangleRDD3); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result3 = joinQuery3.SpatialJoinQuery(objectRDD3,rectangleRDD3).collect();
        
        
        RectangleRDD rectangleRDD4 = new RectangleRDD(sc, InputLocation, offset, splitter);

        RectangleRDD objectRDD4 = new RectangleRDD(sc, InputLocation, offset, splitter, "equalgrid", 20);

        JoinQuery joinQuery4 = new JoinQuery(sc,objectRDD4,rectangleRDD4); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result4 = joinQuery4.SpatialJoinQuery(objectRDD4,rectangleRDD4).collect();
        
        
        RectangleRDD rectangleRDD5 = new RectangleRDD(sc, InputLocation, offset, splitter);
        
        RectangleRDD objectRDD5 = new RectangleRDD(sc, InputLocation, offset, splitter, "rtree", 20);

        JoinQuery joinQuery5 = new JoinQuery(sc,objectRDD5,rectangleRDD5); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result5 = joinQuery5.SpatialJoinQuery(objectRDD5,rectangleRDD5).collect();
        
        
        RectangleRDD rectangleRDD6 = new RectangleRDD(sc, InputLocation, offset, splitter);
        
        RectangleRDD objectRDD6 = new RectangleRDD(sc, InputLocation, offset, splitter, "voronoi", 20);

        JoinQuery joinQuery6 = new JoinQuery(sc,objectRDD6,rectangleRDD6); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result6 = joinQuery6.SpatialJoinQuery(objectRDD6,rectangleRDD6).collect();
        
        
        RectangleRDD rectangleRDD7 = new RectangleRDD(sc, InputLocation, offset, splitter);

        RectangleRDD objectRDD7 = new RectangleRDD(sc, InputLocation, offset, splitter, "equalgrid", 20);
        
        objectRDD7.buildIndex("strtree");

        JoinQuery joinQuery7 = new JoinQuery(sc,objectRDD7,rectangleRDD7); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result7 = joinQuery7.SpatialJoinQueryUsingIndex(objectRDD7,rectangleRDD7).collect();
        
        
        RectangleRDD rectangleRDD8 = new RectangleRDD(sc, InputLocation, offset, splitter);
        
        RectangleRDD objectRDD8 = new RectangleRDD(sc, InputLocation, offset, splitter, "rtree", 30);

        objectRDD8.buildIndex("strtree");
        
        JoinQuery joinQuery8 = new JoinQuery(sc,objectRDD8,rectangleRDD8); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result8 = joinQuery8.SpatialJoinQueryUsingIndex(objectRDD8,rectangleRDD8).collect();
        
        
        RectangleRDD rectangleRDD9 = new RectangleRDD(sc, InputLocation, offset, splitter);
        
        RectangleRDD objectRDD9 = new RectangleRDD(sc, InputLocation, offset, splitter, "voronoi", 20);

        objectRDD9.buildIndex("strtree");
        
        JoinQuery joinQuery9 = new JoinQuery(sc,objectRDD9,rectangleRDD9); 
        
        List<Tuple2<Envelope, HashSet<Envelope>>> result9 = joinQuery9.SpatialJoinQueryUsingIndex(objectRDD9,rectangleRDD9).collect();
        if (result1.size()!=result2.size() || result1.size()!=result3.size()
        		|| result1.size()!=result4.size()|| result1.size()!=result5.size()
        		|| result1.size()!=result6.size()|| result1.size()!=result7.size()
        		|| result1.size()!=result8.size()|| result1.size()!=result9.size()
        		)
        {
        	System.out.println("-----Rectangle join results are not consistent-----");
        	System.out.println(result1.size());
        	System.out.println(result2.size());
        	System.out.println(result3.size());
        	System.out.println(result4.size());
        	System.out.println(result5.size());
        	System.out.println(result6.size());
        	System.out.println(result7.size());
        	System.out.println(result8.size());
        	System.out.println(result9.size());
        	System.out.println("-----Rectangle join results are not consistent--Done---");
        	throw new Exception("Rectangle join results are not consistent!");
        }
        
        
    }



}