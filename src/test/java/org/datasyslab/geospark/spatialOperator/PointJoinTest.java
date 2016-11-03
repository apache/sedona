package org.datasyslab.geospark.spatialOperator;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

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
    static String InputLocationQueryWindow;
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
            InputLocation = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryWindow = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("queryWindowSet")).getPath();
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

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        JoinQuery joinQuery = new JoinQuery(sc,pointRDD,rectangleRDD); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result = joinQuery.SpatialJoinQuery(pointRDD,rectangleRDD).collect();
        assert result.get(1)._1().getUserData()!=null;
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
        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, numPartitions);
        
        JoinQuery joinQuery = new JoinQuery(sc,pointRDD,rectangleRDD);
        
        //This should throw exception since the previous constructor doesn't build a grided RDD.
        List<Tuple2<Envelope, HashSet<Point>>> result = joinQuery.SpatialJoinQueryUsingIndex(pointRDD,rectangleRDD).collect();

    }

    @Test
    public void testSpatialJoinQueryUsingIndex() throws Exception {

        RectangleRDD rectangleRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, numPartitions);

        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        pointRDD.buildIndex("strtree");

        JoinQuery joinQuery = new JoinQuery(sc,pointRDD,rectangleRDD);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = joinQuery.SpatialJoinQueryUsingIndex(pointRDD,rectangleRDD).collect();
        assert result.get(1)._1().getUserData()!=null;
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

        RectangleRDD rectangleRDD1 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);

        PointRDD pointRDD1 = new PointRDD(sc, InputLocation, offset, splitter, gridType, 20);

        JoinQuery joinQuery1 = new JoinQuery(sc,pointRDD1,rectangleRDD1); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result1 = joinQuery1.SpatialJoinQuery(pointRDD1,rectangleRDD1).collect();
        
        
        RectangleRDD rectangleRDD2 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);
        
        PointRDD pointRDD2 = new PointRDD(sc, InputLocation, offset, splitter, gridType, 40);

        JoinQuery joinQuery2 = new JoinQuery(sc,pointRDD2,rectangleRDD2); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result2 = joinQuery2.SpatialJoinQuery(pointRDD2,rectangleRDD2).collect();
        
        
        RectangleRDD rectangleRDD3 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);
        
        PointRDD pointRDD3 = new PointRDD(sc, InputLocation, offset, splitter, gridType, 80);

        JoinQuery joinQuery3 = new JoinQuery(sc,pointRDD3,rectangleRDD3); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result3 = joinQuery3.SpatialJoinQuery(pointRDD3,rectangleRDD3).collect();
        
        
        RectangleRDD rectangleRDD4 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);

        PointRDD pointRDD4 = new PointRDD(sc, InputLocation, offset, splitter, "equalgrid", 20);

        JoinQuery joinQuery4 = new JoinQuery(sc,pointRDD4,rectangleRDD4); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result4 = joinQuery4.SpatialJoinQuery(pointRDD4,rectangleRDD4).collect();
        
        
        RectangleRDD rectangleRDD5 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);
        
        PointRDD pointRDD5 = new PointRDD(sc, InputLocation, offset, splitter, "rtree", 20);

        JoinQuery joinQuery5 = new JoinQuery(sc,pointRDD5,rectangleRDD5); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result5 = joinQuery5.SpatialJoinQuery(pointRDD5,rectangleRDD5).collect();
        
        
        RectangleRDD rectangleRDD6 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);
        
        PointRDD pointRDD6 = new PointRDD(sc, InputLocation, offset, splitter, "voronoi", 20);

        JoinQuery joinQuery6 = new JoinQuery(sc,pointRDD6,rectangleRDD6); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result6 = joinQuery6.SpatialJoinQuery(pointRDD6,rectangleRDD6).collect();
        
        
        RectangleRDD rectangleRDD7 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);

        PointRDD pointRDD7 = new PointRDD(sc, InputLocation, offset, splitter, "equalgrid", 20);
        
        pointRDD7.buildIndex("strtree");

        JoinQuery joinQuery7 = new JoinQuery(sc,pointRDD7,rectangleRDD7); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result7 = joinQuery7.SpatialJoinQueryUsingIndex(pointRDD7,rectangleRDD7).collect();
        
        
        RectangleRDD rectangleRDD8 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);
        
        PointRDD pointRDD8 = new PointRDD(sc, InputLocation, offset, splitter, "rtree", 80);

        pointRDD8.buildIndex("strtree");
        
        JoinQuery joinQuery8 = new JoinQuery(sc,pointRDD8,rectangleRDD8); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result8 = joinQuery8.SpatialJoinQueryUsingIndex(pointRDD8,rectangleRDD8).collect();
        
        
        RectangleRDD rectangleRDD9 = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter);
        
        PointRDD pointRDD9 = new PointRDD(sc, InputLocation, offset, splitter, "voronoi", 20);

        pointRDD9.buildIndex("strtree");
        
        JoinQuery joinQuery9 = new JoinQuery(sc,pointRDD9,rectangleRDD9); 
        
        List<Tuple2<Envelope, HashSet<Point>>> result9 = joinQuery9.SpatialJoinQueryUsingIndex(pointRDD9,rectangleRDD9).collect();
        if (result1.size()!=result2.size() || result1.size()!=result3.size()
        		|| result1.size()!=result4.size()|| result1.size()!=result5.size()
        		|| result1.size()!=result6.size()|| result1.size()!=result7.size()
        		|| result1.size()!=result8.size()|| result1.size()!=result9.size()
        		)
        {
        	System.out.println("-----Point join results are not consistent-----");
        	System.out.println(result1.size());
        	System.out.println(result2.size());
        	System.out.println(result3.size());
        	System.out.println(result4.size());
        	System.out.println(result5.size());
        	System.out.println(result6.size());
        	System.out.println(result7.size());
        	System.out.println(result8.size());
        	System.out.println(result9.size());
        	System.out.println("-----Point join results are not consistent--Done---");
        	throw new Exception("Point join results are not consistent!");
        }
        
        
    }

}