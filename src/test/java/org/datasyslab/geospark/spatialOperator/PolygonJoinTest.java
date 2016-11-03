package org.datasyslab.geospark.spatialOperator;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Polygon;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import scala.Tuple2;


public class PolygonJoinTest {
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
        SparkConf conf = new SparkConf().setAppName("PolygonJoin").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PolygonJoinTest.class.getClassLoader().getResourceAsStream("polygon.test.properties");
        InputLocation = "file://"+PolygonJoinTest.class.getClassLoader().getResource("primaryroads-polygon.csv").getPath();
        offset = 0;
        splitter = "";
        gridType = "";
        indexType = "";
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);

            //InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+PolygonJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
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

    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, numPartitions);

        PolygonRDD objectRDD = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

        JoinQuery joinQuery = new JoinQuery(sc,objectRDD,polygonRDD); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result = joinQuery.SpatialJoinQuery(objectRDD,polygonRDD).collect();
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
    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, numPartitions);

    	PolygonRDD objectRDD = new PolygonRDD(sc, InputLocation, offset, splitter, numPartitions);
        
        JoinQuery joinQuery = new JoinQuery(sc,objectRDD,polygonRDD);
        
        //This should throw exception since the previous constructor doesn't build a grided RDD.
        List<Tuple2<Polygon, HashSet<Polygon>>> result = joinQuery.SpatialJoinQueryUsingIndex(objectRDD,polygonRDD).collect();

    }

    @Test
    public void testSpatialJoinQueryUsingIndex() throws Exception {

    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, numPartitions);

    	PolygonRDD objectRDD = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);

    	objectRDD.buildIndex("strtree");

        JoinQuery joinQuery = new JoinQuery(sc,objectRDD,polygonRDD);
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result = joinQuery.SpatialJoinQueryUsingIndex(objectRDD,polygonRDD).collect();
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

        PolygonRDD queryWindowRDD1 = new PolygonRDD(sc, InputLocation, offset, splitter);

        PolygonRDD objectRDD1 = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, 20);

        JoinQuery joinQuery1 = new JoinQuery(sc,objectRDD1,queryWindowRDD1); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result1 = joinQuery1.SpatialJoinQuery(objectRDD1,queryWindowRDD1).collect();
        
        
        PolygonRDD queryWindowRDD2 = new PolygonRDD(sc, InputLocation, offset, splitter);
        
        PolygonRDD objectRDD2 = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, 30);

        JoinQuery joinQuery2 = new JoinQuery(sc,objectRDD2,queryWindowRDD2); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result2 = joinQuery2.SpatialJoinQuery(objectRDD2,queryWindowRDD2).collect();
        
        
        PolygonRDD queryWindowRDD3 = new PolygonRDD(sc, InputLocation, offset, splitter);
        
        PolygonRDD objectRDD3 = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, 40);

        JoinQuery joinQuery3 = new JoinQuery(sc,objectRDD3,queryWindowRDD3); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result3 = joinQuery3.SpatialJoinQuery(objectRDD3,queryWindowRDD3).collect();
        
        
        PolygonRDD queryWindowRDD4 = new PolygonRDD(sc, InputLocation, offset, splitter);

        PolygonRDD objectRDD4 = new PolygonRDD(sc, InputLocation, offset, splitter, "equalgrid", 20);

        JoinQuery joinQuery4 = new JoinQuery(sc,objectRDD4,queryWindowRDD4); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result4 = joinQuery4.SpatialJoinQuery(objectRDD4,queryWindowRDD4).collect();
        
        
        PolygonRDD queryWindowRDD5 = new PolygonRDD(sc, InputLocation, offset, splitter);
        
        PolygonRDD objectRDD5 = new PolygonRDD(sc, InputLocation, offset, splitter, "rtree", 20);

        JoinQuery joinQuery5 = new JoinQuery(sc,objectRDD5,queryWindowRDD5); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result5 = joinQuery5.SpatialJoinQuery(objectRDD5,queryWindowRDD5).collect();
        
        
        PolygonRDD queryWindowRDD6 = new PolygonRDD(sc, InputLocation, offset, splitter);
        
        PolygonRDD objectRDD6 = new PolygonRDD(sc, InputLocation, offset, splitter, "voronoi", 20);

        JoinQuery joinQuery6 = new JoinQuery(sc,objectRDD6,queryWindowRDD6); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result6 = joinQuery6.SpatialJoinQuery(objectRDD6,queryWindowRDD6).collect();
        
        
        PolygonRDD queryWindowRDD7 = new PolygonRDD(sc, InputLocation, offset, splitter);

        PolygonRDD objectRDD7 = new PolygonRDD(sc, InputLocation, offset, splitter, "equalgrid", 20);
        
        objectRDD7.buildIndex("strtree");

        JoinQuery joinQuery7 = new JoinQuery(sc,objectRDD7,queryWindowRDD7); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result7 = joinQuery7.SpatialJoinQueryUsingIndex(objectRDD7,queryWindowRDD7).collect();
        
        
        PolygonRDD queryWindowRDD8 = new PolygonRDD(sc, InputLocation, offset, splitter);
        
        PolygonRDD objectRDD8 = new PolygonRDD(sc, InputLocation, offset, splitter, "rtree", 40);

        objectRDD8.buildIndex("strtree");
        
        JoinQuery joinQuery8 = new JoinQuery(sc,objectRDD8,queryWindowRDD8); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result8 = joinQuery8.SpatialJoinQueryUsingIndex(objectRDD8,queryWindowRDD8).collect();
        
        
        PolygonRDD queryWindowRDD9 = new PolygonRDD(sc, InputLocation, offset, splitter);
        
        PolygonRDD objectRDD9 = new PolygonRDD(sc, InputLocation, offset, splitter, "voronoi", 20);

        objectRDD9.buildIndex("strtree");
        
        JoinQuery joinQuery9 = new JoinQuery(sc,objectRDD9,queryWindowRDD9); 
        
        List<Tuple2<Polygon, HashSet<Polygon>>> result9 = joinQuery9.SpatialJoinQueryUsingIndex(objectRDD9,queryWindowRDD9).collect();
        if (result1.size()!=result2.size() || result1.size()!=result3.size()
        		|| result1.size()!=result4.size()|| result1.size()!=result5.size()
        		|| result1.size()!=result6.size()|| result1.size()!=result7.size()
        		|| result1.size()!=result8.size()|| result1.size()!=result9.size()
        		)
        {
        	System.out.println("-----Polygon join results are not consistent-----");
        	System.out.println(result1.size());
        	System.out.println(result2.size());
        	System.out.println(result3.size());
        	System.out.println(result4.size());
        	System.out.println(result5.size());
        	System.out.println(result6.size());
        	System.out.println(result7.size());
        	System.out.println(result8.size());
        	System.out.println(result9.size());
        	System.out.println("-----Polygon join results are not consistent--Done---");
        	throw new Exception("Polygon join results are not consistent!");
        }
        
        
    }


}