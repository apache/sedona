package org.datasyslab.geospark.spatialRDD;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.commons.lang.IllegalClassException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class PolygonRDDTest {
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
        SparkConf conf = new SparkConf().setAppName("PolygonTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PolygonRDDTest.class.getClassLoader().getResourceAsStream("polygon.test.properties");
        InputLocation = "file://"+PolygonRDDTest.class.getClassLoader().getResource("primaryroads-polygon.csv").getPath();
        offset = 0;
        splitter = "";
        gridType = "";
        indexType = "";
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            //InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://"+PolygonRDDTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
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




    /*
        This test case will load a sample data file and
     */
    @Test
    public void testConstructor() throws Exception {
        //The grid type is X right now.
        PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        //todo: Set this to debug level


        //todo: Move this into log4j.
        Map<Integer, Long> map = polygonRDD.gridPolygonRDD.countByKey();
        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / polygonRDD.totalNumberOfRecords;
            //System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }

    /*
     *  This test case test whether the X-Y grid can be build correctly.
     */
    /*
    @Test
    public void testXYGrid() throws Exception {
        //The grid type is X right now.
        PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, "X-Y", 10);
        //todo: Set this to debug level
        for (EnvelopeWithGrid d : polygonRDD.grids) {
            System.out.println("PolygonRDD grids: "+d.grid);
        }

        //todo: Move this into log4j.
        Map<Integer, Object> map = polygonRDD.gridPolygonRDD.countByKey();
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / polygonRDD.totalNumberOfRecords;
            System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }*/
    /*
     *  This test case test whether the equal size grids can be build correctly.
     */
    @Test
    public void testEqualSizeGridsSpatialPartitioing() throws Exception {
    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, "equalgrid", 10);
        for (EnvelopeWithGrid d : polygonRDD.grids) {
        	System.out.println("PolygonRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = polygonRDD.gridPolygonRDD.countByKey();

        System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / polygonRDD.totalNumberOfRecords;
            System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }
    /*
     *  This test case test whether the Hilbert Curve grid can be build correctly.
     */
    @Test
    public void testHilbertCurveSpatialPartitioing() throws Exception {
    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, "hilbert", 10);
        for (EnvelopeWithGrid d : polygonRDD.grids) {
        	//System.out.println("PolygonRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = polygonRDD.gridPolygonRDD.countByKey();

       // System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / polygonRDD.totalNumberOfRecords;
            //System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }
    /*
     *  This test case test whether the STR-Tree grid can be build correctly.
     */
    @Test
    public void testRTreeSpatialPartitioing() throws Exception {
    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, "rtree", 10);
        for (EnvelopeWithGrid d : polygonRDD.grids) {
        	//System.out.println("PolygonRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = polygonRDD.gridPolygonRDD.countByKey();

        //System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / polygonRDD.totalNumberOfRecords;
            //System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }
    /*
     *  This test case test whether the Voronoi grid can be build correctly.
     */
    @Test
    public void testVoronoiSpatialPartitioing() throws Exception {
    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, "voronoi", 10);
        for (EnvelopeWithGrid d : polygonRDD.grids) {
        	//System.out.println("PolygonRDD spatial partitioning grids: "+d.grid);
        }
        //todo: Move this into log4j.
        Map<Integer, Long> map = polygonRDD.gridPolygonRDD.countByKey();

        //System.out.println(map.size());

        for (Entry<Integer, Long> entry : map.entrySet()) {
            Long number = (Long) entry.getValue();
            Double percentage = number.doubleValue() / polygonRDD.totalNumberOfRecords;
            //System.out.println(entry.getKey() + " : " + String.format("%.4f", percentage));
        }
    }


    /*
     * If we try to build a index on a rawPolygonRDD which is not construct with grid. We shall see an error.
     */
    @Test//(expected=IllegalClassException.class)
    public void testBuildIndexWithoutSetGrid() throws Exception {
        PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, numPartitions);
        polygonRDD.buildIndex("R-Tree");
    }

    /*
        Test build Index.
     */
    @Test
    public void testBuildIndex() throws Exception {
        PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
        polygonRDD.buildIndex("R-Tree");
        List<Polygon> result = polygonRDD.indexedRDD.take(1).get(0)._2().query(polygonRDD.boundaryEnvelope);
        for(Polygon e: result) {
            //System.out.println(e.getEnvelopeInternal());
        }
    }
    
    /*
    Test build Index.
 */
@Test
public void testMBR() throws Exception {
    PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, numPartitions);
    RectangleRDD rectangleRDD=polygonRDD.MinimumBoundingRectangle();
    List<Envelope> result = rectangleRDD.rawRectangleRDD.collect();
    for(Envelope e: result) {
        System.out.println(e);
    }
}  
    
    /*
     *  If we want to use a grid type that is not supported yet, an exception will be throwed out.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testBuildWithNoExistsGrid() throws Exception {
        PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, "ff", numPartitions);
    }

    /*
     * If the partition number is set too large, we will
     */
    @Test(expected=IllegalArgumentException.class)
    public void testTooLargePartitionNumber() throws Exception {
        PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, gridType, 1000000);
    }

    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}
