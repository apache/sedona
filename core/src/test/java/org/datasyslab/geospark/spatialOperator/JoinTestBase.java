package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class JoinTestBase {

    /** The sc. */
    public static JavaSparkContext sc;

    /** The prop. */
    static Properties prop;

    /** The Input location. */
    static String InputLocation;

    /** The Input location query window. */
    static String InputLocationQueryWindow;

    /** The Input location query polygon. */
    static String InputLocationQueryPolygon;

    /** The offset. */
    static Integer offset;

    /** The splitter. */
    static FileDataSplitter splitter;

    /** The grid type. */
    static GridType gridType;

    /** The index type. */
    static IndexType indexType;

    /** The num partitions. */
    static Integer numPartitions;

    /** The conf. */
    static SparkConf conf;

    protected static void initialize(final String testSuiteName, final String propertiesFileName) {
        conf = new SparkConf().setAppName(testSuiteName).setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        final ClassLoader classLoader = JoinTestBase.class.getClassLoader();
        final InputStream input = classLoader.getResourceAsStream(propertiesFileName);
        InputLocation = "file://"+ classLoader.getResource("primaryroads.csv").getPath();
        offset = 0;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            InputLocation = "file://"+ classLoader.getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryWindow = "file://"+ classLoader.getResource(prop.getProperty("queryWindowSet")).getPath();
            InputLocationQueryPolygon = "file://"+ classLoader.getResource(prop.getProperty("queryPolygonSet")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
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

    protected <T extends Geometry> long countJoinResults(List<Tuple2<Polygon, HashSet<T>>> results) {
        int count = 0;
        for (final Tuple2<Polygon, HashSet<T>> tuple : results) {
            count += tuple._2().size();
        }
        return count;
    }

    protected  <T extends Geometry> void sanityCheckJoinResults(List<Tuple2<Polygon, HashSet<T>>> results) {
        for (final Tuple2<Polygon, HashSet<T>> tuple : results) {
            assertNotNull(tuple._1().getUserData());
            assertFalse(tuple._2().isEmpty());
            for (final T shape : tuple._2()) {
                assertNotNull(shape.getUserData());
                assertTrue(tuple._1().intersects(shape));
            }
        }
    }

    protected  <T extends Geometry> void sanityCheckFlatJoinResults(List<Tuple2<Polygon, T>> results) {
        for (final Tuple2<Polygon, T> tuple : results) {
            assertNotNull(tuple._1().getUserData());
            assertNotNull(tuple._2().getUserData());
            assertTrue(tuple._1().intersects(tuple._2()));
        }
    }
}
