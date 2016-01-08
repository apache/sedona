package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

/**
 * Created by jinxuanwu on 1/5/16.
 */
public class testGeoJS {
    public static JavaSparkContext sc;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("JobTileMatchWithAcronymExpension").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    @Test
    public void jts2geonjs(){
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate coordinate = new Coordinate(1.0, 2.0);
        Geometry point = geometryFactory.createPoint(coordinate);

        GeoJSONWriter writer = new GeoJSONWriter();
        GeoJSON json = writer.write(point);
        String jsonstring = json.toString();

        System.out.println(jsonstring);
    }

    @Test
    public void testPointRDDSaveAsGeoJson(){
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
        //todo: Delete exist file in hdfs, to be refactored later.
        Configuration hadoopConf = new Configuration();
        String namenode = "hdfs://localhost:9000/";
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(URI.create(namenode), hadoopConf);
            Path path = new Path(namenode + "output.txt");
            if(hdfs.exists(path)) {
                hdfs.delete(path, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        pointRDD.saveAsGeoJSON("hdfs://localhost:9000/output.txt");
    }
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}
