package org.datasyslab.geospark.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

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

    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}
