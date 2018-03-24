/**
 * FILE: testGeoJSON.java
 * PATH: org.datasyslab.geospark.utils.testGeoJSON.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wololo.geojson.Feature;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Arizona State University DataSystems Lab
 */

// TODO: Auto-generated Javadoc

/**
 * The Class testGeoJSON.
 */
public class testGeoJSON
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        SparkConf conf = new SparkConf().setAppName("JobTileMatchWithAcronymExpension").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    /**
     * Jts 2 geonjson.
     */
    @Test
    public void jts2geonjson()
    {
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate coordinate = new Coordinate(1.0, 2.0);
        Geometry point = geometryFactory.createPoint(coordinate);

        GeoJSONWriter writer = new GeoJSONWriter();
        Map<String, Object> userData = new HashMap<String, Object>();
        userData.put("UserData", "Payload");
        Feature jsonFeature = new Feature(writer.write(point), userData);

        String jsonstring = jsonFeature.toString();
        assert jsonstring.equals("{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1.0,2.0]},\"properties\":{\"UserData\":\"Payload\"}}");
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }
}
