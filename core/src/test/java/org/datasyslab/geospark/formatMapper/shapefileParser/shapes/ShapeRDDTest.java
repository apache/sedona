package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.junit.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by zongsizhang on 6/13/17.
 */
public class ShapeRDDTest implements Serializable{

    /** The sc. */
    public static JavaSparkContext sc;

    /** The Input location. */
    public static String InputLocation;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("ShapeRDDTest").setMaster("local[2]").set("spark.executor.cores","2");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
    }

    /**
     * Test if shapeRDD get correct number of shapes from .shp file
     * @throws IOException
     */
    @Test
    public void testLoadShapeFile() throws IOException {
        // load shape with geotool.shapefile
        InputLocation = ShapeRDDTest.class.getClassLoader().getResource("shapefiles/polygon").getPath();
        File file = new File(InputLocation);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", file.toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        // load shapes with our tool
        ShapeRDD shapeRDD = new ShapeRDD(InputLocation,sc);
        Assert.assertEquals(shapeRDD.getShapeWritableRDD().collect().size(), collection.size());
    }

    /**
     * test if shapeRDD load .shp fie with shape type = Polygon correctly.
     * @throws IOException
     */
    @Test
    public void testLoadShapeFilePolygon() throws IOException{
        InputLocation = ShapeRDDTest.class.getClassLoader().getResource("shapefiles/polygon").getPath();
        // load shape with geotool.shapefile
        File file = new File(InputLocation);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", file.toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while(features.hasNext()){
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        final Iterator<String> featureIterator = featureTexts.iterator();
        ShapeRDD shapeRDD = new ShapeRDD(InputLocation,sc);
        for (com.vividsolutions.jts.geom.Geometry geometry : shapeRDD.getShapeWritableRDD().collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * test if shapeRDD load .shp fie with shape type = PolyLine correctly.
     * @throws IOException
     */
    @Test
    public void testLoadShapeFilePolyLine() throws IOException{
        InputLocation = ShapeRDDTest.class.getClassLoader().getResource("shapefiles/polyline").getPath();
        // load shape with geotool.shapefile
        File file = new File(InputLocation);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", file.toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while(features.hasNext()){
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        final Iterator<String> featureIterator = featureTexts.iterator();
        ShapeRDD shapeRDD = new ShapeRDD(InputLocation,sc);
        for (com.vividsolutions.jts.geom.Geometry geometry : shapeRDD.getShapeWritableRDD().collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * Test if shapeRDD load shape type = MultiPoint correctly.
     * @throws IOException
     */
    @Test
    public void testLoadShapeFileMultiPoint() throws IOException{
        InputLocation = ShapeRDDTest.class.getClassLoader().getResource("shapefiles/multipoint").getPath();
        // load shape with geotool.shapefile
        File file = new File(InputLocation);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", file.toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while(features.hasNext()){
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        final Iterator<String> featureIterator = featureTexts.iterator();
        ShapeRDD shapeRDD = new ShapeRDD(InputLocation,sc);
        for (com.vividsolutions.jts.geom.Geometry geometry : shapeRDD.getShapeWritableRDD().collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * Test if shapeRDD load shape type = Point correctly.
     * @throws IOException
     */
    @Test
    public void testLoadShapeFilePoint() throws IOException{
        InputLocation = ShapeRDDTest.class.getClassLoader().getResource("shapefiles/point").getPath();
        // load shape with geotool.shapefile
        File file = new File(InputLocation);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", file.toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while(features.hasNext()){
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        final Iterator<String> featureIterator = featureTexts.iterator();
        ShapeRDD shapeRDD = new ShapeRDD(InputLocation,sc);
        for (com.vividsolutions.jts.geom.Geometry geometry : shapeRDD.getShapeWritableRDD().collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * Test if shapeRDD load .dbf file correctly
     * @throws IOException
     */
    @Test
    public void testLoadDbfFile() throws IOException{
        InputLocation = ShapeRDDTest.class.getClassLoader().getResource("shapefiles/dbf").getPath();
        // load shape with geotool.shapefile
        File file = new File(InputLocation);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", file.toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while(features.hasNext()){
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        final Iterator<String> featureIterator = featureTexts.iterator();
        ShapeRDD shapeRDD = new ShapeRDD(InputLocation,sc);
        for (com.vividsolutions.jts.geom.Geometry geometry : shapeRDD.getShapeWritableRDD().collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        sc.stop();
    }

}