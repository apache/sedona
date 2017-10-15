/**
 * FILE: ShapefileReaderTest.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapefileReaderTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */

package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.boundary.BoundBox;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class ShapefileReaderTest implements Serializable{

    /** The sc. */
    public static JavaSparkContext sc;

    /** The Input location. */
    public static String InputLocation;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        SparkConf conf = new SparkConf().setAppName("ShapefileRDDTest").setMaster("local[2]").set("spark.executor.cores","2");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
    }

    /**
     * Test correctness of parsing shapefile
     * @throws IOException
     */
    @Test
    public void testReadToGeometryRDD() throws IOException {
        // load shape with geotool.shapefile
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/polygon").getPath();
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
        JavaRDD<Geometry> shapeRDD = ShapefileReader.readToGeometryRDD(sc, InputLocation);
        Assert.assertEquals(shapeRDD.collect().size(), collection.size());
        dataStore.dispose();
    }

    /**
     * Test correctness of parsing files with shape type = Polygon
     * @throws IOException
     */
    @Test
    public void testReadToPolygonRDD() throws IOException {
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/polygon").getPath();
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

        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, InputLocation);
        PolygonRDD spatialRDD = ShapefileReader.geometryToPolygon(geometryRDD);

        try {
            RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180,180,-90,90), false, false).count();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
        dataStore.dispose();
    }

    /**
     * Test correctness of parsing files with shape type = PolyLine
     * @throws IOException
     */
    @Test
    public void testreadToLineStringRDD() throws IOException{
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/polyline").getPath();
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
        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, InputLocation);
        LineStringRDD spatialRDD = ShapefileReader.geometryToLineString(geometryRDD);
        try {
            RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180,180,-90,90), false, false).count();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
        dataStore.dispose();
    }

    /**
     * Test correctness of parsing files with shape type = Point
     * @throws IOException
     */
    @Test
    public void testReadToPointRDD_Point() throws IOException{
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/point").getPath();
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
        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, InputLocation);
        PointRDD spatialRDD = ShapefileReader.geometryToPoint(geometryRDD);
        try {
            RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180,180,-90,90), false, false).count();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
        dataStore.dispose();
    }

    /**
     * Test correctness of parsing files with shape type = MultiPoint
     * @throws IOException
     */
    @Test
    public void testReadToPointRDD_MultiPoint() throws IOException{
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/multipoint").getPath();
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
        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, InputLocation);
        PointRDD spatialRDD = ShapefileReader.geometryToPoint(geometryRDD);
        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
        dataStore.dispose();
    }

    /**
     * Test correctness of .dbf parser
     * @throws IOException
     */
    @Test
    public void testLoadDbfFile() throws IOException{
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/dbf").getPath();
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
            String attr = "";
            int i = 0;
            for (Property property : feature.getProperties()) {
                if(i == 0){
                    i++;
                    continue;
                }
                if(i > 1) attr += "\t";
                attr += String.valueOf(property.getValue());
                i++;
            }
            featureTexts.add(attr);
        }
        final Iterator<String> featureIterator = featureTexts.iterator();

        for (Geometry geometry : ShapefileReader.readToGeometryRDD(sc, InputLocation).collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.getUserData());
        }
        dataStore.dispose();
    }

    /**
     * Test if parse the boundary in header correctly
     * @throws IOException
     */
    @Test
    public void testReadBoundary() throws IOException{
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/dbf").getPath();
        // load shapefile with geotools's reader
        ShpFiles shpFile = new ShpFiles(InputLocation + "/map.shp");
        GeometryFactory geometryFactory = new GeometryFactory();
        org.geotools.data.shapefile.shp.ShapefileReader gtlReader = new org.geotools.data.shapefile.shp.ShapefileReader(shpFile, false, true, geometryFactory);
        String gtlbounds =
                gtlReader.getHeader().minX() + ":" +
                        gtlReader.getHeader().minY() + ":" +
                        gtlReader.getHeader().maxX() + ":" +
                        gtlReader.getHeader().maxY();
        // read shapefile by our reader
        BoundBox bounds = ShapefileReader.readBoundBox(sc, InputLocation);
        String myBounds =
                bounds.getXMin() + ":" +
                        bounds.getYMin() + ":" +
                        bounds.getXMax() + ":" +
                        bounds.getYMax();
        Assert.assertEquals(gtlbounds, myBounds);
        gtlReader.close();
    }



    @AfterClass
    public static void tearDown() throws Exception {
        sc.stop();
    }
}
