/*
 * FILE: ShapefileReaderTest
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
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
import org.junit.After;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ShapefileReaderTest
        implements Serializable
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        SparkConf conf = new SparkConf().setAppName("ShapefileRDDTest").setMaster("local[2]").set("spark.executor.cores", "2").set("spark.executor.memory", "4g");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
    }

    /**
     * Test correctness of parsing shapefile
     *
     * @throws IOException
     */
    @Test
    public void testReadToGeometryRDD()
            throws IOException
    {
        // load shape with geotool.shapefile
        String inputLocation = getShapeFilePath("polygon");
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        // load shapes with our tool
        JavaRDD<Geometry> shapeRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        Assert.assertEquals(shapeRDD.collect().size(), collection.size());
    }

    /**
     * Test correctness of parsing files with shape type = Polygon
     *
     * @throws IOException
     */
    @Test
    public void testReadToPolygonRDD()
            throws Exception
    {
        String inputLocation = getShapeFilePath("polygon");
        // load shape with geotool.shapefile
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            Object geometry = feature.getDefaultGeometry();
            if (geometry instanceof MultiPolygon) {
                MultiPolygon multiPolygon = (MultiPolygon) geometry;
                if (multiPolygon.getNumGeometries() == 1) {
                    geometry = multiPolygon.getGeometryN(0);
                }
            }
            featureTexts.add(String.valueOf(geometry));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();

        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        PolygonRDD spatialRDD = ShapefileReader.geometryToPolygon(geometryRDD);

        long count = RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        Assert.assertEquals(spatialRDD.rawSpatialRDD.count(), count);

        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * Test correctness of parsing files with shape type = PolyLine
     *
     * @throws IOException
     */
    @Test
    public void testReadToLineStringRDD()
            throws Exception
    {
        String inputLocation = getShapeFilePath("polyline");
        // load shape with geotool.shapefile
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();
        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        LineStringRDD spatialRDD = ShapefileReader.geometryToLineString(geometryRDD);

        long count = RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        Assert.assertEquals(spatialRDD.rawSpatialRDD.count(), count);

        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * Test correctness of parsing files with shape type = Point
     *
     * @throws IOException
     */
    @Test
    public void testReadToPointRDD_Point()
            throws Exception
    {
        String inputLocation = getShapeFilePath("point");
        // load shape with geotool.shapefile
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();
        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        PointRDD spatialRDD = ShapefileReader.geometryToPoint(geometryRDD);

        long count = RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        Assert.assertEquals(spatialRDD.rawSpatialRDD.count(), count);

        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * Test correctness of parsing files with shape type = MultiPoint
     *
     * @throws IOException
     */
    @Test
    public void testReadToPointRDD_MultiPoint()
            throws IOException
    {
        String inputLocation = getShapeFilePath("multipoint");
        // load shape with geotool.shapefile
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            featureTexts.add(String.valueOf(feature.getDefaultGeometry()));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();
        JavaRDD<Geometry> geometryRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        PointRDD spatialRDD = ShapefileReader.geometryToPoint(geometryRDD);
        for (Geometry geometry : geometryRDD.collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.toText());
        }
    }

    /**
     * Test correctness of .dbf parser
     *
     * @throws IOException
     */
    @Test
    public void testLoadDbfFile()
            throws IOException
    {
        String inputLocation = getShapeFilePath("dbf");
        // load shape with geotool.shapefile
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        FeatureIterator<SimpleFeature> features = collection.features();
        ArrayList<String> featureTexts = new ArrayList<String>();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            String attr = "";
            int i = 0;
            for (Property property : feature.getProperties()) {
                if (i == 0) {
                    i++;
                    continue;
                }
                if (i > 1) { attr += "\t"; }
                attr += String.valueOf(property.getValue());
                i++;
            }
            featureTexts.add(attr);
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();

        for (Geometry geometry : ShapefileReader.readToGeometryRDD(sc, inputLocation).collect()) {
            Assert.assertEquals(featureIterator.next(), geometry.getUserData());
        }
    }

    /**
     * Test if parse the boundary in header correctly
     *
     * @throws IOException
     */
    @Test
    public void testReadBoundary()
            throws IOException
    {
        String inputLocation = getShapeFilePath("dbf");
        // load shapefile with geotools's reader
        ShpFiles shpFile = new ShpFiles(inputLocation + "/map.shp");
        GeometryFactory geometryFactory = new GeometryFactory();
        org.geotools.data.shapefile.shp.ShapefileReader gtlReader = new org.geotools.data.shapefile.shp.ShapefileReader(shpFile, false, true, geometryFactory);
        String gtlbounds =
                gtlReader.getHeader().minX() + ":" +
                        gtlReader.getHeader().minY() + ":" +
                        gtlReader.getHeader().maxX() + ":" +
                        gtlReader.getHeader().maxY();
        // read shapefile by our reader
        BoundBox bounds = ShapefileReader.readBoundBox(sc, inputLocation);
        String myBounds =
                bounds.getXMin() + ":" +
                        bounds.getYMin() + ":" +
                        bounds.getXMax() + ":" +
                        bounds.getYMax();
        Assert.assertEquals(gtlbounds, myBounds);
        gtlReader.close();
    }

    private String getShapeFilePath(String fileName)
    {
        return ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/" + fileName).getPath();
    }

    private FeatureCollection<SimpleFeatureType, SimpleFeature> loadFeatures(String filePath)
            throws IOException
    {
        File file = new File(filePath);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", file.toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        dataStore.dispose();
        return source.getFeatures(filter);
    }

    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sc.stop();
    }
}
