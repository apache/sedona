/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.formatMapper.shapefileParser.boundary.BoundBox;
import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.LineStringRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.utils.GeomUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ShapefileReaderTest
        extends TestBase
{

    public static FileSystem fs;

    public static MiniDFSCluster hdfsCluster;

    public static String hdfsURI;

    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(ShapefileReaderTest.class.getName());
        // Set up HDFS minicluster
        File baseDir = new File("./target/hdfs/shapefile").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        HdfsConfiguration hdfsConf = new HdfsConfiguration();
        hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdfsConf);
        hdfsCluster = builder.build();
        fs = FileSystem.get(hdfsConf);
        hdfsURI = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort() + "/";
    }

    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sc.stop();
        hdfsCluster.shutdown();
        fs.close();
    }

    /**
     * Test correctness of parsing file with UNDEFINED type shape
     *
     * @throws IOException
     */
    @Ignore
    public void testShapefileEndWithUndefinedType()
            throws IOException
    {
        // load shape with geotool.shapefile
        String inputLocation = getShapeFilePath("undefined");
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        // load shapes with our tool
        SpatialRDD shapeRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        FeatureIterator<SimpleFeature> features = collection.features();
        int nullNum = 0;
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            Geometry g = (Geometry) feature.getDefaultGeometry();
            if (g == null) { nullNum++; }
        }
        assertEquals(shapeRDD.getRawSpatialRDD().count(), collection.size() - nullNum);
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
        SpatialRDD shapeRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        assertEquals(shapeRDD.rawSpatialRDD.collect().size(), collection.size());
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
            featureTexts.add(GeomUtils.printGeom(geometry));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();

        PolygonRDD spatialRDD = ShapefileReader.readToPolygonRDD(sc, inputLocation);
        SpatialRDD<Geometry> geomeryRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);

        long count = RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        assertEquals(spatialRDD.rawSpatialRDD.count(), count);

        for (Geometry geometry : geomeryRDD.rawSpatialRDD.collect()) {
            assertEquals(featureIterator.next(), geometry.toText());
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
            featureTexts.add(GeomUtils.printGeom(feature.getDefaultGeometry()));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();
        LineStringRDD spatialRDD = ShapefileReader.readToLineStringRDD(sc, inputLocation);
        SpatialRDD<Geometry> geomeryRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        long count = RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        assertEquals(spatialRDD.rawSpatialRDD.count(), count);

        for (Geometry geometry : geomeryRDD.rawSpatialRDD.collect()) {
            assertEquals(featureIterator.next(), geometry.toText());
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
            featureTexts.add(GeomUtils.printGeom(feature.getDefaultGeometry()));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();
        PointRDD spatialRDD = ShapefileReader.readToPointRDD(sc, inputLocation);

        long count = RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        assertEquals(spatialRDD.rawSpatialRDD.count(), count);

        for (Geometry geometry : spatialRDD.rawSpatialRDD.collect()) {
            assertEquals(featureIterator.next(), geometry.toText());
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
            featureTexts.add(GeomUtils.printGeom(feature.getDefaultGeometry()));
        }
        features.close();
        final Iterator<String> featureIterator = featureTexts.iterator();
        PointRDD spatialRDD = ShapefileReader.readToPointRDD(sc, inputLocation);
        SpatialRDD<Geometry> geomeryRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        for (Geometry geometry : geomeryRDD.rawSpatialRDD.collect()) {
            assertEquals(featureIterator.next(), geometry.toText());
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

        for (Geometry geometry : ShapefileReader.readToGeometryRDD(sc, inputLocation).rawSpatialRDD.collect()) {
            assertEquals(featureIterator.next(), geometry.getUserData());
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
        assertEquals(gtlbounds, myBounds);
        gtlReader.close();
    }

    /**
     * Test if parse the field names in header correctly
     *
     * @throws IOException
     */
    @Test
    public void testReadFieldNames()
            throws IOException
    {
        String inputLocation = getShapeFilePath("dbf");
        // read shapefile by our reader
        List<String> fieldName = ShapefileReader.readFieldNames(sc, inputLocation);
        assertEquals("[STATEFP, COUNTYFP, COUNTYNS, AFFGEOID, GEOID, NAME, LSAD, ALAND, AWATER]", fieldName.toString());
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

    /**
     * Test whether the shapefile can be loaded from hdfs
     *
     * @throws IOException
     */
    @Test
    public void testLoadFromHDFS()
            throws IOException
    {
        String shapefileHDFSpath = hdfsURI + "dbf";
        fs.copyFromLocalFile(new Path(getShapeFilePath("dbf")), new Path(shapefileHDFSpath));
        RemoteIterator<LocatedFileStatus> hdfsFileIterator = fs.listFiles(new Path(shapefileHDFSpath), false);
        while (hdfsFileIterator.hasNext()) {
            assertEquals(hdfsFileIterator.next().getPath().getParent().toString(), shapefileHDFSpath);
        }
        SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(sc, shapefileHDFSpath);
        assertEquals("[STATEFP, COUNTYFP, COUNTYNS, AFFGEOID, GEOID, NAME, LSAD, ALAND, AWATER]", spatialRDD.fieldNames.toString());
    }

    /**
     * Test read Multiple Shape Files by MultiPartitions
     *
     * @throws IOException
     */
    @Test
    public void testReadMultipleShapeFilesByMultiPartitions()
            throws IOException
    {
        // load shape with geotool.shapefile
        String inputLocation = getShapeFilePath("multipleshapefiles");
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = loadFeatures(inputLocation);
        // load shapes with our tool
        SpatialRDD shapeRDD = ShapefileReader.readToGeometryRDD(sc, inputLocation);
        assert (shapeRDD.rawSpatialRDD.getNumPartitions() == 2);
    }
}
