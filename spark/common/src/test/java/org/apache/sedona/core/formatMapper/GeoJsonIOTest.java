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
package org.apache.sedona.core.formatMapper;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

public class GeoJsonIOTest extends TestBase {

  public static String geoJsonGeomWithFeatureProperty = null;
  public static String geoJsonGeomWithoutFeatureProperty = null;
  public static String geoJsonWithInvalidGeometries = null;
  public static String geoJsonWithNullProperty = null;
  public static String geoJsonContainsId = null;

  @BeforeClass
  public static void onceExecutedBeforeAll() throws IOException {
    initialize(GeoJsonIOTest.class.getName());
    geoJsonGeomWithFeatureProperty =
        GeoJsonIOTest.class.getClassLoader().getResource("testPolygon.json").getPath();
    geoJsonGeomWithoutFeatureProperty =
        GeoJsonIOTest.class.getClassLoader().getResource("testpolygon-no-property.json").getPath();
    geoJsonWithInvalidGeometries =
        GeoJsonIOTest.class.getClassLoader().getResource("testInvalidPolygon.json").getPath();
    geoJsonWithNullProperty =
        GeoJsonIOTest.class
            .getClassLoader()
            .getResource("testpolygon-with-null-property-value.json")
            .getPath();
    geoJsonContainsId =
        GeoJsonIOTest.class.getClassLoader().getResource("testContainsId.json").getPath();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    sc.stop();
  }

  /**
   * Test correctness of parsing geojson file
   *
   * @throws IOException
   */
  @Test
  public void testReadToGeometryRDD() throws IOException {
    // load geojson with our tool
    SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithFeatureProperty);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 1001);
    geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithoutFeatureProperty);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 10);
  }

  @Test
  public void testReadWriteGeoJson() throws IOException {
    String tmpFilePath = "target/geojson.tmp";
    SpatialRDD initRdd = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithFeatureProperty);
    deleteFile(tmpFilePath);
    initRdd.saveAsGeoJSON(tmpFilePath);
    SpatialRDD newRdd = GeoJsonReader.readToGeometryRDD(sc, tmpFilePath);

    // Check the basic correctness
    assertEquals(initRdd.fieldNames.size(), newRdd.fieldNames.size());
    assertEquals(initRdd.rawSpatialRDD.count(), newRdd.rawSpatialRDD.count());

    // Note that two RDDs may put <field name, value> in different order

    // Put field names and values to a hash map for comparison
    Geometry initGeom = (Geometry) initRdd.rawSpatialRDD.takeOrdered(1).get(0);
    String[] initGeomFields = initGeom.getUserData().toString().split("\t");
    Map<String, Object> initKvs = new HashMap<String, Object>();
    for (int i = 0; i < initGeomFields.length; i++) {
      initKvs.put(initRdd.fieldNames.get(i).toString(), initGeomFields[i]);
    }

    // Put field names and values to a hash map for comparison
    Geometry newGeom = (Geometry) newRdd.rawSpatialRDD.takeOrdered(1).get(0);
    String[] newGeomFields = newGeom.getUserData().toString().split("\t");
    Map<String, Object> newKvs = new HashMap<String, Object>();
    for (int i = 0; i < initGeomFields.length; i++) {
      newKvs.put(newRdd.fieldNames.get(i).toString(), newGeomFields[i]);
    }

    for (int i = 0; i < initRdd.fieldNames.size(); i++) {
      // The same field name should fetch the same value in both maps
      assertEquals(
          initKvs.get(initRdd.fieldNames.get(i).toString()),
          newKvs.get(initRdd.fieldNames.get(i).toString()));
    }
  }

  @Test
  public void testReadWriteSpecialGeoJsons() throws IOException {
    String tmpFilePath = "target/geojson.tmp";
    SpatialRDD initRdd = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithoutFeatureProperty);
    deleteFile(tmpFilePath);
    initRdd.saveAsGeoJSON(tmpFilePath);
    SpatialRDD newRdd = GeoJsonReader.readToGeometryRDD(sc, tmpFilePath);
    assertEquals(initRdd.rawSpatialRDD.count(), newRdd.rawSpatialRDD.count());

    initRdd = GeoJsonReader.readToGeometryRDD(sc, geoJsonWithNullProperty);
    deleteFile(tmpFilePath);
    initRdd.saveAsGeoJSON(tmpFilePath);
    newRdd = GeoJsonReader.readToGeometryRDD(sc, tmpFilePath);
    assertEquals(initRdd.rawSpatialRDD.count(), newRdd.rawSpatialRDD.count());

    //        deleteFile(tmpFilePath);
  }

  /**
   * Test geojson with null values in the properties
   *
   * @throws IOException
   */
  @Test
  public void testReadToGeometryRDDWithNullValue() throws IOException {
    SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonWithNullProperty);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 3);
  }

  /**
   * Test correctness of parsing geojson file
   *
   * @throws IOException
   */
  @Test
  public void testReadToValidGeometryRDD() throws IOException {
    // ensure that flag does not affect valid geometries
    SpatialRDD geojsonRDD =
        GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithFeatureProperty, true, false);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 1001);
    geojsonRDD =
        GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithoutFeatureProperty, true, false);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 10);
    // 2 valid and 1 invalid geometries
    geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonWithInvalidGeometries, false, false);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 2);

    geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonWithInvalidGeometries);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 3);
  }

  /** Test correctness of parsing geojson file including id */
  @Test
  public void testReadToIncludeIdRDD() throws IOException {
    SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonContainsId, true, false);
    assertEquals(geojsonRDD.rawSpatialRDD.count(), 1);
    assertEquals(geojsonRDD.fieldNames.size(), 3);
  }
}
