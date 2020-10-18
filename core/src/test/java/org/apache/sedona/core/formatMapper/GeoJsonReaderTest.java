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

import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class GeoJsonReaderTest
        extends TestBase
{

    public static String geoJsonGeomWithFeatureProperty = null;
    public static String geoJsonGeomWithoutFeatureProperty = null;
    public static String geoJsonWithInvalidGeometries = null;
    public static String geoJsonWithNullProperty = null;
    public static String geoJsonContainsId = null;

    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(GeoJsonReaderTest.class.getName());
        geoJsonGeomWithFeatureProperty = GeoJsonReaderTest.class.getClassLoader().getResource("testPolygon.json").getPath();
        geoJsonGeomWithoutFeatureProperty = GeoJsonReaderTest.class.getClassLoader().getResource("testpolygon-no-property.json").getPath();
        geoJsonWithInvalidGeometries = GeoJsonReaderTest.class.getClassLoader().getResource("testInvalidPolygon.json").getPath();
        geoJsonWithNullProperty = GeoJsonReaderTest.class.getClassLoader().getResource("testpolygon-with-null-property-value.json").getPath();
        geoJsonContainsId = GeoJsonReaderTest.class.getClassLoader().getResource("testContainsId.json").getPath();
    }

    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sc.stop();
    }

    /**
     * Test correctness of parsing geojson file
     *
     * @throws IOException
     */
    @Test
    public void testReadToGeometryRDD()
            throws IOException
    {
        // load geojson with our tool
        SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithFeatureProperty);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 1001);
        geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithoutFeatureProperty);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 10);
    }

    /**
     * Test geojson with null values in the properties
     *
     * @throws IOException
     */
    @Test
    public void testReadToGeometryRDDWithNullValue()
            throws IOException
    {
        SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonWithNullProperty);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 3);
    }

    /**
     * Test correctness of parsing geojson file
     *
     * @throws IOException
     */
    @Test
    public void testReadToValidGeometryRDD()
            throws IOException
    {
        //ensure that flag does not affect valid geometries
        SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithFeatureProperty, true, false);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 1001);
        geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonGeomWithoutFeatureProperty, true, false);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 10);
        //2 valid and 1 invalid geometries
        geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonWithInvalidGeometries, false, false);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 2);

        geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonWithInvalidGeometries);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 3);
    }

    /**
     * Test correctness of parsing geojson file including id
     */
    @Test
    public void testReadToIncludIdRDD()
            throws IOException
    {
        SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonContainsId, true, false);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 1);
        assertEquals(geojsonRDD.fieldNames.size(), 3);
    }
}
