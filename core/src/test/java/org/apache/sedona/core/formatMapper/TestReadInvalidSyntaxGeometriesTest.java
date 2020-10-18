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

public class TestReadInvalidSyntaxGeometriesTest
        extends TestBase
{

    public static String invalidSyntaxGeoJsonGeomWithFeatureProperty = null;

    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(GeoJsonReaderTest.class.getName());
        invalidSyntaxGeoJsonGeomWithFeatureProperty = TestReadInvalidSyntaxGeometriesTest.class.getClassLoader().getResource("invalidSyntaxGeometriesJson.json").getPath();
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
        // would crash with java.lang.IllegalArgumentException: Points of LinearRing do not form a closed linestring if Invalid syntax is not skipped
        SpatialRDD geojsonRDD = GeoJsonReader.readToGeometryRDD(sc, invalidSyntaxGeoJsonGeomWithFeatureProperty, false, true);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 1);
    }
}
