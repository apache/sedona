/*
 * FILE: GeoJsonReaderTest
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

package org.datasyslab.geospark.formatMapper;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class GeoJsonReaderTest
        extends GeoSparkTestBase
{

    public static String geoJsonGeomWithFeatureProperty = null;
    public static String geoJsonGeomWithoutFeatureProperty = null;

    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(GeoJsonReaderTest.class.getName());
        geoJsonGeomWithFeatureProperty = GeoJsonReaderTest.class.getClassLoader().getResource("testPolygon.json").getPath();
        geoJsonGeomWithoutFeatureProperty = GeoJsonReaderTest.class.getClassLoader().getResource("testpolygon-no-property.json").getPath();
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
}
