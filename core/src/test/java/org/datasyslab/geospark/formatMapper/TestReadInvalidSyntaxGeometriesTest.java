package org.datasyslab.geospark.formatMapper;

import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestReadInvalidSyntaxGeometriesTest extends GeoSparkTestBase {

    public static String invalidSyntaxGeoJsonGeomWithFeatureProperty = null;

    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(GeoJsonReaderTest.class.getName());
        invalidSyntaxGeoJsonGeomWithFeatureProperty = GeoJsonReaderTest.class.getClassLoader().getResource("invalidSyntaxGeometriesJson.json").getPath();
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
        SpatialRDD geojsonRDD = GeometryReader.readToGeometryRDD(sc, invalidSyntaxGeoJsonGeomWithFeatureProperty,
                FileDataSplitter.GEOJSON, false, true);
        assertEquals(geojsonRDD.rawSpatialRDD.count(), 1);
    }

}
