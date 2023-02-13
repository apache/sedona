/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.core.spatialRDD;

import org.apache.spark.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;

import static org.junit.Assert.assertEquals;

public class GeometryOpTest extends SpatialRDDTestBase
{
    private static String InputLocationGeojson;
    private static String InputLocationWkt;
    private static String InputLocationWkb;
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(GeometryOpTest.class.getSimpleName(), "polygon.test.properties");
        InputLocationGeojson = "file://" + GeometryOpTest.class.getClassLoader().getResource(prop.getProperty("inputLocationGeojson")).getPath();
        InputLocationWkt = "file://" + GeometryOpTest.class.getClassLoader().getResource(prop.getProperty("inputLocationWkt")).getPath();
        InputLocationWkb = "file://" + GeometryOpTest.class.getClassLoader().getResource(prop.getProperty("inputLocationWkb")).getPath();
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }

    @Test
    public void testFlipPolygonCoordinates()
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        Polygon oldGeom = spatialRDD.rawSpatialRDD.take(1).get(0);
        spatialRDD.flipCoordinates();
        Polygon newGeom = spatialRDD.rawSpatialRDD.take(1).get(0);
        assertEquals(oldGeom.getCoordinates().length, newGeom.getCoordinates().length);
        for (int i =0; i < newGeom.getCoordinates().length; i++) {
            assertEquals(newGeom.getCoordinates()[i].getX(), oldGeom.getCoordinates()[i].getY(), 0);
            assertEquals(newGeom.getCoordinates()[i].getY(), oldGeom.getCoordinates()[i].getX(), 0);
        }
    }
}
